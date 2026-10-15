using System;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Protocol;

public sealed class FileTransferService : IAsyncDisposable
{
    private readonly Lock _stateLock = new();

    private readonly Dictionary<TransferKey, TransferEntry> _transfers = [];

    private sealed class TransferEntry(FileTransfer transfer)
    {
        public FileTransfer Transfer { get; } = transfer;
        public TaskCompletionSource<bool>? Answer { get; set; }
        public IncomingFileTransfer? IncomingFile { get; set; }
        public string? SourcePath { get; set; }
    }

    private bool _disposed;

    public event Action<FileTransfer>? FileOffered;
    public event Action<FileTransfer, Exception>? TransferFailed;

    public void HandleFileOffer(ReadOnlySpan<byte> payload)
    {
        var offer = Payloads.DecodeFileOffer(payload);
        if (offer is null)
            return;

        var key = new TransferKey(offer.Value.DeviceId, offer.Value.FileId);
        FileTransfer fileTransfer;

        lock (_stateLock)
        {
            if (_disposed)
                return;

            if (_transfers.ContainsKey(key))
                return;

            fileTransfer = new FileTransfer()
            {
                FileId = offer.Value.FileId,
                PeerDeviceId = offer.Value.DeviceId,
                FileName = offer.Value.Name,
                FileSize = offer.Value.Size,
                Sha256 = offer.Value.Sha256,
                Direction = TransferDirection.Incoming,
                State = TransferState.WaitingForAnswer,
            };

            _transfers.Add(key, new TransferEntry(fileTransfer));
            fileTransfer = fileTransfer.Snapshot();
        }

        FileOffered?.Invoke(fileTransfer);
    }

    public FileTransfer CreateOutgoingTransfer(
        Guid peerDeviceId,
        string fileName,
        ulong fileSize,
        byte[] sha256
    ) => RegisterOutgoingTransfer(peerDeviceId, fileName, fileSize, sha256, sourcePath: null);

    private FileTransfer RegisterOutgoingTransfer(
        Guid peerDeviceId,
        string fileName,
        ulong fileSize,
        byte[] sha256,
        string? sourcePath
    )
    {
        ArgumentOutOfRangeException.ThrowIfGreaterThan(
            fileSize,
            (ulong)ProtocolConstants.MaxFileSize
        );

        ArgumentNullException.ThrowIfNull(sha256);
        if (sha256.Length != PayloadsConstants.Sha256Size)
            throw new ArgumentException("SHA-256 must contain 32 bytes", nameof(sha256));

        if (
            string.IsNullOrEmpty(fileName)
            || Encoding.UTF8.GetByteCount(fileName) > PayloadsConstants.MaxNameBytes
        )
            throw new ArgumentException("File Name required or not too long");

        FileTransfer fileTransfer;

        lock (_stateLock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            fileTransfer = new()
            {
                FileId = Guid.NewGuid(),
                PeerDeviceId = peerDeviceId,
                FileName = fileName,
                FileSize = fileSize,
                Sha256 = (byte[])sha256.Clone(),
                Direction = TransferDirection.Outgoing,
                State = TransferState.WaitingForAnswer,
            };
            var key = new TransferKey(fileTransfer.PeerDeviceId, fileTransfer.FileId);
            _transfers.Add(key, new TransferEntry(fileTransfer)
            {
                SourcePath = sourcePath,
                Answer = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously),
            });
            return fileTransfer.Snapshot();
        }
    }

    public async Task<FileTransfer> PrepareOutgoingTransferAsync(
        Guid peerDeviceId,
        string filePath,
        CancellationToken ct
    )
    {
        ct.ThrowIfCancellationRequested();
        lock (_stateLock)
            ObjectDisposedException.ThrowIf(_disposed, this);

        var fullPath = Path.GetFullPath(filePath);
        await using var fileStream = new FileStream(
            fullPath,
            FileMode.Open,
            FileAccess.Read,
            FileShare.Read
        );

        var fileSize = fileStream.Length;

        ArgumentOutOfRangeException.ThrowIfGreaterThan(
            fileStream.Length,
            ProtocolConstants.MaxFileSize
        );

        var fileName = Path.GetFileName(fullPath);
        var sha256 = await SHA256.HashDataAsync(fileStream, ct);
        ct.ThrowIfCancellationRequested();
        return RegisterOutgoingTransfer(peerDeviceId, fileName, (ulong)fileSize, sha256, fullPath);
    }

    public FileTransfer GetTransfer(Guid peerDeviceId, Guid fileId)
    {
        lock (_stateLock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            return _transfers[new TransferKey(peerDeviceId, fileId)].Transfer.Snapshot();
        }
    }

    private static IncomingFileTransfer? ChangeState(TransferEntry entry, TransferState state)
    {
        entry.Transfer.State = state;
        if (state is TransferState.Completed or TransferState.Rejected or TransferState.Failed or TransferState.Cancelled)
            entry.SourcePath = null;

        if (state is not (TransferState.Failed or TransferState.Cancelled))
            return null;

        entry.Answer?.TrySetCanceled();
        entry.Answer = null;
        var file = entry.IncomingFile;
        entry.IncomingFile = null;
        return file;
    }

    internal void SetState(TransferKey key, TransferState state, Exception? error = null)
    {
        IncomingFileTransfer? file;
        FileTransfer snapshot;
        lock (_stateLock)
        {
            // An operation may finish unwinding after the service has been disposed.
            if (_disposed)
                return;

            var entry = _transfers[key];
            if (entry.Transfer.State is TransferState.Completed or TransferState.Rejected or TransferState.Failed or TransferState.Cancelled)
                return;

            file = ChangeState(entry, state);
            snapshot = entry.Transfer.Snapshot();
        }
        try
        {
            file?.Dispose();
        }
        finally
        {
            if (state == TransferState.Failed && error is not null)
                TransferFailed?.Invoke(snapshot, error);
        }
    }

    public FileTransfer SetIncomingDecision(
        Guid peerDeviceId,
        Guid fileId,
        bool accepted,
        string directory
    )
    {
        var key = new TransferKey(peerDeviceId, fileId);
        TransferEntry entry;
        FileTransfer transfer;
        lock (_stateLock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            if (!_transfers.TryGetValue(key, out var value))
                throw new ArgumentException("Transfer not exists");

            entry = value;
            transfer = entry.Transfer;

            if (transfer.Direction != TransferDirection.Incoming)
                throw new InvalidOperationException("Transfer must be incoming");

            if (transfer.State != TransferState.WaitingForAnswer)
                throw new InvalidOperationException("Transfer is not waiting for a decision");

            if (!accepted)
            {
                ChangeState(entry, TransferState.Rejected);
                return transfer.Snapshot();
            }
        }
        var incomingFile = IncomingFileTransfer.Create(transfer.FileSize, directory);

        try
        {
            lock (_stateLock)
            {
                ObjectDisposedException.ThrowIf(_disposed, this);

                if (transfer.State != TransferState.WaitingForAnswer)
                    throw new InvalidOperationException("Transfer is not waiting for a decision");

                entry.IncomingFile = incomingFile;
                ChangeState(entry, TransferState.Transferring);
                return transfer.Snapshot();
            }
        }
        catch
        {
            incomingFile.Dispose();
            throw;
        }
    }

    public bool HandleFileAnswer(ReadOnlySpan<byte> payload)
    {
        var answer = Payloads.DecodeFileAnswer(payload);
        if (answer is null)
            return false;

        lock (_stateLock)
        {
            if (_disposed)
                return false;

            var key = new TransferKey(answer.Value.DeviceId, answer.Value.FileId);
            if (!_transfers.TryGetValue(key, out var entry))
                return false;

            var transfer = entry.Transfer;
            if (transfer.Direction != TransferDirection.Outgoing)
                return false;

            if (transfer.State != TransferState.WaitingForAnswer)
                return false;

            var completion = entry.Answer;
            if (completion is null)
                return false;

            if (!answer.Value.Accepted)
            {
                ChangeState(entry, TransferState.Rejected);
                completion.TrySetResult(false);
                return true;
            }

            if (answer.Value.NextIndex != 0)
                return false;

            transfer.NextIndex = answer.Value.NextIndex;
            ChangeState(entry, TransferState.Transferring);
            completion.TrySetResult(true);
        }
        return true;
    }

    public async Task<bool> WaitForFileAnswerAsync(
        Guid peerDeviceId,
        Guid fileId,
        CancellationToken ct
    )
    {
        var key = new TransferKey(peerDeviceId, fileId);
        TransferEntry entry;
        TaskCompletionSource<bool> completion;

        lock (_stateLock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            entry = _transfers[key];
            completion = entry.Answer
                ?? throw new InvalidOperationException("Transfer is not waiting for an answer");
        }
        try
        {
            return await completion.Task.WaitAsync(ProtocolConstants.FileAnswerTimeout, ct);
        }
        catch (TimeoutException ex)
        {
            var answer = StopWaitingForAnswer(entry, completion, TransferState.Failed, ex);
            if (answer.HasValue)
                return answer.Value;
            throw;
        }
        catch (OperationCanceledException)
        {
            var answer = StopWaitingForAnswer(entry, completion, TransferState.Cancelled);
            if (answer.HasValue)
                return answer.Value;
            throw;
        }
        finally
        {
            lock (_stateLock)
            {
                if (entry.Answer == completion)
                    entry.Answer = null;
            }
        }
    }

    private bool? StopWaitingForAnswer(
        TransferEntry entry,
        TaskCompletionSource<bool> completion,
        TransferState state,
        Exception? error = null
    )
    {
        FileTransfer snapshot;
        lock (_stateLock)
        {
            if (completion.Task.IsCompletedSuccessfully)
                return completion.Task.Result;
            if (_disposed || entry.Transfer.State != TransferState.WaitingForAnswer)
                return null;

            ChangeState(entry, state);
            snapshot = entry.Transfer.Snapshot();
        }
        if (error is not null)
            TransferFailed?.Invoke(snapshot, error);
        return null;
    }

    public bool HandleChunk(ReadOnlySpan<byte> payload)
    {
        var chunk = Payloads.DecodeChunk(payload);
        if (chunk is null)
            return false;
        var key = new TransferKey(chunk.Value.DeviceId, chunk.Value.FileId);
        TransferEntry entry;
        IncomingFileTransfer incomingFile;
        lock (_stateLock)
        {
            if (_disposed || !_transfers.TryGetValue(key, out var value))
                return false;
            entry = value;
            if (entry.Transfer.State != TransferState.Transferring
                || entry.Transfer.Direction != TransferDirection.Incoming
                || entry.IncomingFile is null)
                return false;
            incomingFile = entry.IncomingFile;
        }
        try
        {
            if (!incomingFile.TryWriteChunk(chunk.Value.Index, chunk.Value.Data, out var nextIndex))
                return false;

            lock (_stateLock)
            {
                if (_disposed || entry.IncomingFile != incomingFile
                    || entry.Transfer.State != TransferState.Transferring)
                    return false;
                entry.Transfer.NextIndex = Math.Max(entry.Transfer.NextIndex, nextIndex);
                return true;
            }
        }
        catch (IOException ex)
        {
            SetState(key, TransferState.Failed, ex);
            return false;
        }
    }

    internal FileStream OpenOutgoingFile(TransferKey key)
    {
        string fullPath;
        FileTransfer transfer;
        lock (_stateLock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            transfer = GetOutgoingTransferForSending(key).Snapshot();
            fullPath = _transfers[key].SourcePath
                ?? throw new InvalidOperationException("Source path not found");
        }
        var stream = new FileStream(fullPath, FileMode.Open, FileAccess.Read, FileShare.Read);
        try
        {
            // shortcut: external writers may bypass FileShare.Read; the receiver must verify the final SHA-256.
            if ((ulong)stream.Length != transfer.FileSize
                || !SHA256.HashData(stream).AsSpan().SequenceEqual(transfer.Sha256))
                throw new IOException("Source file has changed since the offer was prepared");
            stream.Position = 0;
            lock (_stateLock)
            {
                ObjectDisposedException.ThrowIf(_disposed, this);
                GetOutgoingTransferForSending(key);
            }
            return stream;
        }
        catch
        {
            stream.Dispose();
            throw;
        }
    }

    internal void ConfirmOutgoingChunk(TransferKey key, uint index)
    {
        lock (_stateLock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            var transfer = GetOutgoingTransferForSending(key);

            if (index != transfer.NextIndex)
                throw new InvalidOperationException("Unexpected acknowledged chunk index");

            var offset = (ulong)index * (ulong)ProtocolConstants.ChunkSize;
            if (offset >= transfer.FileSize)
                throw new ArgumentOutOfRangeException(nameof(index));

            transfer.NextIndex++;
        }
    }

    private FileTransfer GetOutgoingTransferForSending(TransferKey key)
    {
        if (!_transfers.TryGetValue(key, out var entry))
            throw new ArgumentException("Transfer not found", nameof(key));

        var transfer = entry.Transfer;
        if (transfer.Direction != TransferDirection.Outgoing)
            throw new InvalidOperationException("Transfer direction must be outgoing");

        if (transfer.State != TransferState.Transferring)
            throw new InvalidOperationException("Transfer state must be transferring");

        return transfer;
    }

    public async ValueTask DisposeAsync()
    {
        List<IncomingFileTransfer> files;
        lock (_stateLock)
        {
            if (_disposed)
                return;
            _disposed = true;
            files = [];
            foreach (var entry in _transfers.Values)
            {
                entry.Answer?.TrySetCanceled();
                if (entry.IncomingFile is not null)
                    files.Add(entry.IncomingFile);
            }
            _transfers.Clear();
        }
        var errors = new List<Exception>();

        foreach (var file in files)
        {
            try
            {
                await file.DisposeAsync();
            }
            catch (Exception ex)
            {
                errors.Add(ex);
            }
        }

        if (errors.Count > 0)
            throw new AggregateException("Failed to clean up incoming files", errors);
    }
}
