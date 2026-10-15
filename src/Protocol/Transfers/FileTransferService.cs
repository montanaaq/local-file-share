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

    private readonly Dictionary<TransferKey, TaskCompletionSource<bool>> _pendingAnswers = [];
    private readonly Dictionary<TransferKey, IncomingFileTransfer> _incomingFiles = [];
    private readonly Dictionary<TransferKey, string> _outgoingPaths = [];
    private readonly Dictionary<TransferKey, FileTransfer> _transfers = [];

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
            ObjectDisposedException.ThrowIf(_disposed, this);

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

            _transfers.Add(key, fileTransfer);
        }

        FileOffered?.Invoke(fileTransfer);
    }

    public FileTransfer CreateOutgoingTransfer(
        Guid peerDeviceId,
        string fileName,
        ulong fileSize,
        byte[] sha256
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
            fileName == string.Empty
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
                Sha256 = sha256,
                Direction = TransferDirection.Outgoing,
                State = TransferState.WaitingForAnswer,
            };
            var key = new TransferKey(fileTransfer.PeerDeviceId, fileTransfer.FileId);
            _transfers.Add(key, fileTransfer);

            _pendingAnswers.Add(
                key,
                new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously)
            );
        }

        return fileTransfer;
    }

    public async Task<FileTransfer> PrepareOutgoingTransferAsync(
        Guid peerDeviceId,
        string filePath,
        CancellationToken ct
    )
    {
        await using var fileStream = new FileStream(
            filePath,
            FileMode.Open,
            FileAccess.Read,
            FileShare.Read
        );

        var fileSize = fileStream.Length;

        ArgumentOutOfRangeException.ThrowIfGreaterThan(
            fileStream.Length,
            ProtocolConstants.MaxFileSize
        );

        var fileName = Path.GetFileName(filePath);
        var sha256 = await SHA256.HashDataAsync(fileStream, ct);
        var fullPath = Path.GetFullPath(filePath);

        var transfer = CreateOutgoingTransfer(peerDeviceId, fileName, (ulong)fileSize, sha256);
        var key = new TransferKey(peerDeviceId, transfer.FileId);

        try
        {
            lock (_stateLock)
            {
                ObjectDisposedException.ThrowIf(_disposed, this);

                _outgoingPaths.Add(key, fullPath);
            }
            return transfer;
        }
        catch
        {
            SetState(key, TransferState.Failed);
            throw;
        }
    }

    internal void SetState(TransferKey key, TransferState state)
    {
        IncomingFileTransfer? file = null;
        lock (_stateLock)
        {
            if (state is TransferState.Failed or TransferState.Cancelled)
                _incomingFiles.Remove(key, out file);

            if (
                state is TransferState.Failed or TransferState.Cancelled
                && _pendingAnswers.Remove(key, out var completion)
            )
                completion.TrySetCanceled();

            _transfers[key].State = state;
        }
        file?.Dispose();
    }

    public FileTransfer SetIncomingDecision(
        Guid peerDeviceId,
        Guid fileId,
        bool accepted,
        string directory
    )
    {
        var key = new TransferKey(peerDeviceId, fileId);
        FileTransfer transfer;
        lock (_stateLock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            if (!_transfers.TryGetValue(key, out var value))
                throw new ArgumentException("Transfer not exists");

            transfer = value;

            if (transfer.Direction != TransferDirection.Incoming)
                throw new InvalidOperationException("Transfer must be incoming");

            if (transfer.State != TransferState.WaitingForAnswer)
                throw new InvalidOperationException("Transfer is not waiting for a decision");

            if (!accepted)
            {
                transfer.State = TransferState.Rejected;
                return transfer;
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

                _incomingFiles.Add(key, incomingFile);
                transfer.State = TransferState.Transferring;
            }
            return transfer;
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
            var key = new TransferKey(answer.Value.DeviceId, answer.Value.FileId);
            if (!_transfers.TryGetValue(key, out var transfer))
                return false;

            if (transfer.Direction != TransferDirection.Outgoing)
                return false;

            if (transfer.State != TransferState.WaitingForAnswer)
                return false;

            if (!_pendingAnswers.TryGetValue(key, out var completion))
                return false;

            if (!answer.Value.Accepted)
            {
                transfer.State = TransferState.Rejected;
                completion.TrySetResult(false);
                return true;
            }

            if (answer.Value.NextIndex != 0)
                return false;

            transfer.NextIndex = answer.Value.NextIndex;
            transfer.State = TransferState.Transferring;
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
        TaskCompletionSource<bool> completion;

        lock (_stateLock)
        {
            if (!_pendingAnswers.TryGetValue(key, out var value))
                throw new InvalidOperationException("Completion not exists");

            completion = value;
        }
        try
        {
            return await completion.Task.WaitAsync(ProtocolConstants.FileAnswerTimeout, ct);
        }
        catch (TimeoutException)
        {
            lock (_stateLock)
            {
                if (completion.Task.IsCompletedSuccessfully)
                    return completion.Task.Result;

                _transfers.TryGetValue(key, out var transfer);
                transfer?.State = TransferState.Failed;
                _pendingAnswers.Remove(key);
            }
            throw;
        }
        catch (OperationCanceledException)
        {
            lock (_stateLock)
            {
                _transfers.TryGetValue(key, out var transfer);
                if (transfer?.State == TransferState.WaitingForAnswer)
                {
                    transfer?.State = TransferState.Failed;
                    _pendingAnswers.Remove(key);
                }
            }
            throw;
        }
        finally
        {
            lock (_stateLock)
            {
                _pendingAnswers.Remove(key);
            }
        }
    }

    public bool HandleChunk(ReadOnlySpan<byte> payload)
    {
        var chunk = Payloads.DecodeChunk(payload);
        if (chunk is null)
            return false;
        var key = new TransferKey(chunk.Value.DeviceId, chunk.Value.FileId);
        FileTransfer? transfer = null;
        try
        {
            lock (_stateLock)
            {
                if (_disposed)
                    return false;

                if (
                    !_transfers.TryGetValue(key, out transfer)
                    || transfer.State != TransferState.Transferring
                    || transfer.Direction != TransferDirection.Incoming
                )
                    return false;

                if (!_incomingFiles.TryGetValue(key, out var incomingFile))
                    return false;

                if (!incomingFile.TryWriteChunk(chunk.Value.Index, chunk.Value.Data))
                    return false;

                transfer.NextIndex = incomingFile.NextIndex;
                return true;
            }
        }
        catch (IOException ioEx) when (transfer is not null)
        {
            SetState(key, TransferState.Failed);
            TransferFailed?.Invoke(transfer, ioEx);
            return false;
        }
    }

    internal FileStream OpenOutgoingFile(TransferKey key)
    {
        string fullPath;
        lock (_stateLock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            GetOutgoingTransferForSending(key);

            if (!_outgoingPaths.TryGetValue(key, out var path))
                throw new ArgumentException("Path not found");
            fullPath = path;
        }
        return new FileStream(fullPath, FileMode.Open, FileAccess.Read, FileShare.Read);
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
        if (!_transfers.TryGetValue(key, out var transfer))
            throw new ArgumentException("Transfer not found", nameof(key));

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
            files = [.. _incomingFiles.Values];
            _incomingFiles.Clear();

            foreach (var completion in _pendingAnswers.Values)
                completion.TrySetCanceled();

            _pendingAnswers.Clear();
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
