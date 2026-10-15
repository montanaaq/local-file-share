using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
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
    private readonly Dictionary<TransferKey, FileTransfer> _transfers = [];

    private bool _disposed;

    public event Action<FileTransfer>? FileOffered;

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

        ArgumentOutOfRangeException.ThrowIfGreaterThan(
            fileStream.Length,
            ProtocolConstants.MaxFileSize
        );

        var fileName = Path.GetFileName(filePath);
        var sha256 = await SHA256.HashDataAsync(fileStream, ct);

        return CreateOutgoingTransfer(peerDeviceId, fileName, (ulong)fileStream.Length, sha256);
    }

    public void SetState(Guid peerDeviceId, Guid fileId, TransferState state)
    {
        IncomingFileTransfer? file = null;
        lock (_stateLock)
        {
            var key = new TransferKey(peerDeviceId, fileId);

            if (state is TransferState.Failed or TransferState.Cancelled)
                _incomingFiles.Remove(key, out file);

            if (
                state is TransferState.Failed or TransferState.Cancelled
                && _pendingAnswers.Remove(key, out var completion)
            )
                completion.TrySetCanceled();

            _transfers[key].State = state;
        }
        if (file is not null)
        {
            file.Stream.Dispose();
            File.Delete(file.PartPath);
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
        var incomingFile = CreateIncomingFile(transfer, directory);

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
            incomingFile.Stream.Dispose();
            File.Delete(incomingFile.PartPath);
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

    private static IncomingFileTransfer CreateIncomingFile(FileTransfer transfer, string directory)
    {
        var chunkSize = (ulong)ProtocolConstants.ChunkSize;
        var chunkCount = (int)((transfer.FileSize + chunkSize - 1) / chunkSize);

        Directory.CreateDirectory(directory);
        var partPath = Path.Combine(directory, $"{Guid.NewGuid():N}.part");

        var stream = new FileStream(
            partPath,
            FileMode.CreateNew,
            FileAccess.ReadWrite,
            FileShare.None
        );
        try
        {
            return new IncomingFileTransfer(partPath, stream, chunkCount);
        }
        catch
        {
            stream.Dispose();
            File.Delete(partPath);
            throw;
        }
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
                File.Delete(file.PartPath);
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
