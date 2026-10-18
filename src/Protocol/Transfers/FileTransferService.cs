using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Protocol;

internal sealed class FileTransferService : IAsyncDisposable
{
    private readonly Lock _stateLock = new();

    private readonly Dictionary<TransferKey, TransferEntry> _transfers = [];

    private bool _disposed;

    public event Action<FileTransfer>? FileOffered;
    public event Action<FileTransfer, Exception>? TransferFailed;

    public void HandleFileOffer(ReadOnlySpan<byte> payload)
    {
        var offer = Payloads.DecodeFileOffer(payload);
        if (offer is null)
        {
            return;
        }

        var key = new TransferKey(offer.Value.DeviceId, offer.Value.FileId);
        FileTransfer fileTransfer;

        lock (_stateLock)
        {
            if (_disposed)
            {
                return;
            }

            if (_transfers.ContainsKey(key))
            {
                return;
            }

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
    ) =>
        RegisterOutgoingTransfer(
            new OutgoingFileTransfer(peerDeviceId, fileName, fileSize, sha256)
        );

    private FileTransfer RegisterOutgoingTransfer(OutgoingFileTransfer file)
    {
        lock (_stateLock)
        {
            ThrowIfDisposed();

            var transfer = file.Transfer;
            var key = new TransferKey(transfer.PeerDeviceId, transfer.FileId);
            _transfers.Add(key, new TransferEntry(file));
            return transfer.Snapshot();
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
        {
            ThrowIfDisposed();
        }

        var file = await OutgoingFileTransfer
            .PrepareAsync(peerDeviceId, filePath, ct)
            .ConfigureAwait(false);
        return RegisterOutgoingTransfer(file);
    }

    public FileTransfer GetTransfer(Guid peerDeviceId, Guid fileId)
    {
        lock (_stateLock)
        {
            ThrowIfDisposed();

            return GetEntry(new TransferKey(peerDeviceId, fileId)).Transfer.Snapshot();
        }
    }

    internal void SetState(TransferKey key, TransferState state, Exception? error = null)
    {
        IncomingFileTransfer? file;
        FileTransfer snapshot;
        lock (_stateLock)
        {
            // An operation may finish unwinding after the service has been disposed.
            if (_disposed)
            {
                return;
            }

            var entry = GetEntry(key);
            if (entry.IsFinal)
            {
                return;
            }

            file = entry.ChangeState(state);
            snapshot = entry.Transfer.Snapshot();
        }
        try
        {
            file?.Dispose();
        }
        finally
        {
            if (state == TransferState.Failed && error is not null)
            {
                TransferFailed?.Invoke(snapshot, error);
            }
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
            ThrowIfDisposed();

            entry = GetEntry(key);
            entry.Require(TransferDirection.Incoming, TransferState.WaitingForAnswer);
            transfer = entry.Transfer;

            if (!accepted)
            {
                entry.ChangeState(TransferState.Rejected);
                return transfer.Snapshot();
            }
        }
        var incomingFile = IncomingFileTransfer.Create(transfer.FileSize, directory);

        try
        {
            lock (_stateLock)
            {
                ThrowIfDisposed();

                entry.Require(TransferDirection.Incoming, TransferState.WaitingForAnswer);

                entry.IncomingFile = incomingFile;
                entry.ChangeState(TransferState.Transferring);
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
        {
            return false;
        }

        lock (_stateLock)
        {
            if (_disposed)
            {
                return false;
            }

            var key = new TransferKey(answer.Value.DeviceId, answer.Value.FileId);
            if (!_transfers.TryGetValue(key, out var entry))
            {
                return false;
            }

            var transfer = entry.Transfer;
            if (transfer.Direction != TransferDirection.Outgoing)
            {
                return false;
            }

            if (transfer.State != TransferState.WaitingForAnswer)
            {
                return false;
            }

            var completion = entry.Answer;
            if (completion is null)
            {
                return false;
            }

            if (!answer.Value.Accepted)
            {
                entry.ChangeState(TransferState.Rejected);
                completion.TrySetResult(false);
                return true;
            }

            if (answer.Value.NextIndex != 0)
            {
                return false;
            }

            transfer.NextIndex = answer.Value.NextIndex;
            entry.ChangeState(TransferState.Transferring);
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
            ThrowIfDisposed();

            entry = GetEntry(key);
            completion =
                entry.Answer
                ?? throw new InvalidOperationException("Transfer is not waiting for an answer");
        }
        try
        {
            return await completion.Task.WaitAsync(ProtocolConstants.FileAnswerTimeout, ct);
        }
        catch (TimeoutException exception)
        {
            bool? answer = StopWaiting(
                entry,
                completion,
                expectedState: TransferState.WaitingForAnswer,
                state: TransferState.Failed,
                exception
            );
            if (answer.HasValue)
            {
                return answer.Value;
            }

            throw;
        }
        catch (OperationCanceledException)
        {
            bool? answer = StopWaiting(
                entry,
                completion,
                expectedState: TransferState.WaitingForAnswer,
                state: TransferState.Cancelled
            );
            if (answer.HasValue)
            {
                return answer.Value;
            }

            throw;
        }
        finally
        {
            lock (_stateLock)
            {
                if (entry.Answer == completion)
                {
                    entry.Answer = null;
                }
            }
        }
    }

    internal async Task<bool> WaitForFileResultAsync(TransferKey key, CancellationToken ct)
    {
        TransferEntry entry;
        TaskCompletionSource<bool> completion;
        lock (_stateLock)
        {
            ThrowIfDisposed();

            entry = GetEntry(key);
            entry.Require(TransferDirection.Outgoing, TransferState.Transferring);

            completion =
                entry.Result
                ?? throw new InvalidOperationException("Transfer is not waiting for a result");
        }
        bool ok;
        try
        {
            ok = await completion.Task.WaitAsync(ProtocolConstants.FileResultTimeout, ct);
        }
        catch (TimeoutException)
        {
            bool? received = StopWaiting(
                entry,
                completion,
                expectedState: TransferState.Transferring,
                state: TransferState.Paused
            );
            if (received.HasValue)
            {
                ok = received.Value;
            }
            else
            {
                throw;
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            bool? received = StopWaiting(
                entry,
                completion,
                TransferState.Transferring,
                TransferState.Cancelled
            );
            if (received.HasValue)
            {
                ok = received.Value;
            }
            else
            {
                throw;
            }
        }
        finally
        {
            lock (_stateLock)
            {
                if (entry.Result == completion)
                {
                    entry.Result = null;
                }
            }
        }
        if (ok)
        {
            SetState(key, TransferState.Completed);
        }
        else
        {
            SetState(
                key,
                TransferState.Failed,
                new InvalidDataException("Receiver could not verify or save the file")
            );
        }

        return ok;
    }

    private bool? StopWaiting(
        TransferEntry entry,
        TaskCompletionSource<bool> completion,
        TransferState expectedState,
        TransferState state,
        Exception? error = null
    )
    {
        FileTransfer snapshot;
        lock (_stateLock)
        {
            if (completion.Task.IsCompletedSuccessfully)
            {
                return completion.Task.Result;
            }

            if (_disposed || entry.Transfer.State != expectedState)
            {
                return null;
            }

            entry.ChangeState(state);
            snapshot = entry.Transfer.Snapshot();
        }
        if (error is not null)
        {
            TransferFailed?.Invoke(snapshot, error);
        }

        return null;
    }

    public bool HandleChunk(ReadOnlySpan<byte> payload)
    {
        var chunk = Payloads.DecodeChunk(payload);
        if (chunk is null)
        {
            return false;
        }

        var key = new TransferKey(chunk.Value.DeviceId, chunk.Value.FileId);
        TransferEntry entry;
        IncomingFileTransfer incomingFile;
        lock (_stateLock)
        {
            if (_disposed || !_transfers.TryGetValue(key, out var value))
            {
                return false;
            }

            entry = value;
            if (entry.Transfer.Direction != TransferDirection.Incoming)
            {
                return false;
            }

            if (entry.Transfer.State is TransferState.Verifying or TransferState.Completed)
            {
                if (chunk.Value.Index >= entry.Transfer.NextIndex)
                {
                    return false;
                }

                ulong remaining =
                    entry.Transfer.FileSize
                    - (ulong)chunk.Value.Index * (ulong)ProtocolConstants.ChunkSize;
                return chunk.Value.Data.Length
                    == (int)Math.Min((ulong)ProtocolConstants.ChunkSize, remaining);
            }

            if (entry.Transfer.State != TransferState.Transferring || entry.IncomingFile is null)
            {
                return false;
            }

            incomingFile = entry.IncomingFile;
        }
        try
        {
            if (
                !incomingFile.TryWriteChunk(chunk.Value.Index, chunk.Value.Data, out uint nextIndex)
            )
            {
                return false;
            }

            lock (_stateLock)
            {
                if (
                    _disposed
                    || entry.IncomingFile != incomingFile
                    || entry.Transfer.State != TransferState.Transferring
                )
                {
                    return false;
                }

                entry.Transfer.NextIndex = Math.Max(entry.Transfer.NextIndex, nextIndex);
                return true;
            }
        }
        catch (IOException exception)
        {
            SetState(key, TransferState.Failed, exception);
            return false;
        }
    }

    public bool HandleFileResult(ReadOnlySpan<byte> payload)
    {
        var fileResult = Payloads.DecodeFileResult(payload);

        if (fileResult is null)
        {
            return false;
        }

        lock (_stateLock)
        {
            var key = new TransferKey(fileResult.Value.DeviceId, fileResult.Value.FileId);

            if (_disposed || !_transfers.TryGetValue(key, out var entry))
            {
                return false;
            }

            if (entry.Transfer.Direction != TransferDirection.Outgoing)
            {
                return false;
            }

            if (entry.ReceivedResult.HasValue)
            {
                return entry.ReceivedResult.Value == fileResult.Value.Ok;
            }

            if (entry.Transfer.State != TransferState.Transferring)
            {
                return false;
            }

            var completion = entry.Result;
            if (completion is null)
            {
                return false;
            }

            if (!completion.TrySetResult(fileResult.Value.Ok))
            {
                return false;
            }

            entry.ReceivedResult = fileResult.Value.Ok;
            return true;
        }
    }

    internal Task<bool>? HandleFileDone(ReadOnlySpan<byte> payload, CancellationToken ct)
    {
        var fileDone = Payloads.DecodeFileDone(payload);
        if (fileDone is null)
        {
            return null;
        }

        var key = new TransferKey(fileDone.Value.DeviceId, fileDone.Value.FileId);
        lock (_stateLock)
        {
            if (!_transfers.TryGetValue(key, out var entry))
            {
                return null;
            }

            if (entry.Transfer.Direction != TransferDirection.Incoming)
            {
                return null;
            }

            if (entry.IncomingCompletion is not null)
            {
                return entry.IncomingCompletion;
            }

            if (entry.Transfer.State != TransferState.Transferring || entry.IncomingFile == null)
            {
                return null;
            }

            return CompleteIncomingTransferAsync(key, ct);
        }
    }

    internal Task<bool> CompleteIncomingTransferAsync(TransferKey key, CancellationToken ct)
    {
        lock (_stateLock)
        {
            ThrowIfDisposed();

            var entry = GetEntry(key);

            if (entry.Transfer.Direction != TransferDirection.Incoming)
            {
                throw new InvalidOperationException("Transfer must be incoming");
            }

            if (entry.IncomingCompletion is not null)
            {
                return entry.IncomingCompletion;
            }

            entry.Require(TransferDirection.Incoming, TransferState.Transferring);

            var incomingFile =
                entry.IncomingFile
                ?? throw new InvalidOperationException("Transfer has no incoming file");
            byte[] expectedSha256 = entry.Transfer.Sha256;
            string fileName = entry.Transfer.FileName;

            entry.ChangeState(TransferState.Verifying);
            entry.IncomingCompletion = Task.Run(() =>
                CompleteIncomingTransferCoreAsync(key, incomingFile, expectedSha256, fileName, ct)
            );

            return entry.IncomingCompletion;
        }
    }

    private async Task<bool> CompleteIncomingTransferCoreAsync(
        TransferKey key,
        IncomingFileTransfer incomingFile,
        byte[] expectedSha256,
        string fileName,
        CancellationToken ct
    )
    {
        try
        {
            bool verified = await incomingFile
                .VerifyAsync(expectedSha256, ct)
                .ConfigureAwait(false);
            if (!verified)
            {
                SetState(
                    key,
                    TransferState.Failed,
                    new InvalidDataException("Incoming file verification failed")
                );
                return false;
            }
            await incomingFile.SaveAsync(fileName, ct).ConfigureAwait(false);
            SetState(key, TransferState.Completed);
            return true;
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            SetState(key, TransferState.Cancelled);
            throw;
        }
        catch (Exception exception) when (exception is IOException or UnauthorizedAccessException)
        {
            SetState(key, TransferState.Failed, exception);
            return false;
        }
        catch (Exception exception)
        {
            SetState(key, TransferState.Failed, exception);
            throw;
        }
    }

    internal async Task<FileStream> OpenOutgoingFileAsync(TransferKey key, CancellationToken ct)
    {
        OutgoingFileTransfer file;
        lock (_stateLock)
        {
            ThrowIfDisposed();

            var entry = GetEntry(key);
            entry.Require(TransferDirection.Outgoing, TransferState.Transferring);
            file =
                entry.OutgoingFile
                ?? throw new InvalidOperationException("Transfer has no outgoing file");
        }

        var stream = await file.OpenAsync(ct).ConfigureAwait(false);
        try
        {
            lock (_stateLock)
            {
                ThrowIfDisposed();

                GetEntry(key).Require(TransferDirection.Outgoing, TransferState.Transferring);
            }
            return stream;
        }
        catch
        {
            await stream.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    internal void ConfirmOutgoingChunk(TransferKey key, uint index)
    {
        lock (_stateLock)
        {
            ThrowIfDisposed();

            var entry = GetEntry(key);
            entry.Require(TransferDirection.Outgoing, TransferState.Transferring);
            var file =
                entry.OutgoingFile
                ?? throw new InvalidOperationException("Transfer has no outgoing file");
            file.ConfirmChunk(index);
        }
    }

    private void ThrowIfDisposed()
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(
                nameof(FileTransferService),
                "Object has been disposed"
            );
        }
    }

    private TransferEntry GetEntry(TransferKey key) =>
        _transfers.TryGetValue(key, out var entry)
            ? entry
            : throw new ArgumentException("Transfer not found", nameof(key));

    public async ValueTask DisposeAsync()
    {
        List<IncomingFileTransfer> files;
        List<Task<bool>> incomingCompletions = [];
        lock (_stateLock)
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            files = [];
            foreach (var entry in _transfers.Values)
            {
                entry.Answer?.TrySetCanceled();
                entry.Result?.TrySetCanceled();

                if (entry.IncomingFile is not null)
                {
                    files.Add(entry.IncomingFile);
                }

                if (entry.IncomingCompletion is not null)
                {
                    incomingCompletions.Add(entry.IncomingCompletion);
                }
            }
            _transfers.Clear();
        }
        var errors = new List<Exception>();

        foreach (var completion in incomingCompletions)
        {
            try
            {
                await completion.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                continue;
            }
            catch (Exception exception)
            {
                errors.Add(exception);
            }
        }

        foreach (var file in files)
        {
            try
            {
                await file.DisposeAsync();
            }
            catch (Exception exception)
            {
                errors.Add(exception);
            }
        }

        if (errors.Count > 0)
        {
            throw new AggregateException("Failed to delete temporary files", errors);
        }
    }
}
