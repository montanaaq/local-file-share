using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace Protocol;

internal sealed class FileExchange : IAsyncDisposable, ITransferSource
{
    private readonly IMessageTransport _transport;
    private readonly Func<Guid, IPEndPoint> _resolveEndpoint;
    private readonly Lock _stateLock = new();
    private readonly ProcessedMessages _processedMessages = new();
    private readonly HashSet<Task> _fileResultTasks = [];
    private readonly HashSet<Task> _outgoingCompletions = [];
    private readonly NotificationDispatcher _notifications;
    private readonly bool _ownsNotifications;
    private Task? _disposeTask;
    private readonly CancellationTokenSource _stopCts;
    private bool _disposed;

    public FileExchange(
        IMessageTransport transport,
        Guid deviceId,
        Func<Guid, IPEndPoint> resolveEndpoint,
        CancellationToken stopping,
        NotificationDispatcher? notifications = null
    )
    {
        _transport = transport;
        DeviceId = deviceId;
        _resolveEndpoint = resolveEndpoint;
        _stopCts = CancellationTokenSource.CreateLinkedTokenSource(stopping);
        _ownsNotifications = notifications is null;
        _notifications = notifications ?? new NotificationDispatcher();
        Transfers.FileOffered += transfer =>
            _notifications.Post(FileOffered, handler => handler(transfer));
        Transfers.TransferFailed += (transfer, error) =>
            _notifications.Post(TransferFailed, handler => handler(transfer, error));
    }

    private Guid DeviceId { get; }
    public FileTransferService Transfers { get; } = new();

    public FileTransfer GetTransfer(Guid peerDeviceId, Guid fileId) =>
        Transfers.GetTransfer(peerDeviceId, fileId);

    public event Action<FileTransfer>? FileOffered;
    public event Action<FileTransfer, Exception>? TransferFailed;
    public event Action<IPEndPoint, SocketException>? NetworkError;
    public event Action<FileTransfer, Exception>? FileResultDeliveryFailed;

    public Task<FileTransfer> OfferFileAsync(
        Guid peerDeviceId,
        string filePath,
        CancellationToken ct
    ) => StartOutgoingOperation(() => OfferFileCoreAsync(peerDeviceId, filePath, ct));

    private async Task<FileTransfer> OfferFileCoreAsync(
        Guid peerDeviceId,
        string filePath,
        CancellationToken ct
    )
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct, _stopCts.Token);
        ct = cts.Token;
        ct.ThrowIfCancellationRequested();
        ThrowIfDisposed();
        _resolveEndpoint(peerDeviceId);
        var transfer = await Transfers.PrepareOutgoingTransferAsync(peerDeviceId, filePath, ct);
        try
        {
            byte[] payload = Payloads.EncodeFileOffer(
                DeviceId,
                transfer.FileId,
                transfer.FileSize,
                transfer.Sha256,
                transfer.FileName
            );
            await SendReliableAsync(MessageType.FileOffer, peerDeviceId, payload, ct);
        }
        catch (Exception exception)
        {
            Transfers.SetState(
                new TransferKey(peerDeviceId, transfer.FileId),
                exception is OperationCanceledException
                    ? TransferState.Cancelled
                    : TransferState.Failed,
                exception
            );
            throw;
        }
        bool accepted = await Transfers.WaitForFileAnswerAsync(peerDeviceId, transfer.FileId, ct);
        if (!accepted)
        {
            return Transfers.GetTransfer(peerDeviceId, transfer.FileId);
        }

        var key = new TransferKey(peerDeviceId, transfer.FileId);
        try
        {
            await SendChunksAsync(key, ct);
            await SendFileDoneAsync(key, ct);
            await Transfers.WaitForFileResultAsync(key, ct);
        }
        catch (TimeoutException)
        {
            Transfers.SetState(key, TransferState.Paused);
        }
        catch (SocketException exception) when (exception.IsUnreachable())
        {
            Transfers.SetState(key, TransferState.Paused);
            var endpoint = _resolveEndpoint(peerDeviceId);
            _notifications.Post(NetworkError, handler => handler(endpoint, exception));
        }
        catch (Exception exception)
        {
            Transfers.SetState(
                key,
                exception is OperationCanceledException
                    ? TransferState.Cancelled
                    : TransferState.Failed,
                exception
            );
            throw;
        }
        return Transfers.GetTransfer(peerDeviceId, transfer.FileId);
    }

    public Task<FileTransfer> AnswerFileOfferAsync(
        Guid peerDeviceId,
        Guid fileId,
        bool accepted,
        string directory,
        CancellationToken ct
    ) =>
        StartOutgoingOperation(() =>
            AnswerFileOfferCoreAsync(peerDeviceId, fileId, accepted, directory, ct)
        );

    private async Task<FileTransfer> AnswerFileOfferCoreAsync(
        Guid peerDeviceId,
        Guid fileId,
        bool accepted,
        string directory,
        CancellationToken ct
    )
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct, _stopCts.Token);
        ct = cts.Token;
        ct.ThrowIfCancellationRequested();
        ThrowIfDisposed();
        _resolveEndpoint(peerDeviceId);
        var transfer = Transfers.SetIncomingDecision(peerDeviceId, fileId, accepted, directory);
        try
        {
            byte[] payload = Payloads.EncodeFileAnswer(DeviceId, fileId, accepted, nextIndex: 0);
            await SendReliableAsync(MessageType.FileAnswer, peerDeviceId, payload, ct);
        }
        catch (Exception exception)
        {
            Transfers.SetState(
                new TransferKey(peerDeviceId, fileId),
                exception is OperationCanceledException
                    ? TransferState.Cancelled
                    : TransferState.Failed,
                exception
            );
            throw;
        }

        return transfer;
    }

    private async Task SendChunksAsync(TransferKey key, CancellationToken ct)
    {
        await using var stream = await Transfers.OpenOutgoingFileAsync(key, ct);

        var transfer = Transfers.GetTransfer(key.PeerDeviceId, key.FileId);
        uint index = transfer.NextIndex;
        long offset = (long)index * ProtocolConstants.ChunkSize;

        stream.Position = offset;
        byte[] buffer = new byte[ProtocolConstants.ChunkSize];

        while (offset < (long)transfer.FileSize)
        {
            ct.ThrowIfCancellationRequested();
            int length = (int)Math.Min(buffer.Length, (long)transfer.FileSize - offset);
            await stream.ReadExactlyAsync(buffer.AsMemory(0, length), ct);

            byte[] payload = Payloads.EncodeChunk(
                DeviceId,
                key.FileId,
                index,
                buffer.AsSpan(0, length)
            );
            await SendReliableAsync(MessageType.Chunk, key.PeerDeviceId, payload, ct);
            Transfers.ConfirmOutgoingChunk(key, index);
            index++;
            offset += length;
        }
    }

    private async Task SendFileDoneAsync(TransferKey key, CancellationToken ct)
    {
        byte[] payload = Payloads.EncodeFileDone(DeviceId, key.FileId);
        await SendReliableAsync(MessageType.FileDone, key.PeerDeviceId, payload, ct)
            .ConfigureAwait(false);
    }

    private async Task SendFileResultAsync(TransferKey key, bool ok, CancellationToken ct)
    {
        byte[] payload = Payloads.EncodeFileResult(DeviceId, key.FileId, ok);
        lock (_stateLock)
        {
            if (_disposed)
            {
                return;
            }
        }
        try
        {
            ct.ThrowIfCancellationRequested();
            await SendReliableAsync(MessageType.FileResult, key.PeerDeviceId, payload, ct)
                .ConfigureAwait(false);
        }
        catch (TimeoutException exception)
        {
            var transfer = Transfers.GetTransfer(key.PeerDeviceId, key.FileId);
            _notifications.Post(FileResultDeliveryFailed, handler => handler(transfer, exception));
        }
        catch (SocketException exception) when (exception.IsUnreachable())
        {
            var transfer = Transfers.GetTransfer(key.PeerDeviceId, key.FileId);
            _notifications.Post(FileResultDeliveryFailed, handler => handler(transfer, exception));
        }
    }

    private async Task CompleteAndSendFileResultAsync(
        TransferKey key,
        Task<bool> completion,
        CancellationToken ct
    )
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct, _stopCts.Token);
        bool ok = await completion.ConfigureAwait(false);
        await SendFileResultAsync(key, ok, cts.Token).ConfigureAwait(false);
    }

    private async Task ObserveFileResultsAsync(bool completedOnly, CancellationToken ct)
    {
        Task[] tasks;
        lock (_stateLock)
        {
            tasks = _fileResultTasks.Where(task => !completedOnly || task.IsCompleted).ToArray();
            foreach (var task in tasks)
            {
                _fileResultTasks.Remove(task);
            }
        }
        try
        {
            await Task.WhenAll(tasks).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            return;
        }
    }

    // Called sequentially by the receive loop after checking the sender and endpoint.
    public async Task HandlePacketAsync(
        Packet packet,
        IPEndPoint from,
        Guid senderDeviceId,
        CancellationToken ct
    )
    {
        Task<bool>? completion = null;
        TransferKey completionKey = default;
        switch (packet.Type)
        {
            case MessageType.FileOffer:
                Transfers.HandleFileOffer(packet.Payload);
                break;
            case MessageType.FileAnswer:
                if (!_processedMessages.Contains(senderDeviceId, packet.Seq))
                {
                    if (!Transfers.HandleFileAnswer(packet.Payload))
                    {
                        return;
                    }

                    _processedMessages.Add(senderDeviceId, packet.Seq);
                }
                break;
            case MessageType.FileResult:
                if (!_processedMessages.Contains(senderDeviceId, packet.Seq))
                {
                    if (!Transfers.HandleFileResult(packet.Payload))
                    {
                        return;
                    }

                    _processedMessages.Add(senderDeviceId, packet.Seq);
                }
                break;
            case MessageType.Chunk:
                if (!Transfers.HandleChunk(packet.Payload))
                {
                    return;
                }

                break;
            case MessageType.FileDone:
                var fileDone = Payloads.DecodeFileDone(packet.Payload);
                if (fileDone is null)
                {
                    return;
                }

                completion = Transfers.HandleFileDone(packet.Payload, ct);
                if (completion is null)
                {
                    return;
                }

                completionKey = new TransferKey(senderDeviceId, fileDone.Value.FileId);
                break;
            default:
                return;
        }

        try
        {
            await _transport.SendAckAsync(packet.Seq, from, DeviceId).ConfigureAwait(false);
        }
        catch (SocketException exception) when (exception.IsUnreachable())
        {
            _notifications.Post(NetworkError, handler => handler(from, exception));
        }

        if (completion is not null)
        {
            var delivery = CompleteAndSendFileResultAsync(completionKey, completion, ct);
            lock (_stateLock)
            {
                _fileResultTasks.Add(delivery);
            }
        }
    }

    private Task SendReliableAsync(
        MessageType type,
        Guid peerDeviceId,
        byte[] payload,
        CancellationToken ct
    ) =>
        _transport.SendReliableAsync(
            new Packet(type, _transport.NextSeq(), payload),
            _resolveEndpoint(peerDeviceId),
            peerDeviceId,
            ct
        );

    public Task CheckCompletedOperationsAsync(CancellationToken ct) =>
        ObserveFileResultsAsync(completedOnly: true, ct);

    public Task WaitForPendingOperationsAsync(CancellationToken ct) =>
        ObserveFileResultsAsync(completedOnly: false, ct);

    private Task<FileTransfer> StartOutgoingOperation(Func<Task<FileTransfer>> start)
    {
        lock (_stateLock)
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(nameof(FileExchange), "Object has been disposed");
            }

            _outgoingCompletions.RemoveWhere(task => task.IsCompleted);
            // Register the task before file operations so shutdown can wait for it.
            var operation = Task.Run(start);
            // Errors belong to the caller; shutdown only waits for completion.
            _outgoingCompletions.Add(
                operation.ContinueWith(
                    _ => { },
                    CancellationToken.None,
                    TaskContinuationOptions.ExecuteSynchronously,
                    TaskScheduler.Default
                )
            );
            return operation;
        }
    }

    private void ThrowIfDisposed()
    {
        lock (_stateLock)
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(nameof(FileExchange), "Object has been disposed");
            }
        }
    }

    public ValueTask DisposeAsync()
    {
        lock (_stateLock)
        {
            if (_disposeTask is null)
            {
                _disposed = true;
                _disposeTask = DisposeCoreAsync();
            }
            return new ValueTask(_disposeTask);
        }
    }

    private async Task DisposeCoreAsync()
    {
        _stopCts.Cancel();
        Task[] outgoing;
        lock (_stateLock)
        {
            outgoing = _outgoingCompletions.ToArray();
        }

        try
        {
            await Task.WhenAll(outgoing).ConfigureAwait(false);
            await WaitForPendingOperationsAsync(_stopCts.Token).ConfigureAwait(false);
        }
        finally
        {
            try
            {
                await Transfers.DisposeAsync().ConfigureAwait(false);
            }
            finally
            {
                _stopCts.Dispose();
                if (_ownsNotifications)
                {
                    await _notifications.DisposeAsync().ConfigureAwait(false);
                }
            }
        }
    }
}
