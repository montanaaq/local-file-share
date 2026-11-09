using System;
using System.Collections.Generic;
using System.IO;
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
    private readonly Action<Guid, IPEndPoint>? _acknowledged;
    private readonly Lock _stateLock = new();
    private readonly ProcessedMessages _processedMessages = new();
    private readonly HashSet<Task> _backgroundTasks = [];
    private readonly HashSet<Task> _outgoingCompletions = [];
    private readonly HashSet<TransferKey> _activeOutgoing = [];
    private readonly HashSet<(TransferKey Key, MessageType Type)> _activeReplies = [];
    private readonly Dictionary<TransferKey, long> _lastProbes = [];
    private readonly NotificationDispatcher _notifications;
    private readonly bool _ownsNotifications;
    private readonly TimeProvider _timeProvider;
    private readonly CancellationTokenSource _stopCts;
    private CancellationToken _exchangeToken;
    private Task? _disposeTask;
    private bool _disposed;

    public FileExchange(
        IMessageTransport transport,
        Guid deviceId,
        Func<Guid, IPEndPoint> resolveEndpoint,
        CancellationToken stopping,
        NotificationDispatcher? notifications = null,
        TimeProvider? timeProvider = null,
        Action<Guid, IPEndPoint>? acknowledged = null,
        CancellationToken exchangeToken = default
    )
    {
        _transport = transport;
        DeviceId = deviceId;
        _resolveEndpoint = resolveEndpoint;
        _acknowledged = acknowledged;
        _timeProvider = timeProvider ?? TimeProvider.System;
        _stopCts = CancellationTokenSource.CreateLinkedTokenSource(stopping);
        _exchangeToken = exchangeToken;
        _ownsNotifications = notifications is null;
        _notifications = notifications ?? new NotificationDispatcher();
        Transfers = new FileTransferService(_timeProvider);
        Transfers.FileOffered += transfer =>
            _notifications.Post(FileOffered, handler => handler(transfer));
        Transfers.TransferChanged += transfer =>
            _notifications.Post(TransferChanged, handler => handler(transfer));
        Transfers.TransferFailed += (transfer, error) =>
            _notifications.Post(TransferFailed, handler => handler(transfer, error));
    }

    private Guid DeviceId { get; }
    public FileTransferService Transfers { get; }

    private bool IsExchangeEnabled
    {
        get
        {
            lock (_stateLock)
            {
                return !_exchangeToken.IsCancellationRequested;
            }
        }
    }

    public FileTransfer GetTransfer(TransferKey key) => Transfers.GetTransfer(key);

    public FileTransfer[] GetTransfers() => Transfers.GetTransfers();

    public event Action<FileTransfer>? FileOffered;
    public event Action<FileTransfer>? TransferChanged;
    public event Action<FileTransfer, Exception>? TransferFailed;
    public event Action<IPEndPoint, SocketException>? NetworkError;
    public event Action<FileTransfer, Exception>? FileResultDeliveryFailed;

    /// <summary>
    /// Принимает токен сетевой сессии от ExchangeService. Отменённый токен переводит передачи в паузу.
    /// FileExchange не создаёт и не отменяет источник этого токена.
    /// </summary>
    /// <param name="exchangeToken">Токен текущего периода включённого обмена.</param>
    public void SetExchangeToken(CancellationToken exchangeToken)
    {
        lock (_stateLock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            _exchangeToken = exchangeToken;
            if (exchangeToken.IsCancellationRequested)
            {
                Transfers.PauseAll();
            }
        }
    }

    public FileTransfer CancelPausedTransfer(TransferKey key)
    {
        Transfers.CancelPaused(key);
        return Transfers.GetTransfer(key);
    }

    public Task<FileTransfer> OfferFileAsync(
        Guid peerDeviceId,
        string filePath,
        CancellationToken ct
    ) =>
        StartOutgoingOperation(exchangeToken =>
            OfferFileCoreAsync(peerDeviceId, filePath, ct, exchangeToken)
        );

    /// <summary>
    /// Для peerDeviceId подготавливает файл по filePath, отправляет предложение и ожидает отдельного решения.
    /// callerToken отменяет вызов; отключение обмена сохраняет передачу в паузе.
    /// </summary>
    private async Task<FileTransfer> OfferFileCoreAsync(
        Guid peerDeviceId,
        string filePath,
        CancellationToken callerToken,
        CancellationToken exchangeToken
    )
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(
            callerToken,
            exchangeToken,
            _stopCts.Token
        );
        var ct = cts.Token;
        ct.ThrowIfCancellationRequested();

        _resolveEndpoint(peerDeviceId);
        var transfer = await Transfers.PrepareOutgoingTransferAsync(peerDeviceId, filePath, ct);
        var key = new TransferKey(peerDeviceId, transfer.FileId);
        lock (_stateLock)
        {
            _activeOutgoing.Add(key);
        }
        try
        {
            try
            {
                await SendOfferAsync(transfer, ct).ConfigureAwait(false);
                bool accepted = await Transfers.WaitForFileAnswerAsync(key, ct);
                if (!accepted)
                {
                    return Transfers.GetTransfer(key);
                }
            }
            catch (OperationCanceledException) when (IsExchangeInterruption(callerToken))
            {
                Transfers.SetState(key, TransferState.Paused);
                return Transfers.GetTransfer(key);
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

            await SendFileContentsAsync(key, ct, callerToken).ConfigureAwait(false);
            return Transfers.GetTransfer(key);
        }
        finally
        {
            lock (_stateLock)
            {
                _activeOutgoing.Remove(key);
            }
        }
    }

    public Task<FileTransfer> AnswerFileOfferAsync(
        TransferKey key,
        bool accepted,
        string directory,
        CancellationToken ct
    ) =>
        StartOutgoingOperation(exchangeToken =>
            AnswerFileOfferCoreAsync(key, accepted, directory, ct, exchangeToken)
        );

    /// <summary>
    /// Применяет решение accepted к предложению key; directory задаёт папку приёма.
    /// callerToken отменяет вызов. Потеря ACK решения не отменяет файл, если данные уже поступают или проверяются.
    /// </summary>
    private async Task<FileTransfer> AnswerFileOfferCoreAsync(
        TransferKey key,
        bool accepted,
        string directory,
        CancellationToken callerToken,
        CancellationToken exchangeToken
    )
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(
            callerToken,
            exchangeToken,
            _stopCts.Token
        );
        var ct = cts.Token;
        ct.ThrowIfCancellationRequested();

        _resolveEndpoint(key.PeerDeviceId);
        var transfer = Transfers.SetIncomingDecision(key, accepted, directory);
        try
        {
            await SendAnswerAsync(key, accepted, transfer.NextIndex, ct).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (IsExchangeInterruption(callerToken))
        {
            Transfers.SetState(key, TransferState.Paused);
            return Transfers.GetTransfer(key);
        }
        catch (TimeoutException)
            when (accepted
                && Transfers.GetTransfer(key)
                    is { NextIndex: > 0 }
                        or { State: TransferState.Verifying or TransferState.Completed }
            ) { }
        catch (OperationCanceledException cancelException)
        {
            Transfers.SetState(key, TransferState.Cancelled, cancelException);
            throw;
        }
        catch (Exception exception)
        {
            Transfers.SetState(key, TransferState.Failed, exception);
            throw;
        }

        return Transfers.GetTransfer(key);
    }

    /// <summary>
    /// Для передачи key отправляет оставшиеся чанки, FILE_DONE и ожидает FILE_RESULT.
    /// ct прерывает сетевую операцию; callerToken позволяет отличить отмену вызова от отключения обмена.
    /// </summary>
    private async Task SendFileContentsAsync(
        TransferKey key,
        CancellationToken ct,
        CancellationToken callerToken
    )
    {
        try
        {
            await SendChunksAsync(key, ct).ConfigureAwait(false);
            await SendReliableAsync(
                    MessageType.FileDone,
                    key.PeerDeviceId,
                    Payloads.EncodeFileDone(DeviceId, key.FileId),
                    ct
                )
                .ConfigureAwait(false);
            await Transfers.WaitForFileResultAsync(key, ct).ConfigureAwait(false);
        }
        catch (TimeoutException)
        {
            Transfers.SetState(key, TransferState.Paused);
        }
        catch (SocketException exception) when (exception.IsUnreachable())
        {
            Transfers.SetState(key, TransferState.Paused);

            var endpoint = _resolveEndpoint(key.PeerDeviceId);

            _notifications.Post(NetworkError, handler => handler(endpoint, exception));
        }
        catch (OperationCanceledException) when (IsExchangeInterruption(callerToken))
        {
            Transfers.SetState(key, TransferState.Paused);
        }
        catch (InvalidOperationException)
            when (Transfers.GetTransfer(key).State
                    is TransferState.Paused
                        or TransferState.Cancelled
                        or TransferState.Failed
                        or TransferState.Completed
            ) { }
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
    }

    /// <summary>
    /// Читает исходный файл передачи key с первого недостающего чанка и продвигает индекс только после ACK.
    /// ct отменяет чтение или сетевое ожидание.
    /// </summary>
    private async Task SendChunksAsync(TransferKey key, CancellationToken ct)
    {
        await using var stream = await Transfers
            .OpenOutgoingFileAsync(key, ct)
            .ConfigureAwait(false);

        var transfer = Transfers.GetTransfer(key);
        uint index = transfer.NextIndex;
        long offset = (long)index * ProtocolConstants.ChunkSize;
        stream.Position = offset;

        byte[] buffer = new byte[ProtocolConstants.ChunkSize];
        while (offset < (long)transfer.FileSize)
        {
            ct.ThrowIfCancellationRequested();

            int length = (int)Math.Min(buffer.Length, (long)transfer.FileSize - offset);

            await stream.ReadExactlyAsync(buffer.AsMemory(0, length), ct).ConfigureAwait(false);

            byte[] payload = Payloads.EncodeChunk(
                DeviceId,
                key.FileId,
                index,
                buffer.AsSpan(0, length)
            );

            await SendReliableAsync(MessageType.Chunk, key.PeerDeviceId, payload, ct)
                .ConfigureAwait(false);

            Transfers.ConfirmOutgoingChunk(key, index);
            index++;
            offset += length;
        }
    }

    private Task SendOfferAsync(FileTransfer transfer, CancellationToken ct)
    {
        byte[] payload = Payloads.EncodeFileOffer(
            DeviceId,
            transfer.FileId,
            transfer.FileSize,
            transfer.Sha256,
            transfer.FileName
        );

        return SendReliableAsync(MessageType.FileOffer, transfer.PeerDeviceId, payload, ct);
    }

    private Task SendAnswerAsync(
        TransferKey key,
        bool accepted,
        uint nextIndex,
        CancellationToken ct
    )
    {
        byte[] payload = Payloads.EncodeFileAnswer(DeviceId, key.FileId, accepted, nextIndex);

        return SendReliableAsync(MessageType.FileAnswer, key.PeerDeviceId, payload, ct);
    }

    /// <summary>
    /// Для повторного предложения передачи key отправляет решение accepted и индекс nextIndex без запроса пользователю.
    /// Потерянный ACK не переводит передачу в паузу, если данные уже начали поступать.
    /// </summary>
    private async Task SendAutomaticAnswerAsync(TransferKey key, bool accepted, uint nextIndex)
    {
        CancellationToken ct;
        lock (_stateLock)
        {
            if (_disposed || _exchangeToken.IsCancellationRequested)
            {
                return;
            }
            ct = _exchangeToken;
        }

        using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, _stopCts.Token);
        ct = linked.Token;

        try
        {
            await SendAnswerAsync(key, accepted, nextIndex, ct).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested) { }
        catch (TimeoutException)
            when (accepted
                && Transfers.GetTransfer(key) is var transfer
                && (
                    transfer.NextIndex > nextIndex
                    || transfer.State is TransferState.Verifying or TransferState.Completed
                )
            ) { }
        catch (TimeoutException)
        {
            if (accepted)
            {
                Transfers.SetState(key, TransferState.Paused);
            }
        }
        catch (SocketException exception) when (exception.IsUnreachable())
        {
            if (accepted)
            {
                Transfers.SetState(key, TransferState.Paused);
            }
            var endpoint = _resolveEndpoint(key.PeerDeviceId);
            _notifications.Post(NetworkError, handler => handler(endpoint, exception));
        }
    }

    private async Task CompleteAndSendFileResultAsync(
        TransferKey key,
        Task<bool> completion,
        CancellationToken ct
    )
    {
        bool ok = await completion.ConfigureAwait(false);
        CancellationToken exchangeToken;

        lock (_stateLock)
        {
            if (_disposed || _exchangeToken.IsCancellationRequested)
            {
                return;
            }
            exchangeToken = _exchangeToken;
        }
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(
            ct,
            exchangeToken,
            _stopCts.Token
        );
        try
        {
            await SendReliableAsync(
                    MessageType.FileResult,
                    key.PeerDeviceId,
                    Payloads.EncodeFileResult(DeviceId, key.FileId, ok),
                    cts.Token
                )
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cts.IsCancellationRequested) { }
        catch (TimeoutException exception)
        {
            var transfer = Transfers.GetTransfer(key);
            _notifications.Post(FileResultDeliveryFailed, handler => handler(transfer, exception));
        }
        catch (SocketException exception) when (exception.IsUnreachable())
        {
            var transfer = Transfers.GetTransfer(key);
            _notifications.Post(FileResultDeliveryFailed, handler => handler(transfer, exception));
        }
    }

    /// <summary>
    /// Последовательно обрабатывает файловые сообщения из общего цикла приёма и повторяет ACK дубликатам.
    /// Ожидание пользовательского решения, SHA-256 и отправка результата выполняются отдельно.
    /// </summary>
    /// <param name="packet">Пакет с проверенным транспортом payload.</param>
    /// <param name="from">Адрес и порт источника датаграммы.</param>
    /// <param name="senderDeviceId">Идентификатор отправителя, уже сверенный PeerService с адресом источника.</param>
    /// <param name="ct">Токен отмены операции.</param>
    public async Task HandlePacketAsync(
        Packet packet,
        IPEndPoint from,
        Guid senderDeviceId,
        CancellationToken ct
    )
    {
        Task<bool>? completion = null;
        TransferKey key = default;
        (bool Accepted, uint NextIndex)? answer = null;

        lock (_stateLock)
        {
            if (_disposed || _exchangeToken.IsCancellationRequested)
            {
                return;
            }
            switch (packet.Type)
            {
                case MessageType.FileOffer:
                    var offer = Payloads.DecodeFileOffer(packet.Payload);
                    if (offer is null)
                    {
                        return;
                    }
                    key = new TransferKey(senderDeviceId, offer.Value.FileId);
                    answer = Transfers.HandleFileOffer(packet.Payload);
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
                    completion = Transfers.HandleFileDone(packet.Payload, _stopCts.Token);
                    if (completion is null)
                    {
                        return;
                    }
                    key = new TransferKey(senderDeviceId, fileDone.Value.FileId);
                    break;
                default:
                    return;
            }
        }
        try
        {
            await _transport.SendAckAsync(packet.Seq, from, DeviceId).ConfigureAwait(false);
        }
        catch (SocketException exception) when (exception.IsUnreachable())
        {
            _notifications.Post(NetworkError, handler => handler(from, exception));
        }
        if (answer.HasValue)
        {
            StartBackgroundReply(
                key,
                MessageType.FileAnswer,
                () => SendAutomaticAnswerAsync(key, answer.Value.Accepted, answer.Value.NextIndex)
            );
        }
        if (completion is not null)
        {
            StartBackgroundReply(
                key,
                MessageType.FileResult,
                () => CompleteAndSendFileResultAsync(key, completion, ct)
            );
        }
    }

    private void StartBackgroundReply(TransferKey key, MessageType type, Func<Task> send)
    {
        lock (_stateLock)
        {
            if (_disposed || !_activeReplies.Add((key, type)))
            {
                return;
            }
            _backgroundTasks.Add(
                Task.Run(async () =>
                {
                    try
                    {
                        await send().ConfigureAwait(false);
                    }
                    finally
                    {
                        lock (_stateLock)
                        {
                            _activeReplies.Remove((key, type));
                        }
                    }
                })
            );
        }
    }

    private async Task SendReliableAsync(
        MessageType type,
        Guid peerDeviceId,
        byte[] payload,
        CancellationToken ct
    )
    {
        var endpoint = _resolveEndpoint(peerDeviceId);
        await _transport
            .SendReliableAsync(type, payload, endpoint, peerDeviceId, ct)
            .ConfigureAwait(false);
        _acknowledged?.Invoke(peerDeviceId, endpoint);
    }

    /// <summary>
    /// Проверяет тайм-ауты, наблюдает ошибки фоновых задач и запускает пробы возобновления передач в паузе.
    /// </summary>
    /// <param name="ct">Токен остановки обслуживания; передаётся новым попыткам возобновления.</param>
    public async Task CheckCompletedOperationsAsync(CancellationToken ct)
    {
        Transfers.CheckTimeouts();
        await ObserveBackgroundTasksAsync(completedOnly: true, ct).ConfigureAwait(false);
        if (!IsExchangeEnabled)
        {
            return;
        }
        foreach (var transfer in Transfers.GetTransfers())
        {
            if (
                transfer.Direction != TransferDirection.Outgoing
                || transfer.State != TransferState.Paused
            )
            {
                continue;
            }
            var key = transfer.Key;
            lock (_stateLock)
            {
                if (
                    _disposed
                    || _exchangeToken.IsCancellationRequested
                    || _activeOutgoing.Contains(key)
                )
                {
                    continue;
                }
                long now = _timeProvider.GetTimestamp();
                if (!_lastProbes.TryGetValue(key, out long lastProbe))
                {
                    _lastProbes[key] = now;
                    continue;
                }
                if (_timeProvider.GetElapsedTime(lastProbe, now) < ProtocolConstants.ProbeInterval)
                {
                    continue;
                }
                _lastProbes[key] = now;
                _activeOutgoing.Add(key);
                _backgroundTasks.Add(Task.Run(() => ResumeOutgoingAsync(key, ct), ct));
            }
        }
    }

    /// <summary>
    /// Возобновляет исходящую передачу key по nextIndex получателя, не продлевая исходный срок паузы.
    /// maintenanceToken останавливает попытку вместе с обслуживанием сессии.
    /// </summary>
    private async Task ResumeOutgoingAsync(TransferKey key, CancellationToken maintenanceToken)
    {
        try
        {
            CancellationToken exchangeToken;
            lock (_stateLock)
            {
                if (_disposed || _exchangeToken.IsCancellationRequested)
                {
                    return;
                }
                exchangeToken = _exchangeToken;
            }
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(
                maintenanceToken,
                exchangeToken,
                _stopCts.Token
            );
            if (!Transfers.PrepareResume(key))
            {
                return;
            }
            var transfer = Transfers.GetTransfer(key);
            try
            {
                await SendOfferAsync(transfer, cts.Token).ConfigureAwait(false);
                bool accepted = await Transfers
                    .WaitForFileAnswerAsync(key, cts.Token, resuming: true)
                    .ConfigureAwait(false);

                if (!accepted)
                {
                    return;
                }
                await SendFileContentsAsync(key, cts.Token, maintenanceToken).ConfigureAwait(false);
            }
            catch (TimeoutException)
            {
                Transfers.SetState(key, TransferState.Paused);
            }
            catch (OperationCanceledException) when (!_stopCts.IsCancellationRequested)
            {
                Transfers.SetState(key, TransferState.Paused);
            }
            catch (SocketException exception) when (exception.IsUnreachable())
            {
                Transfers.SetState(key, TransferState.Paused);
                var endpoint = _resolveEndpoint(key.PeerDeviceId);
                _notifications.Post(NetworkError, handler => handler(endpoint, exception));
            }
            catch (Exception exception)
                when (exception is IOException or UnauthorizedAccessException)
            {
                Transfers.SetState(key, TransferState.Failed, exception);
            }
        }
        catch (OperationCanceledException) when (_stopCts.IsCancellationRequested) { }
        finally
        {
            lock (_stateLock)
            {
                _activeOutgoing.Remove(key);
            }
        }
    }

    public Task WaitForPendingOperationsAsync(CancellationToken ct) =>
        ObserveBackgroundTasksAsync(completedOnly: false, ct);

    private async Task ObserveBackgroundTasksAsync(bool completedOnly, CancellationToken ct)
    {
        Task[] tasks;
        lock (_stateLock)
        {
            tasks = _backgroundTasks.Where(task => !completedOnly || task.IsCompleted).ToArray();
            foreach (var task in tasks)
            {
                _backgroundTasks.Remove(task);
            }
        }
        try
        {
            await Task.WhenAll(tasks).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested) { }
    }

    private CancellationToken GetExchangeToken()
    {
        lock (_stateLock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (_exchangeToken.IsCancellationRequested)
            {
                throw new InvalidOperationException("Network exchange is disabled");
            }
            return _exchangeToken;
        }
    }

    private bool IsExchangeInterruption(CancellationToken callerToken) =>
        !_stopCts.IsCancellationRequested && !callerToken.IsCancellationRequested;

    private Task<FileTransfer> StartOutgoingOperation(
        Func<CancellationToken, Task<FileTransfer>> start
    )
    {
        lock (_stateLock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            var exchangeToken = GetExchangeToken();
            var operation = Task.Run(() => start(exchangeToken));
            _outgoingCompletions.TrackCompletion(operation);
            return operation;
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
