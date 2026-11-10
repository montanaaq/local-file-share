using System;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Protocol.Exchange;
using Protocol.Extensions;
using Protocol.Messages;
using Protocol.Transfers.Models;
using Protocol.Transport;

namespace Protocol.Transfers;

internal sealed class FileExchange : IAsyncDisposable, ITransferSource
{
    private readonly IMessageTransport _transport;
    private readonly Func<Guid, IPEndPoint> _resolveEndpoint;
    private readonly Action<Guid, IPEndPoint>? _acknowledged;

    private readonly Lock _stateLock = new();
    private readonly ProcessedMessages _processedMessages = new();

    private readonly FileBackgroundTaskHandler _backgroundTasks = new();
    private readonly OutgoingFileExchange _outgoing;

    private readonly NotificationDispatcher _notifications;
    private readonly bool _ownsNotifications;

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

        var clock = timeProvider ?? TimeProvider.System;

        _stopCts = CancellationTokenSource.CreateLinkedTokenSource(stopping);
        _exchangeToken = exchangeToken;

        _ownsNotifications = notifications is null;
        _notifications = notifications ?? new NotificationDispatcher();

        Transfers = new FileTransferService(clock);
        _outgoing = new OutgoingFileExchange(
            Transfers,
            deviceId,
            SendReliableAsync,
            ReportNetworkError,
            _backgroundTasks,
            clock,
            _stopCts.Token
        );

        Transfers.FileOffered += transfer =>
            _notifications.Post(FileOffered, handler => handler(transfer));
        Transfers.TransferChanged += transfer =>
            _notifications.Post(TransferChanged, handler => handler(transfer));
        Transfers.TransferFailed += (transfer, error) =>
            _notifications.Post(TransferFailed, handler => handler(transfer, error));
    }

    private Guid DeviceId { get; }

    public FileTransferService Transfers { get; }

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
        {
            _resolveEndpoint(peerDeviceId);
            return _outgoing.OfferFileAsync(peerDeviceId, filePath, ct, exchangeToken);
        });

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
        catch (OperationCanceledException cancelException)
        {
            if (IsExchangeInterruption(callerToken))
            {
                Transfers.SetState(key, TransferState.Paused);
                return Transfers.GetTransfer(key);
            }

            Transfers.SetState(key, TransferState.Cancelled, cancelException);
            throw;
        }
        catch (Exception exception)
        {
            if (exception is TimeoutException && accepted)
            {
                var currentTransfer = Transfers.GetTransfer(key);

                if (
                    currentTransfer.NextIndex > 0
                    || currentTransfer.State is TransferState.Verifying or TransferState.Completed
                )
                {
                    return currentTransfer;
                }
            }

            Transfers.SetState(key, TransferState.Failed, exception);
            throw;
        }

        return Transfers.GetTransfer(key);
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
        {
            if (!accepted)
            {
                return;
            }

            var transfer = Transfers.GetTransfer(key);

            if (
                transfer.NextIndex > nextIndex
                || transfer.State is TransferState.Verifying or TransferState.Completed
            )
            {
                return;
            }

            Transfers.SetState(key, TransferState.Paused);
        }
        catch (SocketException exception) when (exception.IsUnreachable())
        {
            if (accepted)
            {
                Transfers.SetState(key, TransferState.Paused);
            }

            ReportNetworkError(key.PeerDeviceId, exception);
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
        catch (Exception exception)
            when (exception is TimeoutException
                || exception is SocketException socketException && socketException.IsUnreachable()
            )
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
                    answer = Transfers.HandleFileOffer(offer.Value);
                    break;

                case MessageType.FileAnswer:
                    if (!_processedMessages.Contains(senderDeviceId, packet.Seq))
                    {
                        var fileAnswer = Payloads.DecodeFileAnswer(packet.Payload);
                        if (
                            fileAnswer is null
                            || !Transfers.HandleFileAnswer(
                                new TransferKey(senderDeviceId, fileAnswer.Value.FileId),
                                fileAnswer.Value.Accepted,
                                fileAnswer.Value.NextIndex
                            )
                        )
                        {
                            return;
                        }

                        _processedMessages.Add(senderDeviceId, packet.Seq);
                    }
                    break;

                case MessageType.FileResult:
                    if (!_processedMessages.Contains(senderDeviceId, packet.Seq))
                    {
                        var fileResult = Payloads.DecodeFileResult(packet.Payload);
                        if (
                            fileResult is null
                            || !Transfers.HandleFileResult(
                                new TransferKey(senderDeviceId, fileResult.Value.FileId),
                                fileResult.Value.Ok
                            )
                        )
                        {
                            return;
                        }

                        _processedMessages.Add(senderDeviceId, packet.Seq);
                    }
                    break;

                case MessageType.Chunk:
                    var chunk = Payloads.DecodeChunk(packet.Payload);
                    if (
                        chunk is null
                        || !Transfers.HandleChunk(
                            new TransferKey(senderDeviceId, chunk.Value.FileId),
                            chunk.Value.Index,
                            chunk.Value.Data
                        )
                    )
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

                    key = new TransferKey(senderDeviceId, fileDone.Value.FileId);
                    completion = Transfers.HandleFileDone(key, _stopCts.Token);

                    if (completion is null)
                    {
                        return;
                    }

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
            _backgroundTasks.StartReply(
                key,
                MessageType.FileAnswer,
                () => SendAutomaticAnswerAsync(key, answer.Value.Accepted, answer.Value.NextIndex)
            );
        }

        if (completion is not null)
        {
            _backgroundTasks.StartReply(
                key,
                MessageType.FileResult,
                () => CompleteAndSendFileResultAsync(key, completion, ct)
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
    /// Проверяет тайм-ауты, наблюдает ошибки фоновых задач и запускает пробы возобновления.
    /// </summary>
    /// <param name="ct">Токен остановки обслуживания сессии.</param>
    public async Task CheckCompletedOperationsAsync(CancellationToken ct)
    {
        Transfers.CheckTimeouts();
        await _backgroundTasks.ObserveCompletedAsync(ct).ConfigureAwait(false);

        lock (_stateLock)
        {
            if (_disposed || _exchangeToken.IsCancellationRequested)
            {
                return;
            }

            _outgoing.ResumePausedTransfers(_exchangeToken, ct);
        }
    }

    private void ReportNetworkError(Guid peerDeviceId, SocketException exception)
    {
        var endpoint = _resolveEndpoint(peerDeviceId);
        _notifications.Post(NetworkError, handler => handler(endpoint, exception));
    }

    public Task WaitForPendingOperationsAsync(CancellationToken ct) =>
        _backgroundTasks.WaitForPendingAsync(ct);

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
            return _backgroundTasks.StartOperation(() => start(exchangeToken));
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

        try
        {
            await _backgroundTasks.StopAsync(_stopCts.Token).ConfigureAwait(false);
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
