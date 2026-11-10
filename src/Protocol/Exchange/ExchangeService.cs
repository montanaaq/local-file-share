using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Protocol.Extensions;
using Protocol.Messages;
using Protocol.Transfers;
using Protocol.Transfers.Models;
using Protocol.Transport;

namespace Protocol.Exchange;

internal sealed class ExchangeService : IAsyncDisposable
{
    private readonly UdpTransport _transport;
    private readonly Guid _deviceId;
    private readonly Func<Guid, IPEndPoint> _resolveEndpoint;
    private readonly Action<Guid, IPEndPoint> _acknowledged;
    private readonly NotificationDispatcher _notifications;
    private readonly FileExchange _files;

    private readonly Lock _stateLock = new();
    private readonly ProcessedMessages _processedTexts = new();
    private readonly HashSet<Task> _textCompletions = [];

    private readonly List<CancellationTokenSource> _retiredExchangeSources = [];
    private readonly CancellationTokenSource _stopCts;
    private CancellationTokenSource _exchangeCts;

    private Task? _disposeTask;
    private bool _disposed;

    public ExchangeService(
        UdpTransport transport,
        Guid deviceId,
        Func<Guid, IPEndPoint> resolveEndpoint,
        CancellationToken stopping,
        NotificationDispatcher notifications,
        TimeProvider timeProvider,
        Action<Guid, IPEndPoint> acknowledged
    )
    {
        _transport = transport;
        _deviceId = deviceId;
        _resolveEndpoint = resolveEndpoint;
        _acknowledged = acknowledged;
        _notifications = notifications;

        _stopCts = CancellationTokenSource.CreateLinkedTokenSource(stopping);
        _exchangeCts = CancellationTokenSource.CreateLinkedTokenSource(_stopCts.Token);

        _files = new FileExchange(
            transport,
            deviceId,
            resolveEndpoint,
            _stopCts.Token,
            notifications,
            timeProvider,
            acknowledged,
            _exchangeCts.Token
        );
    }

    public ITransferSource Transfers => _files;
    public bool IsExchangeEnabled => _transport.IsExchangeEnabled;

    public TransportStatistics GetStatistics() => _transport.GetStatistics();

    public event Action<Guid, string>? TextReceived;
    private event Action<IPEndPoint, SocketException>? NetworkErrorHandlers;

    public event Action<IPEndPoint, SocketException>? NetworkError
    {
        add
        {
            NetworkErrorHandlers += value;
            _files.NetworkError += value;
        }
        remove
        {
            NetworkErrorHandlers -= value;
            _files.NetworkError -= value;
        }
    }

    public event Action<FileTransfer, Exception>? FileResultDeliveryFailed
    {
        add => _files.FileResultDeliveryFailed += value;
        remove => _files.FileResultDeliveryFailed -= value;
    }

    /// <summary>
    /// Управляет сетевым обменом и его токеном отмены для текста, обнаружения и файлов.
    /// Токены прежних периодов освобождаются после завершения использующих их операций при закрытии.
    /// </summary>
    /// <param name="enabled">true начинает период обмена, false отменяет его операции и приостанавливает передачи.</param>
    public void SetExchangeEnabled(bool enabled)
    {
        lock (_stateLock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            if (IsExchangeEnabled == enabled)
            {
                return;
            }

            if (enabled)
            {
                _retiredExchangeSources.Add(_exchangeCts);
                _exchangeCts = CancellationTokenSource.CreateLinkedTokenSource(_stopCts.Token);
                _files.SetExchangeToken(_exchangeCts.Token);
                _transport.IsExchangeEnabled = true;
            }
            else
            {
                _transport.IsExchangeEnabled = false;
                _exchangeCts.Cancel();
                _files.SetExchangeToken(_exchangeCts.Token);
            }
        }
    }

    public bool TryGetExchangeToken(out CancellationToken token)
    {
        lock (_stateLock)
        {
            if (_disposed || _stopCts.IsCancellationRequested || !IsExchangeEnabled)
            {
                token = default;
                return false;
            }

            token = _exchangeCts.Token;
            return true;
        }
    }

    public Task<FileTransfer> OfferFileAsync(
        Guid peerDeviceId,
        string filePath,
        CancellationToken ct
    ) => _files.OfferFileAsync(peerDeviceId, filePath, ct);

    public Task<FileTransfer> AnswerFileOfferAsync(
        TransferKey key,
        bool accepted,
        string directory,
        CancellationToken ct
    ) => _files.AnswerFileOfferAsync(key, accepted, directory, ct);

    public FileTransfer CancelPausedTransfer(TransferKey key) => _files.CancelPausedTransfer(key);

    public Task SendTextAsync(Guid peerDeviceId, string text, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();

        byte[] payload = Payloads.EncodeClipboard(_deviceId, text);

        lock (_stateLock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            if (!IsExchangeEnabled)
            {
                throw new InvalidOperationException("Network exchange is disabled");
            }

            CancellationToken exchangeToken = _exchangeCts.Token;

            var operation = Task.Run(
                async () =>
                {
                    using var linked = CancellationTokenSource.CreateLinkedTokenSource(
                        ct,
                        exchangeToken
                    );

                    var endpoint = _resolveEndpoint(peerDeviceId);
                    await _transport
                        .SendReliableAsync(
                            MessageType.Clipboard,
                            payload,
                            endpoint,
                            peerDeviceId,
                            linked.Token
                        )
                        .ConfigureAwait(false);
                    _acknowledged(peerDeviceId, endpoint);
                },
                ct
            );
            _textCompletions.TrackCompletion(operation);
            return operation;
        }
    }

    /// <summary>
    /// Обрабатывает сообщение от проверенного PeerService отправителя: текст публикуется один раз, файловые сообщения передаются FileExchange.
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
        if (packet.Type != MessageType.Clipboard)
        {
            await _files.HandlePacketAsync(packet, from, senderDeviceId, ct).ConfigureAwait(false);
            return;
        }

        lock (_stateLock)
        {
            if (_disposed || !IsExchangeEnabled)
            {
                return;
            }

            var text = Payloads.DecodeClipboard(packet.Payload);

            if (text is null)
            {
                return;
            }

            if (!_processedTexts.Contains(senderDeviceId, packet.Seq))
            {
                _processedTexts.Add(senderDeviceId, packet.Seq);

                string value = text.Value.Text;
                _notifications.Post(TextReceived, handler => handler(senderDeviceId, value));
            }
        }

        try
        {
            await _transport.SendAckAsync(packet.Seq, from, _deviceId).ConfigureAwait(false);
        }
        catch (SocketException exception) when (exception.IsUnreachable())
        {
            _notifications.Post(NetworkErrorHandlers, handler => handler(from, exception));
        }
    }

    /// <summary>
    /// Обслуживает сроки паузы и фоновые операции, в том числе при отключённом сетевом обмене.
    /// </summary>
    /// <param name="ct">Токен остановки фонового обслуживания всей сессии.</param>
    public async Task MaintainAsync(CancellationToken ct)
    {
        using var timer = new PeriodicTimer(ProtocolConstants.MaintenanceInterval);

        do
        {
            ct.ThrowIfCancellationRequested();

            await _files.CheckCompletedOperationsAsync(ct).ConfigureAwait(false);
        } while (await timer.WaitForNextTickAsync(ct).ConfigureAwait(false));
    }

    public Task WaitForPendingOperationsAsync(CancellationToken ct) =>
        _files.WaitForPendingOperationsAsync(ct);

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

        Task[] texts;

        lock (_stateLock)
        {
            texts = _textCompletions.ToArray();
        }

        try
        {
            await Task.WhenAll(texts).ConfigureAwait(false);
            await _files.DisposeAsync().ConfigureAwait(false);
        }
        finally
        {
            _exchangeCts.Dispose();

            foreach (var source in _retiredExchangeSources)
            {
                source.Dispose();
            }

            _stopCts.Dispose();
        }
    }
}
