using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace Protocol;

public sealed class PeerService : IAsyncDisposable
{
    private readonly UdpTransport _transport;
    private readonly byte[] _peerInfoPayload;
    private readonly Lock _stateLock = new();
    private readonly Dictionary<Guid, (Peer Peer, long LastSeen)> _peers = [];
    private readonly HashSet<IPEndPoint> _manualEndpoints = [];
    private readonly CancellationTokenSource _stopCts = new();
    private Task? _runTask;
    private bool _disposed;
    private readonly ExchangeService _exchange;
    private readonly NotificationDispatcher _notifications;
    private readonly TimeProvider _timeProvider;
    private Task? _disposeTask;

    /// <summary>
    /// Создаёт сессию устройства с новым deviceId и открывает локальный UDP-порт.
    /// </summary>
    /// <param name="name">Непустое имя устройства, не более 255 байт UTF-8.</param>
    /// <param name="localPort">Порт прослушивания; по умолчанию 5000, значение 0 выбирает свободный порт.</param>
    /// <param name="timeProvider">Источник времени для таймеров; null использует системное время.</param>
    public PeerService(
        string name,
        int localPort = ProtocolConstants.DefaultPort,
        TimeProvider? timeProvider = null
    )
    {
        DeviceId = Guid.NewGuid();
        _peerInfoPayload = Payloads.EncodePeerInfo(DeviceId, name);
        _transport = new UdpTransport(localPort);
        _timeProvider = timeProvider ?? TimeProvider.System;
        _notifications = new NotificationDispatcher();
        _exchange = new ExchangeService(
            _transport,
            DeviceId,
            ResolveEndpoint,
            _stopCts.Token,
            _notifications,
            _timeProvider,
            (peerId, endpoint) => RefreshPeer(peerId, endpoint)
        );
    }

    public Guid DeviceId { get; }
    public int LocalPort => _transport.LocalPort;
    public int LossRate
    {
        get => _transport.LossRate;
        set => _transport.LossRate = value;
    }

    public ITransferSource Transfers => _exchange.Transfers;
    public bool IsExchangeEnabled => _exchange.IsExchangeEnabled;

    public TransportStatistics GetStatistics() => _exchange.GetStatistics();

    public event Action<Guid, string>? TextReceived
    {
        add => _exchange.TextReceived += value;
        remove => _exchange.TextReceived -= value;
    }

    /// <summary>
    /// Переключает сетевой обмен без смены deviceId и порта. При отключении передачи переходят в паузу; её срок продолжает отсчитываться.
    /// </summary>
    /// <param name="enabled">true включает обмен, false останавливает сетевые операции.</param>
    public void SetExchangeEnabled(bool enabled)
    {
        ThrowIfDisposed();
        _exchange.SetExchangeEnabled(enabled);
    }

    /// <summary>
    /// Локально отменяет передачу в паузе и удаляет принимаемый .part. Сетевое сообщение отмены не отправляется.
    /// </summary>
    /// <param name="key">Идентификаторы удалённого устройства и файла.</param>
    /// <returns>Снимок отменённой передачи.</returns>
    public FileTransfer CancelPausedTransfer(TransferKey key) =>
        _exchange.CancelPausedTransfer(key);

    /// <summary>
    /// Отправляет текст известному устройству и ожидает ACK.
    /// </summary>
    /// <param name="peerDeviceId">Идентификатор удалённого устройства из списка известных устройств.</param>
    /// <param name="text">Текст размером до 1000 байт строгого UTF-8.</param>
    /// <param name="ct">Токен отмены операции.</param>
    public Task SendTextAsync(Guid peerDeviceId, string text, CancellationToken ct)
    {
        ThrowIfDisposed();
        return _exchange.SendTextAsync(peerDeviceId, text, ct);
    }

    /// <summary>Сообщает об ошибках подписчиков из фонового обработчика уведомлений.</summary>
    public event Action<Exception>? NotificationFailed
    {
        add => _notifications.NotificationFailed += value;
        remove => _notifications.NotificationFailed -= value;
    }

    public event Action<Peer>? PeerChanged;
    private event Action<IPEndPoint, SocketException>? NetworkErrorHandlers;
    public event Action<IPEndPoint, SocketException>? NetworkError
    {
        add
        {
            NetworkErrorHandlers += value;
            _exchange.NetworkError += value;
        }
        remove
        {
            NetworkErrorHandlers -= value;
            _exchange.NetworkError -= value;
        }
    }
    public event Action<FileTransfer, Exception>? FileResultDeliveryFailed
    {
        add => _exchange.FileResultDeliveryFailed += value;
        remove => _exchange.FileResultDeliveryFailed -= value;
    }

    public Peer[] GetPeers()
    {
        lock (_stateLock)
        {
            return _peers.Values.Select(entry => entry.Peer).ToArray();
        }
    }

    /// <summary>
    /// Добавляет адрес для периодического DISCOVER; устройство появится в списке после ответа.
    /// </summary>
    /// <param name="endpoint">IPv4-адрес и ненулевой UDP-порт удалённого устройства.</param>
    public void AddManualEndpoint(IPEndPoint endpoint)
    {
        ArgumentNullException.ThrowIfNull(endpoint);

        if (endpoint.AddressFamily != AddressFamily.InterNetwork || endpoint.Port == 0)
        {
            throw new ArgumentException(
                "An IPv4 address and a nonzero port are required",
                nameof(endpoint)
            );
        }

        lock (_stateLock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            _manualEndpoints.Add(new IPEndPoint(endpoint.Address, endpoint.Port));
        }
    }

    /// <summary>
    /// Однократно запускает обнаружение, маршрутизацию и обслуживание передач до отмены.
    /// События вызываются в фоновом потоке; подписчики UI должны переключаться на поток интерфейса.
    /// </summary>
    /// <param name="ct">Токен остановки всей сессии, включая её незавершённые сетевые операции.</param>
    public Task RunAsync(CancellationToken ct)
    {
        lock (_stateLock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            if (_runTask is not null)
            {
                throw new InvalidOperationException("Peer service has already been started");
            }

            _runTask = RunCoreAsync(ct);
            return _runTask;
        }
    }

    /// <summary>
    /// Предлагает файл и выполняет отправку после согласия получателя. ACK предложения не означает согласие.
    /// </summary>
    /// <param name="peerDeviceId">Идентификатор удалённого устройства из списка известных устройств.</param>
    /// <param name="filePath">Путь к существующему файлу; размер и SHA-256 определяются при подготовке.</param>
    /// <param name="ct">Токен отмены операции.</param>
    /// <returns>Снимок после завершения операции, в том числе при отказе или переходе в паузу.</returns>
    public Task<FileTransfer> OfferFileAsync(
        Guid peerDeviceId,
        string filePath,
        CancellationToken ct
    ) => _exchange.OfferFileAsync(peerDeviceId, filePath, ct);

    /// <summary>
    /// Принимает или отклоняет входящее предложение и отправляет решение. Приём файла продолжается в фоне.
    /// </summary>
    /// <param name="key">Идентификаторы отправителя предложения и файла.</param>
    /// <param name="accepted">true принимает предложение, false отклоняет его.</param>
    /// <param name="directory">Папка для принимаемого файла; используется при согласии. Коллизии имён не перезаписывают файлы.</param>
    /// <param name="ct">Токен отмены операции.</param>
    /// <returns>Снимок после отправки решения; успешное сохранение отслеживается через Transfers.</returns>
    public Task<FileTransfer> AnswerFileOfferAsync(
        TransferKey key,
        bool accepted,
        string directory,
        CancellationToken ct
    ) => _exchange.AnswerFileOfferAsync(key, accepted, directory, ct);

    private IPEndPoint ResolveEndpoint(Guid peerDeviceId)
    {
        lock (_stateLock)
        {
            return _peers.TryGetValue(peerDeviceId, out var peer)
                ? peer.Peer.Endpoint
                : throw new ArgumentException("Unknown device", nameof(peerDeviceId));
        }
    }

    private async Task RunCoreAsync(CancellationToken ct)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct, _stopCts.Token);
        var receiving = ReceiveLoopAsync(cts.Token);
        var monitoring = MonitorAsync(cts.Token);
        var maintaining = _exchange.MaintainAsync(cts.Token);
        try
        {
            var completed = await Task.WhenAny(receiving, monitoring, maintaining)
                .ConfigureAwait(false);
            await completed.ConfigureAwait(false);
        }
        finally
        {
            cts.Cancel();
            _stopCts.Cancel();
            try
            {
                await Task.WhenAll(receiving, monitoring, maintaining).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cts.IsCancellationRequested) { }
            finally
            {
                await _exchange.WaitForPendingOperationsAsync(cts.Token).ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// До отмены ct читает единственную очередь транспорта, сверяет отправителя и маршрутизирует сообщения.
    /// Обнаружение обновляет адреса; прикладные сообщения принимаются только от известных устройств с ожидаемым endpoint.
    /// </summary>
    private async Task ReceiveLoopAsync(CancellationToken ct)
    {
        while (true)
        {
            var (packet, from) = await _transport.ReceiveAsync(ct).ConfigureAwait(false);
            if (!IsExchangeEnabled)
            {
                continue;
            }
            if (packet.Type is MessageType.Discover or MessageType.PeerInfo)
            {
                var info = Payloads.DecodePeerInfo(packet.Payload);
                if (info is null || info.Value.DeviceId == DeviceId)
                {
                    continue;
                }

                UpdatePeer(info.Value.DeviceId, info.Value.Name, from);
                if (packet.Type is MessageType.Discover)
                {
                    if (_exchange.TryGetExchangeToken(out var exchangeToken))
                    {
                        try
                        {
                            await SendPeerInfoAsync(MessageType.PeerInfo, from, exchangeToken)
                                .ConfigureAwait(false);
                        }
                        catch (OperationCanceledException)
                            when (exchangeToken.IsCancellationRequested) { }
                    }
                }

                continue;
            }

            if (packet.Type is MessageType.Ack or MessageType.Discover or MessageType.PeerInfo)
            {
                continue;
            }

            var senderDeviceId = new Guid(
                packet.Payload.AsSpan(0, PayloadsConstants.DeviceIdSize),
                bigEndian: true
            );
            if (senderDeviceId == DeviceId || !RefreshPeer(senderDeviceId, from))
            {
                continue;
            }

            if (packet.Type != MessageType.Heartbeat)
            {
                await _exchange
                    .HandlePacketAsync(packet, from, senderDeviceId, ct)
                    .ConfigureAwait(false);
                continue;
            }

            try
            {
                await _transport.SendAckAsync(packet.Seq, from, DeviceId).ConfigureAwait(false);
            }
            catch (SocketException exception) when (exception.IsUnreachable())
            {
                _notifications.Post(NetworkErrorHandlers, handler => handler(from, exception));
            }
        }
    }

    private void UpdatePeer(Guid deviceId, string name, IPEndPoint endpoint)
    {
        var peer = new Peer(
            deviceId,
            name,
            endpoint.Address,
            endpoint.Port,
            _timeProvider.GetUtcNow(),
            PeerAvailability.Available
        );
        lock (_stateLock)
        {
            _peers[deviceId] = (peer, _timeProvider.GetTimestamp());
        }

        _notifications.Post(PeerChanged, handler => handler(peer));
    }

    private bool RefreshPeer(Guid deviceId, IPEndPoint endpoint)
    {
        Peer updated;
        lock (_stateLock)
        {
            if (
                !_peers.TryGetValue(deviceId, out var entry)
                || !entry.Peer.Endpoint.Equals(endpoint)
            )
            {
                return false;
            }

            updated = entry.Peer with
            {
                LastSeen = _timeProvider.GetUtcNow(),
                Availability = PeerAvailability.Available,
            };
            _peers[deviceId] = (updated, _timeProvider.GetTimestamp());
        }
        _notifications.Post(PeerChanged, handler => handler(updated));
        return true;
    }

    private async Task MonitorAsync(CancellationToken ct)
    {
        using var timer = new PeriodicTimer(ProtocolConstants.Heartbeat);
        long? lastDiscover = null;
        do
        {
            ct.ThrowIfCancellationRequested();

            long now = _timeProvider.GetTimestamp();
            UpdateAvailability(now);

            if (!_exchange.TryGetExchangeToken(out var exchangeToken))
            {
                lastDiscover = null;
                continue;
            }

            using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, exchangeToken);
            try
            {
                if (
                    lastDiscover is null
                    || _timeProvider.GetElapsedTime(lastDiscover.Value, now)
                        >= ProtocolConstants.DiscoverInterval
                )
                {
                    await DiscoverAsync(linked.Token).ConfigureAwait(false);
                    lastDiscover = now;
                }

                var pausedPeers = Transfers
                    .GetTransfers()
                    .Where(transfer => transfer.State == TransferState.Paused)
                    .Select(transfer => transfer.PeerDeviceId)
                    .ToHashSet();
                var peers = GetPeers()
                    .Where(peer =>
                        peer.Availability != PeerAvailability.Offline
                        || pausedPeers.Contains(peer.DeviceId)
                    );
                await Task.WhenAll(peers.Select(peer => SendHeartbeatAsync(peer, linked.Token)))
                    .ConfigureAwait(false);
            }
            catch (OperationCanceledException)
                when (!ct.IsCancellationRequested && exchangeToken.IsCancellationRequested) { }
        } while (await timer.WaitForNextTickAsync(ct).ConfigureAwait(false));
    }

    private async Task DiscoverAsync(CancellationToken ct)
    {
        HashSet<IPEndPoint> endpoints;
        lock (_stateLock)
        {
            endpoints = [.. _manualEndpoints];
        }

        endpoints.Add(new IPEndPoint(IPAddress.Broadcast, ProtocolConstants.DefaultPort));
        endpoints.Add(new IPEndPoint(IPAddress.Loopback, ProtocolConstants.DefaultPort));

        foreach (var endpoint in endpoints)
        {
            await SendPeerInfoAsync(MessageType.Discover, endpoint, ct).ConfigureAwait(false);
        }
    }

    private async Task SendPeerInfoAsync(
        MessageType type,
        IPEndPoint endpoint,
        CancellationToken ct
    )
    {
        var packet = new Packet(type, _transport.NextSeq(), _peerInfoPayload);
        try
        {
            await _transport.SendAsync(packet, endpoint, ct).ConfigureAwait(false);
        }
        catch (SocketException exception) when (exception.IsUnreachable())
        {
            _notifications.Post(NetworkErrorHandlers, handler => handler(endpoint, exception));
        }
    }

    private async Task SendHeartbeatAsync(Peer peer, CancellationToken ct)
    {
        try
        {
            await _transport
                .SendReliableAsync(
                    MessageType.Heartbeat,
                    Payloads.EncodeHeartbeat(DeviceId),
                    peer.Endpoint,
                    peer.DeviceId,
                    ct
                )
                .ConfigureAwait(false);
            RefreshPeer(peer.DeviceId, peer.Endpoint);
        }
        catch (TimeoutException)
        {
            UpdateAvailability(_timeProvider.GetTimestamp());
        }
        catch (SocketException exception) when (exception.IsUnreachable())
        {
            _notifications.Post(NetworkErrorHandlers, handler => handler(peer.Endpoint, exception));
        }
    }

    /// <summary>
    /// Обновляет доступность известных устройств по времени последнего сообщения и публикует изменения.
    /// </summary>
    /// <param name="now">Текущая монотонная отметка того же TimeProvider, который хранит LastSeen устройства.</param>
    private void UpdateAvailability(long now)
    {
        List<Peer> changed = [];
        lock (_stateLock)
        {
            foreach (var (deviceId, entry) in _peers.ToArray())
            {
                var peer = entry.Peer;
                var elapsed = _timeProvider.GetElapsedTime(entry.LastSeen, now);

                var availability = elapsed switch
                {
                    _ when elapsed >= ProtocolConstants.PeerTimeout => PeerAvailability.Offline,
                    _ when elapsed >= ProtocolConstants.ConnectionTimeout =>
                        PeerAvailability.Unreachable,
                    _ => PeerAvailability.Available,
                };

                if (availability == peer.Availability)
                {
                    continue;
                }

                var updated = peer with { Availability = availability };
                _peers[deviceId] = (updated, entry.LastSeen);
                changed.Add(updated);
            }
        }
        foreach (var peer in changed)
        {
            _notifications.Post(PeerChanged, handler => handler(peer));
        }
    }

    private void ThrowIfDisposed()
    {
        lock (_stateLock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
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
            if (_runTask is not null)
            {
                await _runTask.ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (_stopCts.IsCancellationRequested) { }
        finally
        {
            try
            {
                await _exchange.DisposeAsync().ConfigureAwait(false);
            }
            finally
            {
                _transport.Dispose();
                _stopCts.Dispose();
                await _notifications.DisposeAsync().ConfigureAwait(false);
            }
        }
    }
}
