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
    private readonly FileExchange _fileExchange;
    private readonly NotificationDispatcher _notifications;
    private readonly TimeProvider _timeProvider;
    private Task? _disposeTask;

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
        _fileExchange = new FileExchange(
            _transport,
            DeviceId,
            ResolveEndpoint,
            _stopCts.Token,
            _notifications
        );
    }

    public Guid DeviceId { get; }
    public int LocalPort => _transport.LocalPort;
    public int LossRate
    {
        get => _transport.LossRate;
        set => _transport.LossRate = value;
    }

    public ITransferSource Transfers => _fileExchange;

    /// <summary>Reports subscriber errors on the background notification worker.</summary>
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
            _fileExchange.NetworkError += value;
        }
        remove
        {
            NetworkErrorHandlers -= value;
            _fileExchange.NetworkError -= value;
        }
    }
    public event Action<FileTransfer, Exception>? FileResultDeliveryFailed
    {
        add => _fileExchange.FileResultDeliveryFailed += value;
        remove => _fileExchange.FileResultDeliveryFailed -= value;
    }

    public Peer[] GetPeers()
    {
        lock (_stateLock)
        {
            return _peers.Values.Select(entry => entry.Peer).ToArray();
        }
    }

    public void AddManualEndpoint(IPEndPoint endpoint)
    {
        if (endpoint is null)
        {
            throw new ArgumentNullException(nameof(endpoint), "Value cannot be null");
        }

        if (endpoint.AddressFamily != AddressFamily.InterNetwork || endpoint.Port == 0)
        {
            throw new ArgumentException(
                "An IPv4 address and a nonzero port are required",
                nameof(endpoint)
            );
        }

        lock (_stateLock)
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(nameof(PeerService), "Object has been disposed");
            }

            _manualEndpoints.Add(new IPEndPoint(endpoint.Address, endpoint.Port));
        }
    }

    public Task RunAsync(CancellationToken ct)
    {
        lock (_stateLock)
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(nameof(PeerService), "Object has been disposed");
            }

            if (_runTask is not null)
            {
                throw new InvalidOperationException("Peer service has already been started");
            }

            _runTask = RunCoreAsync(ct);
            return _runTask;
        }
    }

    public Task<FileTransfer> OfferFileAsync(
        Guid peerDeviceId,
        string filePath,
        CancellationToken ct
    ) => _fileExchange.OfferFileAsync(peerDeviceId, filePath, ct);

    public Task<FileTransfer> AnswerFileOfferAsync(
        Guid peerDeviceId,
        Guid fileId,
        bool accepted,
        string directory,
        CancellationToken ct
    ) => _fileExchange.AnswerFileOfferAsync(peerDeviceId, fileId, accepted, directory, ct);

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
        try
        {
            var completed = await Task.WhenAny(receiving, monitoring).ConfigureAwait(false);
            await completed.ConfigureAwait(false);
        }
        finally
        {
            cts.Cancel();
            try
            {
                await Task.WhenAll(receiving, monitoring).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cts.IsCancellationRequested) { }
            finally
            {
                await _fileExchange.WaitForPendingOperationsAsync(cts.Token).ConfigureAwait(false);
            }
        }
    }

    private async Task ReceiveLoopAsync(CancellationToken ct)
    {
        while (true)
        {
            var (packet, from) = await _transport.ReceiveAsync(ct).ConfigureAwait(false);
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
                    await SendPeerInfoAsync(MessageType.PeerInfo, from).ConfigureAwait(false);
                }

                continue;
            }

            if (
                packet.Type
                is not (
                    MessageType.Heartbeat
                    or MessageType.FileOffer
                    or MessageType.FileAnswer
                    or MessageType.Chunk
                    or MessageType.FileDone
                    or MessageType.FileResult
                )
            )
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
                await _fileExchange
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
            await _fileExchange.CheckCompletedOperationsAsync(ct).ConfigureAwait(false);
            long now = _timeProvider.GetTimestamp();
            if (
                lastDiscover is null
                || _timeProvider.GetElapsedTime(lastDiscover.Value, now)
                    >= ProtocolConstants.DiscoverInterval
            )
            {
                await DiscoverAsync().ConfigureAwait(false);
                lastDiscover = now;
            }

            UpdateAvailability(now);
            var peers = GetPeers().Where(peer => peer.Availability != PeerAvailability.Offline);

            await Task.WhenAll(peers.Select(peer => SendHeartbeatAsync(peer, ct)))
                .ConfigureAwait(false);
        } while (await timer.WaitForNextTickAsync(ct).ConfigureAwait(false));
    }

    private async Task DiscoverAsync()
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
            await SendPeerInfoAsync(MessageType.Discover, endpoint).ConfigureAwait(false);
        }
    }

    private async Task SendPeerInfoAsync(MessageType type, IPEndPoint endpoint)
    {
        var packet = new Packet(type, _transport.NextSeq(), _peerInfoPayload);
        try
        {
            await _transport.SendAsync(packet, endpoint).ConfigureAwait(false);
        }
        catch (SocketException exception) when (exception.IsUnreachable())
        {
            _notifications.Post(NetworkErrorHandlers, handler => handler(endpoint, exception));
        }
    }

    private async Task SendHeartbeatAsync(Peer peer, CancellationToken ct)
    {
        var packet = new Packet(
            MessageType.Heartbeat,
            _transport.NextSeq(),
            Payloads.EncodeHeartbeat(DeviceId)
        );
        try
        {
            await _transport
                .SendReliableAsync(packet, peer.Endpoint, peer.DeviceId, ct)
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

    private void UpdateAvailability(long now)
    {
        List<Peer> changed = [];
        lock (_stateLock)
        {
            foreach (var (deviceId, entry) in _peers.ToArray())
            {
                var peer = entry.Peer;
                var elapsed = _timeProvider.GetElapsedTime(entry.LastSeen, now);
                PeerAvailability availability;

                if (elapsed >= ProtocolConstants.PeerTimeout)
                {
                    availability = PeerAvailability.Offline;
                }
                else if (elapsed >= ProtocolConstants.ConnectionTimeout)
                {
                    availability = PeerAvailability.Unreachable;
                }
                else
                {
                    availability = PeerAvailability.Available;
                }

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
                await _fileExchange.DisposeAsync().ConfigureAwait(false);
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
