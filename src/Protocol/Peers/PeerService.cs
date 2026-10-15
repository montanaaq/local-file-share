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
    private readonly Dictionary<Guid, Peer> _peers = [];
    private readonly HashSet<IPEndPoint> _manualEndpoints = [];
    private readonly CancellationTokenSource _stopCts = new();
    private Task? _runTask;
    private bool _disposed;
    private readonly ProcessedMessages _processedMessages = new();

    public PeerService(string name, int localPort = ProtocolConstants.DefaultPort)
    {
        DeviceId = Guid.NewGuid();
        _peerInfoPayload = Payloads.EncodePeerInfo(DeviceId, name);
        _transport = new UdpTransport(localPort);
    }

    public Guid DeviceId { get; }
    public int LocalPort => _transport.LocalPort;
    public int LossRate
    {
        get => _transport.LossRate;
        set => _transport.LossRate = value;
    }

    public FileTransferService Transfers { get; } = new();

    public event Action<Peer>? PeerChanged;
    public event Action<IPEndPoint, SocketException>? NetworkError;

    public Peer[] GetPeers()
    {
        lock (_stateLock)
            return _peers.Values.ToArray();
    }

    public void AddManualEndpoint(IPEndPoint endpoint)
    {
        ArgumentNullException.ThrowIfNull(endpoint);
        if (endpoint.AddressFamily != AddressFamily.InterNetwork || endpoint.Port == 0)
            throw new ArgumentException(
                "An IPv4 address and nonzero port are required",
                nameof(endpoint)
            );

        lock (_stateLock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            _manualEndpoints.Add(new IPEndPoint(endpoint.Address, endpoint.Port));
        }
    }

    public Task RunAsync(CancellationToken ct)
    {
        lock (_stateLock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (_runTask is not null)
                throw new InvalidOperationException("Peer service has already been started");

            _runTask = RunCoreAsync(ct);
            return _runTask;
        }
    }

    public async Task<FileTransfer> OfferFileAsync(
        Guid peerDeviceId,
        string filePath,
        CancellationToken ct
    )
    {
        ct.ThrowIfCancellationRequested();
        lock (_stateLock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            if (!_peers.ContainsKey(peerDeviceId))
                throw new ArgumentException("Unknown device", nameof(peerDeviceId));
        }
        var transfer = await Transfers.PrepareOutgoingTransferAsync(peerDeviceId, filePath, ct);
        try
        {
            var payload = Payloads.EncodeFileOffer(
                DeviceId,
                transfer.FileId,
                transfer.FileSize,
                transfer.Sha256,
                transfer.FileName
            );
            var packet = new Packet(MessageType.FileOffer, _transport.NextSeq(), payload);

            Peer peer;
            lock (_stateLock)
            {
                ObjectDisposedException.ThrowIf(_disposed, this);
                peer = _peers[peerDeviceId];
            }

            await _transport.SendReliableAsync(packet, peer.Endpoint, peerDeviceId, ct);
        }
        catch
        {
            Transfers.SetState(peerDeviceId, transfer.FileId, TransferState.Failed);
            throw;
        }
        await Transfers.WaitForFileAnswerAsync(peerDeviceId, transfer.FileId, ct);

        return transfer;
    }

    public async Task<FileTransfer> AnswerFileOfferAsync(
        Guid peerDeviceId,
        Guid fileId,
        bool accepted,
        string directory,
        CancellationToken ct
    )
    {
        ct.ThrowIfCancellationRequested();
        Peer peer;
        lock (_stateLock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            if (!_peers.TryGetValue(peerDeviceId, out var value))
                throw new ArgumentException("Peer not exists");

            peer = value;
        }
        var transfer = Transfers.SetIncomingDecision(peerDeviceId, fileId, accepted, directory);
        try
        {
            var payload = Payloads.EncodeFileAnswer(DeviceId, fileId, accepted, nextIndex: 0);
            var packet = new Packet(MessageType.FileAnswer, _transport.NextSeq(), payload);

            await _transport.SendReliableAsync(packet, peer.Endpoint, peerDeviceId, ct);
        }
        catch
        {
            Transfers.SetState(peerDeviceId, fileId, TransferState.Failed);
            throw;
        }

        return transfer;
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
                    continue;

                UpdatePeer(info.Value.DeviceId, info.Value.Name, from);
                if (packet.Type == MessageType.Discover)
                    await SendPeerInfoAsync(MessageType.PeerInfo, from).ConfigureAwait(false);
                continue;
            }

            if (
                packet.Type
                is not (MessageType.Heartbeat or MessageType.FileOffer or MessageType.FileAnswer)
            )
                continue;

            var senderDeviceId = new Guid(
                packet.Payload.AsSpan(0, PayloadsConstants.DeviceIdSize),
                bigEndian: true
            );
            if (senderDeviceId == DeviceId || !RefreshPeer(senderDeviceId, from))
                continue;

            if (packet.Type == MessageType.FileOffer)
                Transfers.HandleFileOffer(packet.Payload);

            if (
                packet.Type == MessageType.FileAnswer
                && !_processedMessages.Contains(senderDeviceId, packet.Seq)
            )
            {
                if (!Transfers.HandleFileAnswer(packet.Payload))
                    continue;

                _processedMessages.Add(senderDeviceId, packet.Seq);
            }

            try
            {
                await _transport.SendAckAsync(packet.Seq, from, DeviceId).ConfigureAwait(false);
            }
            catch (SocketException ex) when (IsUnreachable(ex))
            {
                NetworkError?.Invoke(from, ex);
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
            DateTimeOffset.UtcNow,
            PeerAvailability.Available
        );
        lock (_stateLock)
            _peers[deviceId] = peer;
        PeerChanged?.Invoke(peer);
    }

    private bool RefreshPeer(Guid deviceId, IPEndPoint endpoint)
    {
        Peer updated;
        lock (_stateLock)
        {
            if (!_peers.TryGetValue(deviceId, out var peer) || !peer.Endpoint.Equals(endpoint))
                return false;

            updated = peer with
            {
                LastSeen = DateTimeOffset.UtcNow,
                Availability = PeerAvailability.Available,
            };
            _peers[deviceId] = updated;
        }
        PeerChanged?.Invoke(updated);
        return true;
    }

    private async Task MonitorAsync(CancellationToken ct)
    {
        using var timer = new PeriodicTimer(ProtocolConstants.Heartbeat);
        var nextDiscover = DateTimeOffset.MinValue;
        do
        {
            ct.ThrowIfCancellationRequested();
            var now = DateTimeOffset.UtcNow;
            if (now >= nextDiscover)
            {
                await DiscoverAsync().ConfigureAwait(false);
                nextDiscover = now + ProtocolConstants.DiscoverInterval;
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
            await SendPeerInfoAsync(MessageType.Discover, endpoint).ConfigureAwait(false);
    }

    private async Task SendPeerInfoAsync(MessageType type, IPEndPoint endpoint)
    {
        var packet = new Packet(type, _transport.NextSeq(), _peerInfoPayload);
        try
        {
            await _transport.SendAsync(packet, endpoint).ConfigureAwait(false);
        }
        catch (SocketException ex) when (IsUnreachable(ex))
        {
            NetworkError?.Invoke(endpoint, ex);
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
            UpdateAvailability(DateTimeOffset.UtcNow);
        }
        catch (SocketException ex) when (IsUnreachable(ex))
        {
            NetworkError?.Invoke(peer.Endpoint, ex);
        }
    }

    private static bool IsUnreachable(SocketException ex)
    {
        return ex.SocketErrorCode
            is SocketError.NetworkUnreachable
                or SocketError.HostUnreachable
                or SocketError.NetworkDown
                or SocketError.AddressNotAvailable
                or SocketError.ConnectionRefused;
    }

    private void UpdateAvailability(DateTimeOffset now)
    {
        List<Peer> changed = [];
        lock (_stateLock)
        {
            foreach (var peer in _peers.Values.ToArray())
            {
                var elapsed = now - peer.LastSeen;
                PeerAvailability availability;

                if (elapsed >= ProtocolConstants.PeerTimeout)
                    availability = PeerAvailability.Offline;
                else if (elapsed >= ProtocolConstants.ConnectionTimeout)
                    availability = PeerAvailability.Unreachable;
                else
                    availability = PeerAvailability.Available;

                if (availability == peer.Availability)
                    continue;

                var updated = peer with { Availability = availability };
                _peers[peer.DeviceId] = updated;
                changed.Add(updated);
            }
        }
        foreach (var peer in changed)
            PeerChanged?.Invoke(peer);
    }

    public async ValueTask DisposeAsync()
    {
        Task? running;
        lock (_stateLock)
        {
            if (_disposed)
                return;
            _disposed = true;
            running = _runTask;
        }
        _stopCts.Cancel();
        try
        {
            if (running is not null)
                await running.ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (_stopCts.IsCancellationRequested) { }
        finally
        {
            try
            {
                await Transfers.DisposeAsync().ConfigureAwait(false);
            }
            finally
            {
                _transport.Dispose();
                _stopCts.Dispose();
            }
        }
    }
}
