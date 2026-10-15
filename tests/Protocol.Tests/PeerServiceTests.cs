using System.Net;

namespace Protocol.Tests;

public class PeerServiceTests
{
    [Fact]
    public async Task RunAsync_DiscoversPeersAndRestoresAvailability_AfterPacketLoss()
    {
        await using var first = new PeerService("First", 0);
        await using var second = new PeerService("Second", 0);
        first.AddManualEndpoint(new IPEndPoint(IPAddress.Loopback, second.LocalPort));
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var firstRun = first.RunAsync(cts.Token);
        var secondRun = second.RunAsync(cts.Token);

        try
        {
            await WaitForPeerAsync(first, second.DeviceId, PeerAvailability.Available, cts.Token);
            await WaitForPeerAsync(second, first.DeviceId, PeerAvailability.Available, cts.Token);
            var initial = Assert.Single(first.GetPeers());
            Assert.Equal("Second", initial.Name);
            Assert.Equal(second.LocalPort, initial.Port);

            await Task.Delay(TimeSpan.FromMilliseconds(1500), cts.Token);
            Assert.True(Assert.Single(first.GetPeers()).LastSeen > initial.LastSeen);

            second.LossRate = 100;
            await WaitForPeerAsync(first, second.DeviceId, PeerAvailability.Unreachable, cts.Token);
            await WaitForPeerAsync(first, second.DeviceId, PeerAvailability.Offline, cts.Token);
            Assert.Single(first.GetPeers());

            second.LossRate = 0;
            await WaitForPeerAsync(first, second.DeviceId, PeerAvailability.Available, cts.Token);
            Assert.Single(first.GetPeers());
        }
        finally
        {
            cts.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => firstRun);
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => secondRun);
        }
    }

    [Fact]
    public async Task RunAsync_IgnoresOwnDeviceAndUpdatesEndpoint_WhenPeerIsRediscovered()
    {
        await using var service = new PeerService("Local", 0);
        using var sender = new UdpTransport();
        using var moved = new UdpTransport();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var running = service.RunAsync(cts.Token);
        var target = new IPEndPoint(IPAddress.Loopback, service.LocalPort);
        var deviceId = Guid.NewGuid();
        try
        {
            await sender.SendAsync(new Packet(MessageType.PeerInfo, sender.NextSeq(),
                Payloads.EncodePeerInfo(service.DeviceId, "Self")), target);
            await sender.SendAsync(new Packet(MessageType.PeerInfo, sender.NextSeq(),
                Payloads.EncodePeerInfo(deviceId, "Remote")), target);
            await WaitForPeerAsync(service, deviceId, PeerAvailability.Available, cts.Token);
            Assert.Equal(sender.LocalPort, Assert.Single(service.GetPeers()).Port);

            await moved.SendAsync(new Packet(MessageType.PeerInfo, moved.NextSeq(),
                Payloads.EncodePeerInfo(deviceId, "Renamed")), target);
            while (Assert.Single(service.GetPeers()).Port != moved.LocalPort)
                await Task.Delay(20, cts.Token);
            Assert.Equal("Renamed", Assert.Single(service.GetPeers()).Name);
        }
        finally
        {
            cts.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => running);
        }
    }

    private static async Task WaitForPeerAsync(
        PeerService service,
        Guid deviceId,
        PeerAvailability availability,
        CancellationToken ct
    )
    {
        while (!service.GetPeers().Any(peer => peer.DeviceId == deviceId && peer.Availability == availability))
            await Task.Delay(20, ct);
    }
}
