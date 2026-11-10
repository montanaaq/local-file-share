using System.Net;
using System.Net.Sockets;
using Protocol.Extensions;
using Protocol.Messages;
using Protocol.Transport;

namespace Protocol.Tests;

public class UdpTransportTests
{
    [Fact]
    public async Task GetStatistics_CountsRetriesAndMatchingAcknowledgments()
    {
        using var sender = new UdpTransport();
        using var receiver = new UdpTransport();
        var endpoint = new IPEndPoint(IPAddress.Loopback, receiver.LocalPort);
        var receiverId = Guid.NewGuid();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var packet = new Packet(
            MessageType.Chunk,
            sender.NextSeq(),
            Payloads.EncodeChunk(Guid.NewGuid(), Guid.NewGuid(), 0, [1])
        );

        var sending = sender.SendReliableAsync(packet, endpoint, receiverId, timeout.Token);
        var first = await receiver.ReceiveAsync(timeout.Token);
        var retry = await receiver.ReceiveAsync(timeout.Token);
        Assert.Equal(packet.Seq, first.Packet.Seq);
        Assert.Equal(first.Packet.Seq, retry.Packet.Seq);
        Assert.Equal(packet.Payload, first.Packet.Payload);
        Assert.Equal(first.Packet.Payload, retry.Packet.Payload);
        await receiver.SendAckAsync(packet.Seq, retry.From, receiverId);
        await sending;

        var sent = sender.GetStatistics();
        var received = receiver.GetStatistics();
        Assert.True(sent.SentPackets >= 2);
        Assert.Equal(sent.SentPackets - 1, sent.Retries);
        Assert.Equal(1, sent.AcknowledgmentsReceived);
        Assert.True(received.ReceivedPackets >= 2);
        Assert.Equal(1, received.AcknowledgmentsSent);
        Assert.Equal(0, sent.Errors);
    }

    [Fact]
    public async Task GetStatistics_CountsSimulatedLossInBothDirections()
    {
        using var sender = new UdpTransport();
        using var receiver = new UdpTransport();
        var endpoint = new IPEndPoint(IPAddress.Loopback, receiver.LocalPort);
        var deviceId = Guid.NewGuid();
        sender.LossRate = 100;
        await sender.SendAsync(
            new Packet(MessageType.Heartbeat, sender.NextSeq(), Payloads.EncodeHeartbeat(deviceId)),
            endpoint
        );
        Assert.Equal(0, sender.GetStatistics().SentPackets);
        Assert.Equal(1, sender.GetStatistics().DroppedOutgoingPackets);

        sender.LossRate = 0;
        receiver.LossRate = 100;
        await sender.SendAsync(
            new Packet(MessageType.Heartbeat, sender.NextSeq(), Payloads.EncodeHeartbeat(deviceId)),
            endpoint
        );
        using var timeout = new CancellationTokenSource(TimeSpan.FromMilliseconds(200));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await receiver.ReceiveAsync(timeout.Token)
        );

        Assert.Equal(1, sender.GetStatistics().SentPackets);
        Assert.Equal(1, receiver.GetStatistics().ReceivedPackets);
        Assert.Equal(1, receiver.GetStatistics().DroppedIncomingPackets);
    }
}
