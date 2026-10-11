using System.Net;

namespace Protocol.Tests;

public class UdpTransportTests
{
    [Theory]
    [InlineData("device")]
    [InlineData("endpoint")]
    [InlineData("seq")]
    [InlineData("legacy")]
    [InlineData("extraBytes")]
    public async Task SendReliableAsync_IgnoresAck_WhenItDoesNotMatch(string mismatch)
    {
        using var sender = new UdpTransport();
        using var receiver = new UdpTransport();
        using var other = new UdpTransport();
        var deviceId = Guid.NewGuid();
        var target = new IPEndPoint(IPAddress.Loopback, receiver.LocalPort);
        var packet = new Packet(
            MessageType.Chunk,
            sender.NextSeq(),
            Payloads.EncodeChunk(Guid.NewGuid(), Guid.NewGuid(), 0, [1, 2, 3])
        );
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var sending = sender.SendReliableAsync(packet, target, deviceId, cts.Token);
        var (first, from) = await receiver.ReceiveAsync(cts.Token);
        Assert.NotNull(first);

        var payload = Payloads.EncodeAck(
            mismatch == "device" ? Guid.NewGuid() : deviceId,
            mismatch == "seq" ? packet.Seq + 1 : packet.Seq
        );
        if (mismatch == "legacy")
            payload = [0, 0, 0, 0, 0, 0];
        if (mismatch == "extraBytes")
            payload = [.. payload, 0];

        var ackSender = mismatch == "endpoint" ? other : receiver;
        await ackSender.SendAsync(new Packet(MessageType.Ack, ackSender.NextSeq(), payload), from);

        var (retry, _) = await receiver.ReceiveAsync(cts.Token);
        Assert.NotNull(retry);
        Assert.Equal(packet.Seq, retry.Seq);
        Assert.False(sending.IsCompleted);

        await receiver.SendAsync(
            new Packet(
                MessageType.Ack,
                receiver.NextSeq(),
                Payloads.EncodeAck(deviceId, packet.Seq)
            ),
            from
        );
        await sending;
    }

    [Fact]
    public async Task SendReliableAsync_Stops_WhenCancelled()
    {
        using var sender = new UdpTransport();
        using var receiver = new UdpTransport();
        var target = new IPEndPoint(IPAddress.Loopback, receiver.LocalPort);
        var packet = new Packet(
            MessageType.Chunk,
            sender.NextSeq(),
            Payloads.EncodeChunk(Guid.NewGuid(), Guid.NewGuid(), 0, [1])
        );
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var sending = sender.SendReliableAsync(packet, target, Guid.NewGuid(), cts.Token);
        await receiver.ReceiveAsync(cts.Token);
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => sending);
    }

    [Fact]
    public async Task SendAsync_DeliversPacketToOtherTransport()
    {
        using var sender = new UdpTransport();
        using var receiver = new UdpTransport();

        var target = new IPEndPoint(IPAddress.Loopback, receiver.LocalPort);
        var packet = new Packet(
            MessageType.Chunk,
            sender.NextSeq(),
            Payloads.EncodeChunk(Guid.NewGuid(), Guid.NewGuid(), 0, [1, 2, 3])
        );
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));

        await sender.SendAsync(packet, target);
        var (received, _) = await receiver.ReceiveAsync(cts.Token);

        Assert.NotNull(received);
        Assert.Equal(packet.Seq, received.Seq);
        Assert.Equal(packet.Payload, received.Payload);
    }

    [Fact]
    public async Task SendReliableAsync_Completes_WhenAckArrives()
    {
        using var sender = new UdpTransport();
        using var receiver = new UdpTransport();

        var target = new IPEndPoint(IPAddress.Loopback, receiver.LocalPort);
        var deviceId = Guid.NewGuid();
        var packet = new Packet(
            MessageType.Chunk,
            sender.NextSeq(),
            Payloads.EncodeChunk(Guid.NewGuid(), Guid.NewGuid(), 0, [1, 2, 3])
        );
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));

        var sending = sender.SendReliableAsync(packet, target, deviceId, cts.Token);

        var (received, from) = await receiver.ReceiveAsync(cts.Token);

        Assert.NotNull(received);
        var ack = new Packet(
            MessageType.Ack,
            receiver.NextSeq(),
            Payloads.EncodeAck(deviceId, received.Seq)
        );
        await receiver.SendAsync(ack, from);

        await sending;
        Assert.True(sending.IsCompletedSuccessfully);
    }

    [Fact]
    public async Task SendReliableAsync_Throws_WhenNoAckArrives()
    {
        using var sender = new UdpTransport();
        using var receiver = new UdpTransport();

        var target = new IPEndPoint(IPAddress.Loopback, receiver.LocalPort);
        var deviceId = Guid.NewGuid();
        var packet = new Packet(
            MessageType.Chunk,
            sender.NextSeq(),
            Payloads.EncodeChunk(Guid.NewGuid(), Guid.NewGuid(), 0, [1, 2, 3])
        );
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        var sending = sender.SendReliableAsync(packet, target, deviceId, cts.Token);
        for (var attempt = 0; attempt < 5; attempt++)
        {
            var (received, _) = await receiver.ReceiveAsync(cts.Token);
            Assert.NotNull(received);
            Assert.Equal(packet.Seq, received.Seq);
        }

        await Assert.ThrowsAsync<TimeoutException>(() => sending);
        using var noMorePackets = new CancellationTokenSource(TimeSpan.FromMilliseconds(300));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await receiver.ReceiveAsync(noMorePackets.Token)
        );
    }

    [Fact]
    public async Task SendReliableAsync_Retries_WithSameSeq()
    {
        using var sender = new UdpTransport();
        using var receiver = new UdpTransport();

        var target = new IPEndPoint(IPAddress.Loopback, receiver.LocalPort);
        var deviceId = Guid.NewGuid();
        var packet = new Packet(
            MessageType.Chunk,
            sender.NextSeq(),
            Payloads.EncodeChunk(Guid.NewGuid(), Guid.NewGuid(), 0, [1, 2, 3])
        );
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        var sending = sender.SendReliableAsync(packet, target, deviceId, cts.Token);

        var (firstReceived, _) = await receiver.ReceiveAsync(cts.Token);
        var (secondReceived, from) = await receiver.ReceiveAsync(cts.Token);

        Assert.NotNull(firstReceived);
        Assert.NotNull(secondReceived);
        Assert.Equal(packet.Seq, firstReceived.Seq);
        Assert.Equal(packet.Seq, secondReceived.Seq);
        Assert.Equal(firstReceived.Payload, secondReceived.Payload);

        await receiver.SendAsync(
            new Packet(
                MessageType.Ack,
                receiver.NextSeq(),
                Payloads.EncodeAck(deviceId, secondReceived.Seq)
            ),
            from
        );
        await sending;
    }
}
