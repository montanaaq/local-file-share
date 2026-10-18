using System.Net;
using Microsoft.VisualStudio.TestPlatform.CommunicationUtilities;

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

        byte[] payload = Payloads.EncodeAck(
            mismatch == "device" ? Guid.NewGuid() : deviceId,
            mismatch == "seq" ? packet.Seq + 1 : packet.Seq
        );
        if (mismatch == "legacy")
        {
            payload = [0, 0, 0, 0, 0, 0];
        }

        if (mismatch == "extraBytes")
        {
            payload = [.. payload, 0];
        }

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

        await receiver.SendAckAsync(packet.Seq, from, deviceId);

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
        for (int attempt = 0; attempt < 5; attempt++)
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

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SendReliableAsync_CompletesMatchingSend_WhenAcksArriveInReverseOrder(
        bool differentEndpoints
    )
    {
        using var sender = new UdpTransport();
        using var receiver = new UdpTransport();
        using var other = differentEndpoints ? new UdpTransport() : null;
        var secondReceiver = other ?? receiver;

        var target = new IPEndPoint(IPAddress.Loopback, receiver.LocalPort);
        var secondTarget = new IPEndPoint(IPAddress.Loopback, secondReceiver.LocalPort);
        var deviceId = Guid.NewGuid();
        var senderDeviceId = Guid.NewGuid();
        var fileId = Guid.NewGuid();
        var firstPacket = new Packet(
            MessageType.Chunk,
            sender.NextSeq(),
            Payloads.EncodeChunk(senderDeviceId, fileId, 0, [1, 2, 3])
        );
        var secondPacket = new Packet(
            MessageType.Chunk,
            sender.NextSeq(),
            Payloads.EncodeChunk(senderDeviceId, fileId, 1, [4, 5, 6])
        );
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        var firstSending = sender.SendReliableAsync(firstPacket, target, deviceId, cts.Token);
        var (firstReceived, from) = await receiver.ReceiveAsync(cts.Token);
        Assert.Equal(firstPacket.Seq, firstReceived.Seq);
        Assert.Equal(firstPacket.Payload, firstReceived.Payload);

        var secondSending = sender.SendReliableAsync(
            secondPacket,
            secondTarget,
            deviceId,
            cts.Token
        );
        Packet secondReceived;
        IPEndPoint secondFrom;
        do
        {
            (secondReceived, secondFrom) = await secondReceiver.ReceiveAsync(cts.Token);
        } while (secondReceived.Seq != secondPacket.Seq);

        Assert.Equal(secondPacket.Payload, secondReceived.Payload);
        Assert.Equal(from, secondFrom);
        Assert.False(firstSending.IsCompleted);
        Assert.False(secondSending.IsCompleted);

        var secondAck = new Packet(
            MessageType.Ack,
            secondReceiver.NextSeq(),
            Payloads.EncodeAck(deviceId, secondPacket.Seq)
        );
        await secondReceiver.SendAsync(secondAck, secondFrom);
        await secondSending.WaitAsync(cts.Token);

        Assert.True(secondSending.IsCompletedSuccessfully);
        Assert.False(firstSending.IsCompleted);

        var firstAck = new Packet(
            MessageType.Ack,
            receiver.NextSeq(),
            Payloads.EncodeAck(deviceId, firstPacket.Seq)
        );
        await receiver.SendAsync(firstAck, from);
        await firstSending.WaitAsync(cts.Token);

        Assert.True(firstSending.IsCompletedSuccessfully);
    }

    [Fact]
    public async Task SendReliableAsync_Throws_WhenTransportIsDisposed()
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

        await receiver.ReceiveAsync(cts.Token);
        sender.Dispose();

        await Assert.ThrowsAsync<ObjectDisposedException>(() => sending);
    }

    [Fact]
    public async Task SendReliableAsync_Throws_WhenTransportIsAlreadyDisposed()
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

        sender.Dispose();

        await Assert.ThrowsAsync<ObjectDisposedException>(() =>
            sender.SendReliableAsync(packet, target, deviceId, cts.Token)
        );
    }

    [Fact]
    public async Task SendReliableAsync_PreservesIncomingMessage_WhileWaitingForAck()
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
        var (original, from) = await receiver.ReceiveAsync(cts.Token);
        Assert.Equal(packet.Seq, original.Seq);

        var incoming = new Packet(
            MessageType.Clipboard,
            receiver.NextSeq(),
            Payloads.EncodeClipboard(deviceId, "Hi")
        );
        await receiver.SendAsync(incoming, from);

        var (received, source) = await sender.ReceiveAsync(cts.Token);

        Assert.Equal(incoming.Type, received.Type);
        Assert.Equal(incoming.Seq, received.Seq);
        Assert.Equal(incoming.Payload, received.Payload);
        Assert.Equal(target, source);

        Assert.False(sending.IsCompleted);
        var ack = new Packet(
            MessageType.Ack,
            receiver.NextSeq(),
            Payloads.EncodeAck(deviceId, packet.Seq)
        );
        await receiver.SendAsync(ack, from);

        await sending;

        Assert.True(sending.IsCompletedSuccessfully);
    }

    [Fact]
    public async Task ReceiveAsync_IgnoresPacket_WhenPayloadIsInvalid()
    {
        using var sender = new UdpTransport();
        using var receiver = new UdpTransport();

        var target = new IPEndPoint(IPAddress.Loopback, receiver.LocalPort);
        var invalidPacket = new Packet(MessageType.Heartbeat, sender.NextSeq(), [1, 2, 3]);
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));

        var sending = sender.SendAsync(invalidPacket, target);

        var validPacket = new Packet(
            MessageType.Heartbeat,
            sender.NextSeq(),
            Payloads.EncodeHeartbeat(Guid.NewGuid())
        );

        await sender.SendAsync(validPacket, target);

        var (received, _) = await receiver.ReceiveAsync(cts.Token);

        Assert.Equal(validPacket.Type, received.Type);
        Assert.Equal(validPacket.Seq, received.Seq);
        Assert.Equal(validPacket.Payload, received.Payload);
    }

    [Fact]
    public async Task ReceiveAsync_ReusingSocketBuffer_DoesNotChangePreviouslyReceivedPayload()
    {
        using var sender = new UdpTransport();
        using var receiver = new UdpTransport();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var endpoint = new IPEndPoint(IPAddress.Loopback, receiver.LocalPort);
        var deviceId = Guid.NewGuid();
        byte[] firstPayload = Payloads.EncodePeerInfo(deviceId, "First");
        await sender.SendAsync(
            new Packet(MessageType.PeerInfo, sender.NextSeq(), firstPayload),
            endpoint
        );
        var first = await receiver.ReceiveAsync(timeout.Token);
        byte[] secondPayload = Payloads.EncodePeerInfo(deviceId, "Other");
        await sender.SendAsync(
            new Packet(MessageType.PeerInfo, sender.NextSeq(), secondPayload),
            endpoint
        );
        var second = await receiver.ReceiveAsync(timeout.Token);

        Assert.Equal(firstPayload, first.Packet.Payload);
        Assert.Equal(secondPayload, second.Packet.Payload);
    }
}
