using System.Net;
using System.Security.Cryptography;

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
            await sender.SendAsync(
                new Packet(
                    MessageType.PeerInfo,
                    sender.NextSeq(),
                    Payloads.EncodePeerInfo(service.DeviceId, "Self")
                ),
                target
            );
            await sender.SendAsync(
                new Packet(
                    MessageType.PeerInfo,
                    sender.NextSeq(),
                    Payloads.EncodePeerInfo(deviceId, "Remote")
                ),
                target
            );
            await WaitForPeerAsync(service, deviceId, PeerAvailability.Available, cts.Token);
            Assert.Equal(sender.LocalPort, Assert.Single(service.GetPeers()).Port);

            await moved.SendAsync(
                new Packet(
                    MessageType.PeerInfo,
                    moved.NextSeq(),
                    Payloads.EncodePeerInfo(deviceId, "Renamed")
                ),
                target
            );
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

    [Fact]
    public async Task OfferFileAsync_SendsChunksInOrder_AndAdvancesOnlyAfterAck()
    {
        var path = Path.Combine(Path.GetTempPath(), $"protocol-send-{Guid.NewGuid():N}.bin");
        var data = Enumerable.Range(0, 2500).Select(index => (byte)index).ToArray();
        try
        {
            await File.WriteAllBytesAsync(path, data);
            await using var service = new PeerService("Sender", 0);
            using var remote = new UdpTransport();
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            var running = service.RunAsync(cts.Token);
            var remoteId = Guid.NewGuid();
            try
            {
                var (sending, fileId) = await AcceptOutgoingOfferAsync(service, remote, remoteId, path, cts.Token);
                var first = await ReceiveMessageAsync(remote, remoteId, MessageType.Chunk, cts.Token);
                var retry = await ReceiveMessageAsync(remote, remoteId, MessageType.Chunk, cts.Token);
                Assert.Equal(first.Packet.Seq, retry.Packet.Seq);
                Assert.Equal(first.Packet.Payload, retry.Packet.Payload);
                Assert.Equal(0u, service.Transfers.GetTransfer(remoteId, fileId).NextIndex);

                var packet = retry.Packet;
                var endpoint = retry.From;
                for (uint index = 0; index < 3; index++)
                {
                    var chunk = Payloads.DecodeChunk(packet.Payload);
                    Assert.NotNull(chunk);
                    Assert.Equal(service.DeviceId, chunk.Value.DeviceId);
                    Assert.Equal(fileId, chunk.Value.FileId);
                    Assert.Equal(index, chunk.Value.Index);
                    var offset = (int)index * ProtocolConstants.ChunkSize;
                    Assert.Equal(data.Skip(offset).Take(ProtocolConstants.ChunkSize), chunk.Value.Data);
                    await remote.SendAckAsync(packet.Seq, endpoint, remoteId);
                    if (index == 2)
                        break;
                    var next = await ReceiveMessageAsync(remote, remoteId, MessageType.Chunk, cts.Token);
                    Assert.NotEqual(packet.Seq, next.Packet.Seq);
                    packet = next.Packet;
                    endpoint = next.From;
                }

                var done = await ReceiveMessageAsync(remote, remoteId, MessageType.FileDone, cts.Token);
                Assert.Equal(fileId, Payloads.DecodeFileDone(done.Packet.Payload)!.Value.FileId);
                await remote.SendReliableAsync(new Packet(MessageType.FileResult, remote.NextSeq(),
                    Payloads.EncodeFileResult(remoteId, fileId, true)), done.From, service.DeviceId, cts.Token);
                var doneRetry = await ReceiveMessageAsync(remote, remoteId, MessageType.FileDone, cts.Token);
                Assert.Equal(done.Packet.Seq, doneRetry.Packet.Seq);
                await remote.SendAckAsync(doneRetry.Packet.Seq, doneRetry.From, remoteId);

                var transfer = await sending.WaitAsync(cts.Token);
                Assert.Equal(3u, transfer.NextIndex);
                Assert.Equal(TransferState.Completed, transfer.State);
                await remote.SendReliableAsync(new Packet(MessageType.FileResult, remote.NextSeq(),
                    Payloads.EncodeFileResult(remoteId, fileId, true)), done.From, service.DeviceId, cts.Token);
            }
            finally
            {
                cts.Cancel();
                await Assert.ThrowsAnyAsync<OperationCanceledException>(() => running);
            }
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task OfferFileAsync_PausesWithoutAdvancing_WhenChunkIsNotAcknowledged()
    {
        var path = Path.Combine(Path.GetTempPath(), $"protocol-send-{Guid.NewGuid():N}.bin");
        try
        {
            await File.WriteAllBytesAsync(path, new byte[2048]);
            await using var service = new PeerService("Sender", 0);
            using var remote = new UdpTransport();
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            var running = service.RunAsync(cts.Token);
            var remoteId = Guid.NewGuid();
            try
            {
                var (sending, fileId) = await AcceptOutgoingOfferAsync(service, remote, remoteId, path, cts.Token);
                uint? seq = null;
                for (var attempt = 0; attempt < ProtocolConstants.MaxSendAttempts; attempt++)
                {
                    var received = await ReceiveMessageAsync(remote, remoteId, MessageType.Chunk, cts.Token);
                    seq ??= received.Packet.Seq;
                    Assert.Equal(seq.Value, received.Packet.Seq);
                    Assert.Equal(0u, Payloads.DecodeChunk(received.Packet.Payload)!.Value.Index);
                }
                var transfer = await sending.WaitAsync(cts.Token);
                Assert.Equal(fileId, transfer.FileId);
                Assert.Equal(TransferState.Paused, transfer.State);
                Assert.Equal(0u, transfer.NextIndex);
            }
            finally
            {
                cts.Cancel();
                await Assert.ThrowsAnyAsync<OperationCanceledException>(() => running);
            }
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Theory]
    [InlineData(0)]
    [InlineData(2500)]
    public async Task OfferFileAsync_CompletesTransfer_AndSavesMatchingFile(int size)
    {
        var directory = Path.Combine(Path.GetTempPath(), $"protocol-peers-{Guid.NewGuid():N}");
        Directory.CreateDirectory(directory);
        var path = Path.Combine(directory, "source.bin");
        try
        {
            var data = Enumerable.Range(0, size).Select(index => (byte)index).ToArray();
            await File.WriteAllBytesAsync(path, data);
            await using var sender = new PeerService("Sender", 0);
            await using var receiver = new PeerService("Receiver", 0);
            sender.AddManualEndpoint(new IPEndPoint(IPAddress.Loopback, receiver.LocalPort));
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            var offered = new TaskCompletionSource<FileTransfer>(TaskCreationOptions.RunContinuationsAsynchronously);
            receiver.Transfers.FileOffered += transfer => offered.TrySetResult(transfer);
            var senderRun = sender.RunAsync(cts.Token);
            var receiverRun = receiver.RunAsync(cts.Token);
            try
            {
                await WaitForPeerAsync(sender, receiver.DeviceId, PeerAvailability.Available, cts.Token);
                var sending = sender.OfferFileAsync(receiver.DeviceId, path, cts.Token);
                var offer = await offered.Task.WaitAsync(cts.Token);
                await receiver.AnswerFileOfferAsync(sender.DeviceId, offer.FileId, true, directory, cts.Token);
                var transfer = await sending.WaitAsync(cts.Token);
                var expectedIndex = (uint)((size + ProtocolConstants.ChunkSize - 1) / ProtocolConstants.ChunkSize);
                Assert.Equal(expectedIndex, transfer.NextIndex);
                Assert.Equal(expectedIndex, receiver.Transfers.GetTransfer(sender.DeviceId, offer.FileId).NextIndex);
                Assert.Equal(TransferState.Completed, transfer.State);
                Assert.Equal(TransferState.Completed, receiver.Transfers.GetTransfer(sender.DeviceId, offer.FileId).State);
                var savedPath = Assert.Single(Directory.GetFiles(directory), file => file != path);
                Assert.Equal(data, await File.ReadAllBytesAsync(savedPath, cts.Token));
                Assert.Equal(SHA256.HashData(data), SHA256.HashData(await File.ReadAllBytesAsync(savedPath, cts.Token)));
                Assert.Empty(Directory.GetFiles(directory, "*.part"));
            }
            finally
            {
                cts.Cancel();
                await Assert.ThrowsAnyAsync<OperationCanceledException>(() => senderRun);
                await Assert.ThrowsAnyAsync<OperationCanceledException>(() => receiverRun);
            }
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    [Theory]
    [InlineData(true, true)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    public async Task RunAsync_FileDone_SendsStoredResult_AndKeepsReceiving(
        bool validHash,
        bool acknowledgeResult
    )
    {
        var directory = Path.Combine(Path.GetTempPath(), $"protocol-done-{Guid.NewGuid():N}");
        try
        {
            await using var receiver = new PeerService("Receiver", 0);
            using var remote = new UdpTransport();
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
            var running = receiver.RunAsync(cts.Token);
            var remoteId = Guid.NewGuid();
            var fileId = Guid.NewGuid();
            var target = new IPEndPoint(IPAddress.Loopback, receiver.LocalPort);
            byte[] data = [1, 2, 3];
            var deliveryFailed = new TaskCompletionSource<Exception>(TaskCreationOptions.RunContinuationsAsynchronously);
            receiver.FileResultDeliveryFailed += (_, error) => deliveryFailed.TrySetResult(error);
            try
            {
                await remote.SendAsync(new Packet(MessageType.PeerInfo, remote.NextSeq(),
                    Payloads.EncodePeerInfo(remoteId, "Sender")), target);
                await WaitForPeerAsync(receiver, remoteId, PeerAvailability.Available, cts.Token);
                await remote.SendReliableAsync(new Packet(MessageType.FileOffer, remote.NextSeq(),
                    Payloads.EncodeFileOffer(remoteId, fileId, (ulong)data.Length,
                        validHash ? SHA256.HashData(data) : new byte[32], "report.bin")),
                    target, receiver.DeviceId, cts.Token);
                var answering = receiver.AnswerFileOfferAsync(remoteId, fileId, true, directory, cts.Token);
                var answer = await ReceiveMessageAsync(remote, remoteId, MessageType.FileAnswer, cts.Token);
                await remote.SendAckAsync(answer.Packet.Seq, answer.From, remoteId);
                await answering;
                await remote.SendReliableAsync(new Packet(MessageType.Chunk, remote.NextSeq(),
                    Payloads.EncodeChunk(remoteId, fileId, 0, data)), target, receiver.DeviceId, cts.Token);
                var done = new Packet(MessageType.FileDone, remote.NextSeq(), Payloads.EncodeFileDone(remoteId, fileId));
                await remote.SendReliableAsync(done, target, receiver.DeviceId, cts.Token);
                var result = await ReceiveMessageAsync(remote, remoteId, MessageType.FileResult, cts.Token);
                Assert.Equal(validHash, Payloads.DecodeFileResult(result.Packet.Payload)!.Value.Ok);

                // A pending FILE_RESULT ACK must not prevent the receiver from processing other messages.
                await remote.SendReliableAsync(new Packet(MessageType.Heartbeat, remote.NextSeq(),
                    Payloads.EncodeHeartbeat(remoteId)), target, receiver.DeviceId, cts.Token);

                if (acknowledgeResult)
                {
                    await remote.SendAckAsync(result.Packet.Seq, result.From, remoteId);
                    for (var repeat = 0; repeat < 2; repeat++)
                    {
                        if (repeat == 1)
                            done = new Packet(MessageType.FileDone, remote.NextSeq(), done.Payload);
                        await remote.SendReliableAsync(done, target, receiver.DeviceId, cts.Token);
                        var repeated = await ReceiveMessageAsync(remote, remoteId, MessageType.FileResult, cts.Token);
                        while (repeated.Packet.Seq == result.Packet.Seq)
                        {
                            await remote.SendAckAsync(repeated.Packet.Seq, repeated.From, remoteId);
                            repeated = await ReceiveMessageAsync(remote, remoteId, MessageType.FileResult, cts.Token);
                        }
                        Assert.Equal(validHash, Payloads.DecodeFileResult(repeated.Packet.Payload)!.Value.Ok);
                        await remote.SendAckAsync(repeated.Packet.Seq, repeated.From, remoteId);
                        result = repeated;
                    }
                }
                else
                {
                    Assert.IsType<TimeoutException>(await deliveryFailed.Task.WaitAsync(cts.Token));
                    Assert.False(running.IsCompleted);
                }

                Assert.Equal(validHash ? TransferState.Completed : TransferState.Failed,
                    receiver.Transfers.GetTransfer(remoteId, fileId).State);
                Assert.Empty(Directory.GetFiles(directory, "*.part"));
                if (validHash)
                    Assert.Equal(data, await File.ReadAllBytesAsync(Assert.Single(Directory.GetFiles(directory)), cts.Token));
                else
                    Assert.Empty(Directory.GetFiles(directory));

                if (!acknowledgeResult)
                {
                    await remote.SendReliableAsync(done, target, receiver.DeviceId, cts.Token);
                    await ReceiveMessageAsync(remote, remoteId, MessageType.FileResult, cts.Token);
                    await receiver.DisposeAsync();
                    Assert.True(File.Exists(Path.Combine(directory, "report.bin")));
                }
            }
            finally
            {
                cts.Cancel();
                await Assert.ThrowsAnyAsync<OperationCanceledException>(() => running);
            }
        }
        finally
        {
            if (Directory.Exists(directory))
                Directory.Delete(directory, recursive: true);
        }
    }

    private static async Task<(Task<FileTransfer> Sending, Guid FileId)> AcceptOutgoingOfferAsync(
        PeerService service,
        UdpTransport remote,
        Guid remoteId,
        string path,
        CancellationToken ct
    )
    {
        var endpoint = new IPEndPoint(IPAddress.Loopback, service.LocalPort);
        await remote.SendAsync(new Packet(MessageType.PeerInfo, remote.NextSeq(),
            Payloads.EncodePeerInfo(remoteId, "Receiver")), endpoint);
        await WaitForPeerAsync(service, remoteId, PeerAvailability.Available, ct);
        var sending = service.OfferFileAsync(remoteId, path, ct);
        var received = await ReceiveMessageAsync(remote, remoteId, MessageType.FileOffer, ct);
        var offer = Payloads.DecodeFileOffer(received.Packet.Payload);
        Assert.NotNull(offer);
        await remote.SendAckAsync(received.Packet.Seq, received.From, remoteId);
        await remote.SendReliableAsync(new Packet(MessageType.FileAnswer, remote.NextSeq(),
            Payloads.EncodeFileAnswer(remoteId, offer.Value.FileId, true, 0)), received.From, service.DeviceId, ct);
        return (sending, offer.Value.FileId);
    }

    private static async Task<(Packet Packet, IPEndPoint From)> ReceiveMessageAsync(
        UdpTransport remote,
        Guid remoteId,
        MessageType type,
        CancellationToken ct
    )
    {
        while (true)
        {
            var received = await remote.ReceiveAsync(ct);
            if (received.Packet.Type == type)
                return received;
            if (received.Packet.Type == MessageType.Heartbeat)
                await remote.SendAckAsync(received.Packet.Seq, received.From, remoteId);
        }
    }

    private static async Task WaitForPeerAsync(
        PeerService service,
        Guid deviceId,
        PeerAvailability availability,
        CancellationToken ct
    )
    {
        while (
            !service
                .GetPeers()
                .Any(peer => peer.DeviceId == deviceId && peer.Availability == availability)
        )
            await Task.Delay(20, ct);
    }
}
