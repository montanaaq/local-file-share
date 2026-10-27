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
            var initial = GetPeer(first, second.DeviceId);
            Assert.Equal("Second", initial.Name);
            Assert.Equal(second.LocalPort, initial.Port);

            await Task.Delay(TimeSpan.FromMilliseconds(1500), cts.Token);
            Assert.True(GetPeer(first, second.DeviceId).LastSeen > initial.LastSeen);

            second.LossRate = 100;
            await WaitForPeerAsync(first, second.DeviceId, PeerAvailability.Unreachable, cts.Token);
            await WaitForPeerAsync(first, second.DeviceId, PeerAvailability.Offline, cts.Token);
            GetPeer(first, second.DeviceId);

            second.LossRate = 0;
            await WaitForPeerAsync(first, second.DeviceId, PeerAvailability.Available, cts.Token);
            GetPeer(first, second.DeviceId);
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
            Assert.Equal(sender.LocalPort, GetPeer(service, deviceId).Port);
            Assert.DoesNotContain(service.GetPeers(), peer => peer.DeviceId == service.DeviceId);

            await moved.SendAsync(
                new Packet(
                    MessageType.PeerInfo,
                    moved.NextSeq(),
                    Payloads.EncodePeerInfo(deviceId, "Renamed")
                ),
                target
            );
            while (GetPeer(service, deviceId).Port != moved.LocalPort)
            {
                await Task.Delay(20, cts.Token);
            }

            Assert.Equal("Renamed", GetPeer(service, deviceId).Name);
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
        string path = Path.Combine(Path.GetTempPath(), $"protocol-send-{Guid.NewGuid():N}.bin");
        byte[] data = Enumerable.Range(0, 2500).Select(index => (byte)index).ToArray();
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
                var (sending, fileId) = await AcceptOutgoingOfferAsync(
                    service,
                    remote,
                    remoteId,
                    path,
                    cts.Token
                );
                var first = await ReceiveMessageAsync(
                    remote,
                    remoteId,
                    MessageType.Chunk,
                    cts.Token
                );
                var retry = await ReceiveMessageAsync(
                    remote,
                    remoteId,
                    MessageType.Chunk,
                    cts.Token
                );
                Assert.Equal(first.Packet.Seq, retry.Packet.Seq);
                Assert.Equal(first.Packet.Payload, retry.Packet.Payload);
                Assert.Equal(
                    0u,
                    service.Transfers.GetTransfer(new TransferKey(remoteId, fileId)).NextIndex
                );

                var packet = retry.Packet;
                var endpoint = retry.From;
                for (uint index = 0; index < 3; index++)
                {
                    var chunk = Payloads.DecodeChunk(packet.Payload);
                    Assert.NotNull(chunk);
                    Assert.Equal(service.DeviceId, chunk.Value.DeviceId);
                    Assert.Equal(fileId, chunk.Value.FileId);
                    Assert.Equal(index, chunk.Value.Index);
                    int offset = (int)index * ProtocolConstants.ChunkSize;
                    Assert.Equal(
                        data.Skip(offset).Take(ProtocolConstants.ChunkSize),
                        chunk.Value.Data
                    );
                    await remote.SendAckAsync(packet.Seq, endpoint, remoteId);
                    if (index == 2)
                    {
                        break;
                    }

                    var next = await ReceiveMessageAsync(
                        remote,
                        remoteId,
                        MessageType.Chunk,
                        cts.Token
                    );
                    Assert.NotEqual(packet.Seq, next.Packet.Seq);
                    packet = next.Packet;
                    endpoint = next.From;
                }

                var done = await ReceiveMessageAsync(
                    remote,
                    remoteId,
                    MessageType.FileDone,
                    cts.Token
                );
                Assert.Equal(fileId, Payloads.DecodeFileDone(done.Packet.Payload)!.Value.FileId);
                await remote.SendReliableAsync(
                    new Packet(
                        MessageType.FileResult,
                        remote.NextSeq(),
                        Payloads.EncodeFileResult(remoteId, fileId, true)
                    ),
                    done.From,
                    service.DeviceId,
                    cts.Token
                );
                var doneRetry = await ReceiveMessageAsync(
                    remote,
                    remoteId,
                    MessageType.FileDone,
                    cts.Token
                );
                Assert.Equal(done.Packet.Seq, doneRetry.Packet.Seq);
                await remote.SendAckAsync(doneRetry.Packet.Seq, doneRetry.From, remoteId);

                var transfer = await sending.WaitAsync(cts.Token);
                Assert.Equal(3u, transfer.NextIndex);
                Assert.Equal(TransferState.Completed, transfer.State);
                await remote.SendReliableAsync(
                    new Packet(
                        MessageType.FileResult,
                        remote.NextSeq(),
                        Payloads.EncodeFileResult(remoteId, fileId, true)
                    ),
                    done.From,
                    service.DeviceId,
                    cts.Token
                );
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
        string path = Path.Combine(Path.GetTempPath(), $"protocol-send-{Guid.NewGuid():N}.bin");
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
                var (sending, fileId) = await AcceptOutgoingOfferAsync(
                    service,
                    remote,
                    remoteId,
                    path,
                    cts.Token
                );
                uint? seq = null;
                for (int attempt = 0; attempt < ProtocolConstants.MaxSendAttempts; attempt++)
                {
                    var received = await ReceiveMessageAsync(
                        remote,
                        remoteId,
                        MessageType.Chunk,
                        cts.Token
                    );
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
        string directory = Path.Combine(Path.GetTempPath(), $"protocol-peers-{Guid.NewGuid():N}");
        Directory.CreateDirectory(directory);
        string path = Path.Combine(directory, "source.bin");
        try
        {
            byte[] data = Enumerable.Range(0, size).Select(index => (byte)index).ToArray();
            await File.WriteAllBytesAsync(path, data);
            await using var sender = new PeerService("Sender", 0);
            await using var receiver = new PeerService("Receiver", 0);
            sender.AddManualEndpoint(new IPEndPoint(IPAddress.Loopback, receiver.LocalPort));
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            var offered = new TaskCompletionSource<FileTransfer>(
                TaskCreationOptions.RunContinuationsAsynchronously
            );
            receiver.Transfers.FileOffered += transfer => offered.TrySetResult(transfer);
            var senderRun = sender.RunAsync(cts.Token);
            var receiverRun = receiver.RunAsync(cts.Token);
            try
            {
                await WaitForPeerAsync(
                    sender,
                    receiver.DeviceId,
                    PeerAvailability.Available,
                    cts.Token
                );
                var sending = sender.OfferFileAsync(receiver.DeviceId, path, cts.Token);
                var offer = await offered.Task.WaitAsync(cts.Token);
                await receiver.AnswerFileOfferAsync(
                    new TransferKey(sender.DeviceId, offer.FileId),
                    true,
                    directory,
                    cts.Token
                );
                var transfer = await sending.WaitAsync(cts.Token);
                uint expectedIndex = (uint)(
                    (size + ProtocolConstants.ChunkSize - 1) / ProtocolConstants.ChunkSize
                );
                Assert.Equal(expectedIndex, transfer.NextIndex);
                Assert.Equal(
                    expectedIndex,
                    receiver
                        .Transfers.GetTransfer(new TransferKey(sender.DeviceId, offer.FileId))
                        .NextIndex
                );
                Assert.Equal(TransferState.Completed, transfer.State);
                Assert.Equal(
                    TransferState.Completed,
                    receiver
                        .Transfers.GetTransfer(new TransferKey(sender.DeviceId, offer.FileId))
                        .State
                );
                string savedPath = Assert.Single(
                    Directory.GetFiles(directory),
                    file => file != path
                );
                Assert.Equal(data, await File.ReadAllBytesAsync(savedPath, cts.Token));
                Assert.Equal(
                    SHA256.HashData(data),
                    SHA256.HashData(await File.ReadAllBytesAsync(savedPath, cts.Token))
                );
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
        string directory = Path.Combine(Path.GetTempPath(), $"protocol-done-{Guid.NewGuid():N}");
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
            var deliveryFailed = new TaskCompletionSource<Exception>(
                TaskCreationOptions.RunContinuationsAsynchronously
            );
            receiver.FileResultDeliveryFailed += (_, error) => deliveryFailed.TrySetResult(error);
            try
            {
                await remote.SendAsync(
                    new Packet(
                        MessageType.PeerInfo,
                        remote.NextSeq(),
                        Payloads.EncodePeerInfo(remoteId, "Sender")
                    ),
                    target
                );
                await WaitForPeerAsync(receiver, remoteId, PeerAvailability.Available, cts.Token);
                await remote.SendReliableAsync(
                    new Packet(
                        MessageType.FileOffer,
                        remote.NextSeq(),
                        Payloads.EncodeFileOffer(
                            remoteId,
                            fileId,
                            (ulong)data.Length,
                            validHash ? SHA256.HashData(data) : new byte[32],
                            "report.bin"
                        )
                    ),
                    target,
                    receiver.DeviceId,
                    cts.Token
                );
                var answering = receiver.AnswerFileOfferAsync(
                    new TransferKey(remoteId, fileId),
                    true,
                    directory,
                    cts.Token
                );
                var answer = await ReceiveMessageAsync(
                    remote,
                    remoteId,
                    MessageType.FileAnswer,
                    cts.Token
                );
                await remote.SendAckAsync(answer.Packet.Seq, answer.From, remoteId);
                await answering;
                await remote.SendReliableAsync(
                    new Packet(
                        MessageType.Chunk,
                        remote.NextSeq(),
                        Payloads.EncodeChunk(remoteId, fileId, 0, data)
                    ),
                    target,
                    receiver.DeviceId,
                    cts.Token
                );
                var done = new Packet(
                    MessageType.FileDone,
                    remote.NextSeq(),
                    Payloads.EncodeFileDone(remoteId, fileId)
                );
                await remote.SendReliableAsync(done, target, receiver.DeviceId, cts.Token);
                var result = await ReceiveMessageAsync(
                    remote,
                    remoteId,
                    MessageType.FileResult,
                    cts.Token
                );
                Assert.Equal(validHash, Payloads.DecodeFileResult(result.Packet.Payload)!.Value.Ok);

                // Waiting for a FILE_RESULT ACK must not block other messages.
                await remote.SendReliableAsync(
                    new Packet(
                        MessageType.Heartbeat,
                        remote.NextSeq(),
                        Payloads.EncodeHeartbeat(remoteId)
                    ),
                    target,
                    receiver.DeviceId,
                    cts.Token
                );

                if (acknowledgeResult)
                {
                    await remote.SendAckAsync(result.Packet.Seq, result.From, remoteId);
                    for (int repeat = 0; repeat < 2; repeat++)
                    {
                        if (repeat == 1)
                        {
                            done = new Packet(MessageType.FileDone, remote.NextSeq(), done.Payload);
                        }

                        await remote.SendReliableAsync(done, target, receiver.DeviceId, cts.Token);
                        var repeated = await ReceiveMessageAsync(
                            remote,
                            remoteId,
                            MessageType.FileResult,
                            cts.Token
                        );
                        while (repeated.Packet.Seq == result.Packet.Seq)
                        {
                            await remote.SendAckAsync(repeated.Packet.Seq, repeated.From, remoteId);
                            repeated = await ReceiveMessageAsync(
                                remote,
                                remoteId,
                                MessageType.FileResult,
                                cts.Token
                            );
                        }
                        Assert.Equal(
                            validHash,
                            Payloads.DecodeFileResult(repeated.Packet.Payload)!.Value.Ok
                        );
                        await remote.SendAckAsync(repeated.Packet.Seq, repeated.From, remoteId);
                        result = repeated;
                    }
                }
                else
                {
                    Assert.IsType<TimeoutException>(await deliveryFailed.Task.WaitAsync(cts.Token));
                    Assert.False(running.IsCompleted);
                }

                Assert.Equal(
                    validHash ? TransferState.Completed : TransferState.Failed,
                    receiver.Transfers.GetTransfer(new TransferKey(remoteId, fileId)).State
                );
                Assert.Empty(Directory.GetFiles(directory, "*.part"));
                if (validHash)
                {
                    Assert.Equal(
                        data,
                        await File.ReadAllBytesAsync(
                            Assert.Single(Directory.GetFiles(directory)),
                            cts.Token
                        )
                    );
                }
                else
                {
                    Assert.Empty(Directory.GetFiles(directory));
                }

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
            {
                Directory.Delete(directory, recursive: true);
            }
        }
    }

    [Fact]
    public async Task RunAsync_NotificationFailuresAndSlowSubscribers_DoNotBlockAck()
    {
        await using var service = new PeerService("Local", 0);
        using var remote = new UdpTransport();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        using var release = new ManualResetEventSlim();
        var remoteId = Guid.NewGuid();
        var endpoint = new IPEndPoint(IPAddress.Loopback, service.LocalPort);
        var failure = new InvalidOperationException("Peer subscriber failed");
        var reported = new TaskCompletionSource<Exception>(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        var notified = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var offered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        service.PeerChanged += _ => throw failure;
        service.PeerChanged += _ => notified.TrySetResult();
        service.NotificationFailed += error => reported.TrySetResult(error);
        service.Transfers.FileOffered += _ =>
        {
            offered.TrySetResult();
            release.Wait();
        };
        var running = service.RunAsync(timeout.Token);
        try
        {
            await remote.SendAsync(
                new Packet(
                    MessageType.PeerInfo,
                    remote.NextSeq(),
                    Payloads.EncodePeerInfo(remoteId, "Remote")
                ),
                endpoint
            );
            await notified.Task.WaitAsync(timeout.Token);
            Assert.Same(failure, await reported.Task.WaitAsync(timeout.Token));
            await remote.SendReliableAsync(
                new Packet(
                    MessageType.FileOffer,
                    remote.NextSeq(),
                    Payloads.EncodeFileOffer(
                        remoteId,
                        Guid.NewGuid(),
                        0,
                        SHA256.HashData([]),
                        "empty.bin"
                    )
                ),
                endpoint,
                service.DeviceId,
                timeout.Token
            );
            await offered.Task.WaitAsync(timeout.Token);
            await remote.SendReliableAsync(
                new Packet(
                    MessageType.Heartbeat,
                    remote.NextSeq(),
                    Payloads.EncodeHeartbeat(remoteId)
                ),
                endpoint,
                service.DeviceId,
                timeout.Token
            );
            Assert.False(running.IsCompleted);
        }
        finally
        {
            release.Set();
            timeout.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => running);
        }
    }

    [Fact]
    public async Task RunAsync_UsesElapsedTime_WhenSystemClockMovesForwardAndBackward()
    {
        var clock = new TestTimeProvider();
        await using var service = new PeerService("Local", 0, clock);
        using var remote = new UdpTransport();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var remoteId = Guid.NewGuid();
        var running = service.RunAsync(timeout.Token);
        try
        {
            await remote.SendAsync(
                new Packet(
                    MessageType.PeerInfo,
                    remote.NextSeq(),
                    Payloads.EncodePeerInfo(remoteId, "Remote")
                ),
                new IPEndPoint(IPAddress.Loopback, service.LocalPort)
            );
            await WaitForPeerAsync(service, remoteId, PeerAvailability.Available, timeout.Token);
            clock.MoveUtc(TimeSpan.FromDays(1));
            await Task.Delay(TimeSpan.FromMilliseconds(1300), timeout.Token);
            Assert.Equal(PeerAvailability.Available, GetPeer(service, remoteId).Availability);

            clock.MoveUtc(TimeSpan.FromDays(-2));
            clock.AdvanceElapsed(ProtocolConstants.PeerTimeout);
            await WaitForPeerAsync(service, remoteId, PeerAvailability.Offline, timeout.Token);
        }
        finally
        {
            timeout.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => running);
        }
    }

    [Fact]
    public async Task SendTextAsync_DeliversTextOnce_WhenReliablePacketIsRepeated()
    {
        await using var service = new PeerService("Receiver", 0);
        using var remote = new UdpTransport();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var remoteId = Guid.NewGuid();
        var endpoint = new IPEndPoint(IPAddress.Loopback, service.LocalPort);
        int receivedCount = 0;
        var delivered = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        service.TextReceived += (sender, text) =>
        {
            Assert.Equal(remoteId, sender);
            Assert.Equal("Clipboard text", text);
            if (Interlocked.Increment(ref receivedCount) == 2)
            {
                delivered.TrySetResult();
            }
        };
        var running = service.RunAsync(timeout.Token);
        try
        {
            await remote.SendAsync(
                new Packet(
                    MessageType.PeerInfo,
                    remote.NextSeq(),
                    Payloads.EncodePeerInfo(remoteId, "Sender")
                ),
                endpoint
            );
            await WaitForPeerAsync(service, remoteId, PeerAvailability.Available, timeout.Token);
            var packet = new Packet(
                MessageType.Clipboard,
                remote.NextSeq(),
                Payloads.EncodeClipboard(remoteId, "Clipboard text")
            );
            await remote.SendReliableAsync(packet, endpoint, service.DeviceId, timeout.Token);
            await remote.SendReliableAsync(packet, endpoint, service.DeviceId, timeout.Token);
            await remote.SendReliableAsync(
                packet with
                {
                    Seq = remote.NextSeq(),
                },
                endpoint,
                service.DeviceId,
                timeout.Token
            );
            await delivered.Task.WaitAsync(timeout.Token);
            Assert.Equal(2, Volatile.Read(ref receivedCount));
        }
        finally
        {
            timeout.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => running);
        }
    }

    [Fact]
    public async Task SetExchangeEnabled_PreservesSession_AndTextDeliveryWorksAfterEnabling()
    {
        await using var first = new PeerService("First", 0);
        await using var second = new PeerService("Second", 0);
        first.AddManualEndpoint(new IPEndPoint(IPAddress.Loopback, second.LocalPort));
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var firstRun = first.RunAsync(timeout.Token);
        var secondRun = second.RunAsync(timeout.Token);
        var received = new TaskCompletionSource<string>(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        second.TextReceived += (_, text) => received.TrySetResult(text);
        try
        {
            await WaitForPeerAsync(
                first,
                second.DeviceId,
                PeerAvailability.Available,
                timeout.Token
            );
            await WaitForPeerAsync(
                second,
                first.DeviceId,
                PeerAvailability.Available,
                timeout.Token
            );
            var deviceId = second.DeviceId;
            int port = second.LocalPort;
            second.SetExchangeEnabled(false);
            Assert.False(second.IsExchangeEnabled);
            await Assert.ThrowsAsync<TimeoutException>(() =>
                first.SendTextAsync(second.DeviceId, "Dropped", timeout.Token)
            );
            await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                await second.SendTextAsync(first.DeviceId, "Disabled", timeout.Token)
            );
            Assert.False(received.Task.IsCompleted);
            Assert.False(secondRun.IsCompleted);

            second.SetExchangeEnabled(true);
            Assert.Equal(deviceId, second.DeviceId);
            Assert.Equal(port, second.LocalPort);
            await first.SendTextAsync(second.DeviceId, "Restored", timeout.Token);
            Assert.Equal("Restored", await received.Task.WaitAsync(timeout.Token));
            Assert.True(first.GetStatistics().AcknowledgmentsReceived > 0);
        }
        finally
        {
            timeout.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => firstRun);
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => secondRun);
        }
    }

    [Fact]
    public async Task OfferFileAsync_ResumesAfterBothSidesPause_AndSavesFiveMiBWithMatchingHash()
    {
        string directory = Path.Combine(Path.GetTempPath(), $"protocol-resume-{Guid.NewGuid():N}");
        Directory.CreateDirectory(directory);
        string source = Path.Combine(directory, "source.bin");
        string destination = Path.Combine(directory, "received");
        byte[] data = new byte[5 * 1024 * 1024];
        Random.Shared.NextBytes(data);
        await File.WriteAllBytesAsync(source, data);
        try
        {
            await using var sender = new PeerService("Sender", 0);
            await using var receiver = new PeerService("Receiver", 0);
            sender.AddManualEndpoint(new IPEndPoint(IPAddress.Loopback, receiver.LocalPort));
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(60));
            var senderRun = sender.RunAsync(timeout.Token);
            var receiverRun = receiver.RunAsync(timeout.Token);
            var offered = new TaskCompletionSource<FileTransfer>(
                TaskCreationOptions.RunContinuationsAsynchronously
            );
            receiver.Transfers.FileOffered += transfer => offered.TrySetResult(transfer);
            int interrupted = 0;
            receiver.Transfers.TransferChanged += transfer =>
            {
                if (
                    transfer.State == TransferState.Transferring
                    && transfer.NextIndex >= 4
                    && Interlocked.CompareExchange(ref interrupted, 1, 0) == 0
                )
                {
                    receiver.LossRate = 100;
                }
            };
            try
            {
                await WaitForPeerAsync(
                    sender,
                    receiver.DeviceId,
                    PeerAvailability.Available,
                    timeout.Token
                );
                await WaitForPeerAsync(
                    receiver,
                    sender.DeviceId,
                    PeerAvailability.Available,
                    timeout.Token
                );
                var sending = sender.OfferFileAsync(receiver.DeviceId, source, timeout.Token);
                var offer = await offered.Task.WaitAsync(timeout.Token);
                await receiver.AnswerFileOfferAsync(
                    new TransferKey(sender.DeviceId, offer.FileId),
                    true,
                    destination,
                    timeout.Token
                );
                Assert.Equal(TransferState.Paused, (await sending).State);
                await WaitForTransferStateAsync(
                    receiver,
                    sender.DeviceId,
                    offer.FileId,
                    TransferState.Paused,
                    timeout.Token
                );
                uint nextIndex = receiver
                    .Transfers.GetTransfer(new TransferKey(sender.DeviceId, offer.FileId))
                    .NextIndex;
                Assert.InRange(nextIndex, 4u, offer.ChunkCount - 1);
                Assert.Single(Directory.GetFiles(destination, "*.part"));

                receiver.LossRate = 0;
                await WaitForTransferStateAsync(
                    sender,
                    receiver.DeviceId,
                    offer.FileId,
                    TransferState.Completed,
                    timeout.Token
                );
                await WaitForTransferStateAsync(
                    receiver,
                    sender.DeviceId,
                    offer.FileId,
                    TransferState.Completed,
                    timeout.Token
                );
                var saved = receiver.Transfers.GetTransfer(
                    new TransferKey(sender.DeviceId, offer.FileId)
                );
                Assert.NotNull(saved.SavedPath);
                Assert.Equal(
                    SHA256.HashData(data),
                    SHA256.HashData(await File.ReadAllBytesAsync(saved.SavedPath))
                );
                Assert.Empty(Directory.GetFiles(destination, "*.part"));
                Assert.Equal(1, Volatile.Read(ref interrupted));
                Assert.True(saved.ResumeCount >= 1);
            }
            finally
            {
                timeout.Cancel();
                await Assert.ThrowsAnyAsync<OperationCanceledException>(() => senderRun);
                await Assert.ThrowsAnyAsync<OperationCanceledException>(() => receiverRun);
            }
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    private static async Task WaitForTransferStateAsync(
        PeerService service,
        Guid peerId,
        Guid fileId,
        TransferState state,
        CancellationToken ct
    )
    {
        while (service.Transfers.GetTransfer(new TransferKey(peerId, fileId)).State != state)
        {
            await Task.Delay(20, ct);
        }
    }

    [Fact]
    public async Task SendTextAsync_RefreshesAvailabilityFromAck_WithoutHeartbeatReplies()
    {
        var clock = new TestTimeProvider();
        await using var service = new PeerService("Sender", 0, clock);
        using var remote = new UdpTransport();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var remoteId = Guid.NewGuid();
        var running = service.RunAsync(timeout.Token);
        try
        {
            await remote.SendAsync(
                new Packet(
                    MessageType.PeerInfo,
                    remote.NextSeq(),
                    Payloads.EncodePeerInfo(remoteId, "Remote")
                ),
                new IPEndPoint(IPAddress.Loopback, service.LocalPort)
            );
            await WaitForPeerAsync(service, remoteId, PeerAvailability.Available, timeout.Token);
            clock.AdvanceElapsed(ProtocolConstants.PeerTimeout);
            var sending = service.SendTextAsync(remoteId, "Activity", timeout.Token);
            while (true)
            {
                var message = await remote.ReceiveAsync(timeout.Token);
                if (message.Packet.Type == MessageType.Clipboard)
                {
                    await remote.SendAckAsync(message.Packet.Seq, message.From, remoteId);
                    break;
                }
            }
            await sending;
            await Task.Delay(1300, timeout.Token);
            Assert.Equal(PeerAvailability.Available, GetPeer(service, remoteId).Availability);
        }
        finally
        {
            timeout.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => running);
        }
    }

    [Fact]
    public async Task CancelRun_CancelsIndependentPendingSends()
    {
        await using var service = new PeerService("Sender", 0);
        using var remote = new UdpTransport();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        using var runningCts = new CancellationTokenSource();
        var remoteId = Guid.NewGuid();
        var running = service.RunAsync(runningCts.Token);
        try
        {
            await remote.SendAsync(
                new Packet(
                    MessageType.PeerInfo,
                    remote.NextSeq(),
                    Payloads.EncodePeerInfo(remoteId, "Remote")
                ),
                new IPEndPoint(IPAddress.Loopback, service.LocalPort)
            );
            await WaitForPeerAsync(service, remoteId, PeerAvailability.Available, timeout.Token);
            var sending = service.SendTextAsync(remoteId, "Pending", CancellationToken.None);
            while ((await remote.ReceiveAsync(timeout.Token)).Packet.Type != MessageType.Clipboard)
            { }
            runningCts.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => running);
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
                sending.WaitAsync(timeout.Token)
            );
        }
        finally
        {
            runningCts.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => running);
        }
    }

    [Fact]
    public async Task SetExchangeEnabled_PausesPendingFile_WithoutResettingBudgetOnRepeatedToggles()
    {
        string path = Path.GetTempFileName();
        try
        {
            var clock = new TestTimeProvider();
            await using var service = new PeerService("Sender", 0, clock);
            using var remote = new UdpTransport();
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var remoteId = Guid.NewGuid();
            var running = service.RunAsync(timeout.Token);
            try
            {
                await remote.SendAsync(
                    new Packet(
                        MessageType.PeerInfo,
                        remote.NextSeq(),
                        Payloads.EncodePeerInfo(remoteId, "Remote")
                    ),
                    new IPEndPoint(IPAddress.Loopback, service.LocalPort)
                );
                await WaitForPeerAsync(
                    service,
                    remoteId,
                    PeerAvailability.Available,
                    timeout.Token
                );
                var sending = service.OfferFileAsync(remoteId, path, timeout.Token);
                var offerPacket = await ReceiveMessageAsync(
                    remote,
                    remoteId,
                    MessageType.FileOffer,
                    timeout.Token
                );
                var offer = Payloads.DecodeFileOffer(offerPacket.Packet.Payload)!.Value;
                await remote.SendAckAsync(offerPacket.Packet.Seq, offerPacket.From, remoteId);

                service.SetExchangeEnabled(false);
                Assert.Equal(TransferState.Paused, (await sending.WaitAsync(timeout.Token)).State);
                clock.AdvanceElapsed(TimeSpan.FromSeconds(30));
                service.SetExchangeEnabled(true);
                service.SetExchangeEnabled(false);
                service.SetExchangeEnabled(false);
                var paused = service.Transfers.GetTransfer(new TransferKey(remoteId, offer.FileId));
                Assert.Equal(TransferState.Paused, paused.State);
                Assert.Equal(
                    ProtocolConstants.PauseTimeout - TimeSpan.FromSeconds(30),
                    paused.PauseRemaining
                );
                Assert.Equal(1, paused.PauseCount);
                Assert.Equal(
                    TransferState.Cancelled,
                    service.CancelPausedTransfer(new TransferKey(remoteId, offer.FileId)).State
                );
            }
            finally
            {
                timeout.Cancel();
                await Assert.ThrowsAnyAsync<OperationCanceledException>(() => running);
            }
        }
        finally
        {
            File.Delete(path);
        }
    }

    private static Peer GetPeer(PeerService service, Guid deviceId) =>
        Assert.Single(service.GetPeers(), peer => peer.DeviceId == deviceId);

    private sealed class TestTimeProvider : TimeProvider
    {
        private long _utcTicks;
        private long _elapsedTicks;
        public override long TimestampFrequency => TimeSpan.TicksPerSecond;

        public override DateTimeOffset GetUtcNow() =>
            DateTimeOffset.UnixEpoch.AddTicks(Interlocked.Read(ref _utcTicks));

        public override long GetTimestamp() => Interlocked.Read(ref _elapsedTicks);

        public void MoveUtc(TimeSpan change) => Interlocked.Add(ref _utcTicks, change.Ticks);

        public void AdvanceElapsed(TimeSpan elapsed) =>
            Interlocked.Add(ref _elapsedTicks, elapsed.Ticks);
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
        await remote.SendAsync(
            new Packet(
                MessageType.PeerInfo,
                remote.NextSeq(),
                Payloads.EncodePeerInfo(remoteId, "Receiver")
            ),
            endpoint
        );
        await WaitForPeerAsync(service, remoteId, PeerAvailability.Available, ct);
        var sending = service.OfferFileAsync(remoteId, path, ct);
        var received = await ReceiveMessageAsync(remote, remoteId, MessageType.FileOffer, ct);
        var offer = Payloads.DecodeFileOffer(received.Packet.Payload);
        Assert.NotNull(offer);
        await remote.SendAckAsync(received.Packet.Seq, received.From, remoteId);
        await remote.SendReliableAsync(
            new Packet(
                MessageType.FileAnswer,
                remote.NextSeq(),
                Payloads.EncodeFileAnswer(remoteId, offer.Value.FileId, true, 0)
            ),
            received.From,
            service.DeviceId,
            ct
        );
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
            {
                return received;
            }

            if (received.Packet.Type == MessageType.Heartbeat)
            {
                await remote.SendAckAsync(received.Packet.Seq, received.From, remoteId);
            }
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
        {
            await Task.Delay(20, ct);
        }
    }
}
