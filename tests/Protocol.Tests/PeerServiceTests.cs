using System.Net;
using System.Security.Cryptography;
using Protocol.Extensions;
using Protocol.Messages;
using Protocol.Peers;
using Protocol.Transfers.Models;
using Protocol.Transport;

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

    [Theory]
    [InlineData(0, true, false)]
    [InlineData(2500, true, false)]
    [InlineData(21 * 1024 * 1024, true, false)]
    [InlineData(2500, false, false)]
    [InlineData(2500, true, true)]
    public async Task OfferFileAsync_SavesFileOrReportsRejectionOrSourceChange(
        int size,
        bool accepted,
        bool changeSource
    )
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
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(120));
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

                if (changeSource)
                {
                    await File.WriteAllBytesAsync(path, [9], cts.Token);
                }

                await receiver.AnswerFileOfferAsync(
                    new TransferKey(sender.DeviceId, offer.FileId),
                    accepted,
                    directory,
                    cts.Token
                );

                if (changeSource)
                {
                    await Assert.ThrowsAnyAsync<IOException>(() => sending.WaitAsync(cts.Token));
                    var failed = sender.Transfers.GetTransfer(
                        new TransferKey(receiver.DeviceId, offer.FileId)
                    );
                    Assert.Equal(TransferState.Failed, failed.State);
                    Assert.Equal(TransferError.SourceFileChanged, failed.ErrorCode);
                    return;
                }

                var transfer = await sending.WaitAsync(cts.Token);

                if (!accepted)
                {
                    Assert.Equal(TransferState.Rejected, transfer.State);
                    Assert.Equal(
                        TransferState.Rejected,
                        receiver.Transfers.GetTransfer(offer.Key).State
                    );
                    Assert.Equal(new[] { path }, Directory.GetFiles(directory));
                    return;
                }

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
                    && transfer.NextIndex > 0
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
                Assert.InRange(nextIndex, 1u, offer.ChunkCount - 1);
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

    [Fact]
    public async Task DisableExchange_PausesPendingFileInsteadOfCancellingIt()
    {
        string source = Path.GetTempFileName();

        try
        {
            await using var sender = new PeerService("Sender", 0);
            await using var receiver = new PeerService("Receiver", 0);
            sender.AddManualEndpoint(new IPEndPoint(IPAddress.Loopback, receiver.LocalPort));
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            var offered = new TaskCompletionSource<FileTransfer>(
                TaskCreationOptions.RunContinuationsAsynchronously
            );
            receiver.Transfers.FileOffered += transfer => offered.TrySetResult(transfer);

            var senderRun = sender.RunAsync(timeout.Token);
            var receiverRun = receiver.RunAsync(timeout.Token);

            try
            {
                await WaitForPeerAsync(
                    sender,
                    receiver.DeviceId,
                    PeerAvailability.Available,
                    timeout.Token
                );

                var sending = sender.OfferFileAsync(receiver.DeviceId, source, timeout.Token);
                await offered.Task.WaitAsync(timeout.Token);
                await Task.Delay(20, timeout.Token);

                sender.SetExchangeEnabled(false);

                var transfer = await sending.WaitAsync(timeout.Token);

                Assert.Equal(TransferState.Paused, transfer.State);
                Assert.True(transfer.PauseRemaining > TimeSpan.Zero);
                Assert.Equal(
                    TransferState.Cancelled,
                    sender.CancelPausedTransfer(transfer.Key).State
                );
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
            File.Delete(source);
        }
    }

    [Fact]
    public async Task RestartSession_PreservesDeviceIdentityAndDeliversNewText()
    {
        await using var receiver = new PeerService("Receiver", 0);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        var receiving = receiver.RunAsync(timeout.Token);
        var deviceId = Guid.NewGuid();
        uint nextSeq = 0;

        try
        {
            foreach (string text in new[] { "Before restart", "After restart" })
            {
                var received = new TaskCompletionSource<string>(
                    TaskCreationOptions.RunContinuationsAsynchronously
                );
                void OnTextReceived(Guid senderId, string value)
                {
                    if (senderId == deviceId)
                    {
                        received.TrySetResult(value);
                    }
                }

                receiver.TextReceived += OnTextReceived;

                var sender = new PeerService(
                    "Restarting peer",
                    0,
                    deviceId: deviceId,
                    initialSeq: nextSeq
                );

                try
                {
                    await using (sender)
                    {
                        sender.AddManualEndpoint(
                            new IPEndPoint(IPAddress.Loopback, receiver.LocalPort)
                        );
                        using var stopping = CancellationTokenSource.CreateLinkedTokenSource(
                            timeout.Token
                        );

                        var sending = sender.RunAsync(stopping.Token);

                        try
                        {
                            await WaitForPeerAsync(
                                sender,
                                receiver.DeviceId,
                                PeerAvailability.Available,
                                timeout.Token
                            );
                            await sender.SendTextAsync(receiver.DeviceId, text, timeout.Token);
                            Assert.Equal(text, await received.Task.WaitAsync(timeout.Token));
                            Assert.Equal(
                                deviceId,
                                Assert
                                    .Single(
                                        receiver.GetPeers(),
                                        peer => peer.Name == "Restarting peer"
                                    )
                                    .DeviceId
                            );
                        }
                        finally
                        {
                            stopping.Cancel();
                            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => sending);
                        }
                    }

                    nextSeq = sender.NextSequence;
                }
                finally
                {
                    receiver.TextReceived -= OnTextReceived;
                }
            }
        }
        finally
        {
            timeout.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => receiving);
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

    private static Peer GetPeer(PeerService service, Guid deviceId) =>
        Assert.Single(service.GetPeers(), peer => peer.DeviceId == deviceId);

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
