using System.Collections.Concurrent;
using System.Net;
using System.Security.Cryptography;

namespace Protocol.Tests;

public class FileExchangeTests
{
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task OfferFileAsync_RunsFileProtocol_ThroughTransportContract(bool accepted)
    {
        string path = Path.Combine(Path.GetTempPath(), $"protocol-exchange-{Guid.NewGuid():N}.bin");
        try
        {
            byte[] data = Enumerable.Range(0, 2500).Select(index => (byte)index).ToArray();
            await File.WriteAllBytesAsync(path, data);
            var transport = new TestTransport();
            var localId = Guid.NewGuid();
            var remoteId = Guid.NewGuid();
            var endpoint = new IPEndPoint(IPAddress.Loopback, 5001);
            var sentEndpoints = new List<IPEndPoint>();
            var acknowledgedEndpoints = new List<IPEndPoint>();
            await using var exchange = new FileExchange(
                transport,
                localId,
                _ => endpoint,
                CancellationToken.None,
                acknowledged: (peerId, destination) =>
                {
                    Assert.Equal(remoteId, peerId);
                    acknowledgedEndpoints.Add(destination);
                }
            );
            var sentTypes = new List<MessageType>();
            Guid fileId = default;
            transport.OnSend = async (packet, destination, deviceId, ct) =>
            {
                Assert.Equal(remoteId, deviceId);
                Assert.Equal(endpoint, destination);
                sentEndpoints.Add(destination);
                sentTypes.Add(packet.Type);
                switch (packet.Type)
                {
                    case MessageType.FileOffer:
                        var offer = Payloads.DecodeFileOffer(packet.Payload)!.Value;
                        fileId = offer.FileId;
                        Assert.Equal(localId, offer.DeviceId);
                        Assert.Equal(SHA256.HashData(data), offer.Sha256);
                        await exchange.HandlePacketAsync(
                            new Packet(
                                MessageType.FileAnswer,
                                100,
                                Payloads.EncodeFileAnswer(remoteId, fileId, accepted, 0)
                            ),
                            endpoint,
                            remoteId,
                            ct
                        );
                        break;
                    case MessageType.Chunk:
                        var chunk = Payloads.DecodeChunk(packet.Payload)!.Value;
                        Assert.Equal(
                            chunk.Index,
                            exchange
                                .Transfers.GetTransfer(new TransferKey(remoteId, fileId))
                                .NextIndex
                        );
                        Assert.Equal(
                            data.Skip((int)chunk.Index * ProtocolConstants.ChunkSize)
                                .Take(ProtocolConstants.ChunkSize),
                            chunk.Data
                        );
                        // Each subsequent packet must use the current endpoint.
                        endpoint = new IPEndPoint(IPAddress.Loopback, endpoint.Port + 1);
                        break;
                    case MessageType.FileDone:
                        Assert.Equal(
                            3u,
                            exchange
                                .Transfers.GetTransfer(new TransferKey(remoteId, fileId))
                                .NextIndex
                        );
                        await exchange.HandlePacketAsync(
                            new Packet(
                                MessageType.FileResult,
                                101,
                                Payloads.EncodeFileResult(remoteId, fileId, true)
                            ),
                            endpoint,
                            remoteId,
                            ct
                        );
                        break;
                    default:
                        Assert.Fail($"Unexpected outgoing message: {packet.Type}");
                        break;
                }
            };

            var transfer = await exchange.OfferFileAsync(remoteId, path, CancellationToken.None);

            Assert.Equal(
                accepted ? TransferState.Completed : TransferState.Rejected,
                transfer.State
            );
            Assert.Equal(accepted ? 3u : 0u, transfer.NextIndex);
            Assert.Equal(
                accepted
                    ? new[]
                    {
                        MessageType.FileOffer,
                        MessageType.Chunk,
                        MessageType.Chunk,
                        MessageType.Chunk,
                        MessageType.FileDone,
                    }
                    : new[] { MessageType.FileOffer },
                sentTypes
            );
            Assert.Equal(accepted ? 2 : 1, transport.Acks.Count);
            Assert.Equal(sentEndpoints, acknowledgedEndpoints);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task FileResultDelivery_ObservesErrors_AndCancelsOnDispose(bool unexpectedError)
    {
        string directory = Path.Combine(
            Path.GetTempPath(),
            $"protocol-exchange-{Guid.NewGuid():N}"
        );
        try
        {
            var transport = new TestTransport();
            var localId = Guid.NewGuid();
            var remoteId = Guid.NewGuid();
            var fileId = Guid.NewGuid();
            var endpoint = new IPEndPoint(IPAddress.Loopback, 5001);
            await using var exchange = new FileExchange(
                transport,
                localId,
                _ => endpoint,
                CancellationToken.None
            );
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var started = new TaskCompletionSource(
                TaskCreationOptions.RunContinuationsAsynchronously
            );
            var finished = new TaskCompletionSource(
                TaskCreationOptions.RunContinuationsAsynchronously
            );
            var failure = new InvalidOperationException("Unexpected transport failure");
            transport.OnSend = async (packet, _, _, ct) =>
            {
                if (packet.Type != MessageType.FileResult)
                {
                    return;
                }

                Assert.True(Payloads.DecodeFileResult(packet.Payload)!.Value.Ok);
                started.TrySetResult();
                try
                {
                    if (unexpectedError)
                    {
                        throw failure;
                    }

                    await Task.Delay(Timeout.Infinite, ct);
                }
                finally
                {
                    finished.TrySetResult();
                }
            };
            await exchange.HandlePacketAsync(
                new Packet(
                    MessageType.FileOffer,
                    1,
                    Payloads.EncodeFileOffer(remoteId, fileId, 0, SHA256.HashData([]), "empty.bin")
                ),
                endpoint,
                remoteId,
                cts.Token
            );
            await exchange.AnswerFileOfferAsync(
                new TransferKey(remoteId, fileId),
                true,
                directory,
                cts.Token
            );
            await exchange.HandlePacketAsync(
                new Packet(MessageType.FileDone, 2, Payloads.EncodeFileDone(remoteId, fileId)),
                endpoint,
                remoteId,
                cts.Token
            );
            await started.Task.WaitAsync(cts.Token);

            if (unexpectedError)
            {
                var error = await Assert.ThrowsAsync<InvalidOperationException>(() =>
                    exchange.WaitForPendingOperationsAsync(cts.Token)
                );
                Assert.Same(failure, error);
            }
            else
            {
                await exchange.DisposeAsync().AsTask().WaitAsync(cts.Token);
                Assert.True(finished.Task.IsCompletedSuccessfully);
            }
            Assert.True(File.Exists(Path.Combine(directory, "empty.bin")));
            Assert.Empty(Directory.GetFiles(directory, "*.part"));
        }
        finally
        {
            if (Directory.Exists(directory))
            {
                Directory.Delete(directory, recursive: true);
            }
        }
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task DisposeAsync_WaitsForOutgoingOperationCleanup(bool sendingFile)
    {
        string directory = Path.Combine(
            Path.GetTempPath(),
            $"protocol-shutdown-{Guid.NewGuid():N}"
        );
        Directory.CreateDirectory(directory);
        string path = Path.Combine(directory, "source.bin");
        var releaseCleanup = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        try
        {
            await File.WriteAllBytesAsync(path, new byte[1024]);
            var remoteId = Guid.NewGuid();
            var fileId = Guid.NewGuid();
            var endpoint = new IPEndPoint(IPAddress.Loopback, 5001);
            var transport = new TestTransport();
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            using var cleanupOnTimeout = timeout.Token.Register(() =>
                releaseCleanup.TrySetResult()
            );
            await using var exchange = new FileExchange(
                transport,
                Guid.NewGuid(),
                _ => endpoint,
                CancellationToken.None
            );
            var sending = new TaskCompletionSource(
                TaskCreationOptions.RunContinuationsAsynchronously
            );
            var cancelling = new TaskCompletionSource(
                TaskCreationOptions.RunContinuationsAsynchronously
            );
            transport.OnSend = async (packet, _, _, ct) =>
            {
                if (packet.Type == MessageType.FileOffer)
                {
                    var offer = Payloads.DecodeFileOffer(packet.Payload)!.Value;
                    exchange.Transfers.HandleFileAnswer(
                        Payloads.EncodeFileAnswer(remoteId, offer.FileId, true, 0)
                    );
                }
                if (packet.Type != (sendingFile ? MessageType.Chunk : MessageType.FileAnswer))
                {
                    return;
                }

                sending.TrySetResult();
                try
                {
                    await Task.Delay(Timeout.Infinite, ct);
                }
                finally
                {
                    cancelling.TrySetResult();
                    await releaseCleanup.Task;
                }
            };

            if (!sendingFile)
            {
                exchange.Transfers.HandleFileOffer(
                    Payloads.EncodeFileOffer(remoteId, fileId, 1024, new byte[32], "incoming.bin")
                );
            }

            var operation = sendingFile
                ? exchange.OfferFileAsync(remoteId, path, CancellationToken.None)
                : exchange.AnswerFileOfferAsync(
                    new TransferKey(remoteId, fileId),
                    true,
                    directory,
                    CancellationToken.None
                );
            await sending.Task.WaitAsync(timeout.Token);

            var disposing = exchange.DisposeAsync().AsTask();
            try
            {
                await cancelling.Task.WaitAsync(timeout.Token);
                Assert.False(disposing.IsCompleted);
                Assert.Same(disposing, exchange.DisposeAsync().AsTask());
            }
            finally
            {
                releaseCleanup.TrySetResult();
            }

            await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
                operation.WaitAsync(timeout.Token)
            );
            await disposing.WaitAsync(timeout.Token);
            using var reopened = new FileStream(
                path,
                FileMode.Open,
                FileAccess.ReadWrite,
                FileShare.None
            );
            Assert.Empty(Directory.GetFiles(directory, "*.part"));
        }
        finally
        {
            releaseCleanup.TrySetResult();
            Directory.Delete(directory, recursive: true);
        }
    }

    [Fact]
    public async Task OfferFileAsync_ReturnsFailureToCaller_WithoutRepeatingItOnDispose()
    {
        string path = Path.Combine(Path.GetTempPath(), $"protocol-failure-{Guid.NewGuid():N}.bin");
        try
        {
            await File.WriteAllBytesAsync(path, [1]);
            var failure = new IOException("Could not send offer");
            var transport = new TestTransport
            {
                OnSend = (_, _, _, _) => Task.FromException(failure),
            };
            await using var exchange = new FileExchange(
                transport,
                Guid.NewGuid(),
                _ => new IPEndPoint(IPAddress.Loopback, 5001),
                CancellationToken.None
            );

            var error = await Assert.ThrowsAsync<IOException>(() =>
                exchange.OfferFileAsync(Guid.NewGuid(), path, CancellationToken.None)
            );

            Assert.Same(failure, error);
            await exchange.DisposeAsync();
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task PausedOutgoing_ResumesFromReceiverIndex_UsingCurrentEndpoint()
    {
        string path = Path.Combine(Path.GetTempPath(), $"protocol-resume-{Guid.NewGuid():N}.bin");
        try
        {
            byte[] data = Enumerable.Range(0, 2500).Select(index => (byte)index).ToArray();
            await File.WriteAllBytesAsync(path, data);
            var time = new ManualTimeProvider();
            var transport = new TestTransport();
            var remoteId = Guid.NewGuid();
            var endpoint = new IPEndPoint(IPAddress.Loopback, 5001);
            await using var exchange = new FileExchange(
                transport,
                Guid.NewGuid(),
                _ => endpoint,
                CancellationToken.None,
                timeProvider: time
            );
            var offers = new List<(uint Seq, Guid FileId)>();
            var chunks = new List<uint>();
            uint replySeq = 100;
            transport.OnSend = async (packet, destination, _, ct) =>
            {
                Assert.Equal(endpoint, destination);
                if (packet.Type == MessageType.FileOffer)
                {
                    var offer = Payloads.DecodeFileOffer(packet.Payload)!.Value;
                    offers.Add((packet.Seq, offer.FileId));
                    await exchange.HandlePacketAsync(
                        new Packet(
                            MessageType.FileAnswer,
                            replySeq++,
                            Payloads.EncodeFileAnswer(
                                remoteId,
                                offer.FileId,
                                true,
                                offers.Count == 1 ? 0u : 2u
                            )
                        ),
                        endpoint,
                        remoteId,
                        ct
                    );
                }
                else if (packet.Type == MessageType.Chunk)
                {
                    uint index = Payloads.DecodeChunk(packet.Payload)!.Value.Index;
                    chunks.Add(index);
                    if (offers.Count == 1 && index == 1)
                    {
                        throw new TimeoutException(
                            "Chunk ACK was lost after the receiver stored it"
                        );
                    }
                }
                else if (packet.Type == MessageType.FileDone)
                {
                    var done = Payloads.DecodeFileDone(packet.Payload)!.Value;
                    await exchange.HandlePacketAsync(
                        new Packet(
                            MessageType.FileResult,
                            replySeq++,
                            Payloads.EncodeFileResult(remoteId, done.FileId, true)
                        ),
                        endpoint,
                        remoteId,
                        ct
                    );
                }
            };

            var paused = await exchange.OfferFileAsync(remoteId, path, CancellationToken.None);
            Assert.Equal(TransferState.Paused, paused.State);
            Assert.Equal(1u, paused.NextIndex);
            endpoint = new IPEndPoint(IPAddress.Loopback, 5002);
            await exchange.CheckCompletedOperationsAsync(CancellationToken.None);
            time.Advance(ProtocolConstants.ProbeInterval);
            await exchange.CheckCompletedOperationsAsync(CancellationToken.None);
            await exchange
                .WaitForPendingOperationsAsync(CancellationToken.None)
                .WaitAsync(TimeSpan.FromSeconds(5));

            Assert.Equal(
                TransferState.Completed,
                exchange.GetTransfer(new TransferKey(remoteId, paused.FileId)).State
            );
            Assert.Equal(new uint[] { 0, 1, 2 }, chunks);
            Assert.Equal(2, offers.Count);
            Assert.Equal(offers[0].FileId, offers[1].FileId);
            Assert.NotEqual(offers[0].Seq, offers[1].Seq);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task DisabledExchange_PreservesPartialFile_AndExpiresPauseWithoutNetwork()
    {
        using var exchangeCts = new CancellationTokenSource();
        string directory = Path.Combine(
            Path.GetTempPath(),
            $"protocol-disabled-{Guid.NewGuid():N}"
        );
        try
        {
            var time = new ManualTimeProvider();
            var remoteId = Guid.NewGuid();
            var fileId = Guid.NewGuid();
            var endpoint = new IPEndPoint(IPAddress.Loopback, 5001);
            await using var exchange = new FileExchange(
                new TestTransport(),
                Guid.NewGuid(),
                _ => endpoint,
                CancellationToken.None,
                timeProvider: time,
                exchangeToken: exchangeCts.Token
            );
            await exchange.HandlePacketAsync(
                new Packet(
                    MessageType.FileOffer,
                    1,
                    Payloads.EncodeFileOffer(remoteId, fileId, 2048, new byte[32], "partial.bin")
                ),
                endpoint,
                remoteId,
                CancellationToken.None
            );
            await exchange.AnswerFileOfferAsync(
                new TransferKey(remoteId, fileId),
                true,
                directory,
                CancellationToken.None
            );
            await exchange.HandlePacketAsync(
                new Packet(
                    MessageType.Chunk,
                    2,
                    Payloads.EncodeChunk(remoteId, fileId, 0, new byte[1024])
                ),
                endpoint,
                remoteId,
                CancellationToken.None
            );

            exchangeCts.Cancel();
            exchange.SetExchangeToken(exchangeCts.Token);

            Assert.Equal(
                TransferState.Paused,
                exchange.GetTransfer(new TransferKey(remoteId, fileId)).State
            );
            Assert.Equal(1u, exchange.GetTransfer(new TransferKey(remoteId, fileId)).NextIndex);
            Assert.Single(Directory.GetFiles(directory, "*.part"));
            time.Advance(ProtocolConstants.PauseTimeout);
            await exchange.CheckCompletedOperationsAsync(CancellationToken.None);

            Assert.Equal(
                TransferState.Failed,
                exchange.GetTransfer(new TransferKey(remoteId, fileId)).State
            );
            Assert.Empty(Directory.GetFiles(directory, "*.part"));
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
    public async Task DisableDuringOutgoingChunk_PausesWithoutCancellingTransfer_AndCanResume()
    {
        using var exchangeCts = new CancellationTokenSource();
        string path = Path.Combine(Path.GetTempPath(), $"protocol-toggle-{Guid.NewGuid():N}.bin");
        try
        {
            await File.WriteAllBytesAsync(path, new byte[1024]);
            var time = new ManualTimeProvider();
            var transport = new TestTransport();
            var remoteId = Guid.NewGuid();
            var endpoint = new IPEndPoint(IPAddress.Loopback, 5001);
            var sending = new TaskCompletionSource(
                TaskCreationOptions.RunContinuationsAsynchronously
            );
            await using var exchange = new FileExchange(
                transport,
                Guid.NewGuid(),
                _ => endpoint,
                CancellationToken.None,
                timeProvider: time,
                exchangeToken: exchangeCts.Token
            );
            int offers = 0;
            uint replySeq = 100;
            transport.OnSend = async (packet, _, _, ct) =>
            {
                if (packet.Type == MessageType.FileOffer)
                {
                    offers++;
                    var offer = Payloads.DecodeFileOffer(packet.Payload)!.Value;
                    await exchange.HandlePacketAsync(
                        new Packet(
                            MessageType.FileAnswer,
                            replySeq++,
                            Payloads.EncodeFileAnswer(remoteId, offer.FileId, true, 0)
                        ),
                        endpoint,
                        remoteId,
                        ct
                    );
                }
                else if (packet.Type == MessageType.Chunk && offers == 1)
                {
                    sending.TrySetResult();
                    await Task.Delay(Timeout.Infinite, ct);
                }
                else if (packet.Type == MessageType.FileDone)
                {
                    var done = Payloads.DecodeFileDone(packet.Payload)!.Value;
                    await exchange.HandlePacketAsync(
                        new Packet(
                            MessageType.FileResult,
                            replySeq++,
                            Payloads.EncodeFileResult(remoteId, done.FileId, true)
                        ),
                        endpoint,
                        remoteId,
                        ct
                    );
                }
            };
            var operation = exchange.OfferFileAsync(remoteId, path, CancellationToken.None);
            await sending.Task.WaitAsync(TimeSpan.FromSeconds(5));

            exchangeCts.Cancel();
            exchange.SetExchangeToken(exchangeCts.Token);
            var paused = await operation.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.Equal(TransferState.Paused, paused.State);

            using var resumedExchangeCts = new CancellationTokenSource();
            exchange.SetExchangeToken(resumedExchangeCts.Token);
            await exchange.CheckCompletedOperationsAsync(CancellationToken.None);
            time.Advance(ProtocolConstants.ProbeInterval);
            await exchange.CheckCompletedOperationsAsync(CancellationToken.None);
            await exchange
                .WaitForPendingOperationsAsync(CancellationToken.None)
                .WaitAsync(TimeSpan.FromSeconds(5));

            Assert.Equal(
                TransferState.Completed,
                exchange.GetTransfer(new TransferKey(remoteId, paused.FileId)).State
            );
            Assert.Equal(2, offers);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task CompletedIncoming_RepeatsAnswerAndResult_WithoutSavingTwice()
    {
        string directory = Path.Combine(Path.GetTempPath(), $"protocol-repeat-{Guid.NewGuid():N}");
        try
        {
            var transport = new TestTransport();
            var remoteId = Guid.NewGuid();
            var fileId = Guid.NewGuid();
            var endpoint = new IPEndPoint(IPAddress.Loopback, 5001);
            await using var exchange = new FileExchange(
                transport,
                Guid.NewGuid(),
                _ => endpoint,
                CancellationToken.None
            );
            var sent = new ConcurrentQueue<Packet>();
            int offered = 0;
            exchange.FileOffered += _ => Interlocked.Increment(ref offered);
            transport.OnSend = (packet, _, _, _) =>
            {
                sent.Enqueue(packet);
                return Task.CompletedTask;
            };
            byte[] payload = Payloads.EncodeFileOffer(
                remoteId,
                fileId,
                0,
                SHA256.HashData([]),
                "empty.bin"
            );
            await exchange.HandlePacketAsync(
                new Packet(MessageType.FileOffer, 1, payload),
                endpoint,
                remoteId,
                CancellationToken.None
            );
            await exchange.AnswerFileOfferAsync(
                new TransferKey(remoteId, fileId),
                true,
                directory,
                CancellationToken.None
            );
            await exchange.HandlePacketAsync(
                new Packet(MessageType.FileDone, 2, Payloads.EncodeFileDone(remoteId, fileId)),
                endpoint,
                remoteId,
                CancellationToken.None
            );
            await exchange
                .WaitForPendingOperationsAsync(CancellationToken.None)
                .WaitAsync(TimeSpan.FromSeconds(5));
            await exchange.HandlePacketAsync(
                new Packet(MessageType.FileOffer, 3, payload),
                endpoint,
                remoteId,
                CancellationToken.None
            );
            await exchange
                .WaitForPendingOperationsAsync(CancellationToken.None)
                .WaitAsync(TimeSpan.FromSeconds(5));
            await exchange.HandlePacketAsync(
                new Packet(MessageType.FileDone, 4, Payloads.EncodeFileDone(remoteId, fileId)),
                endpoint,
                remoteId,
                CancellationToken.None
            );
            await exchange
                .WaitForPendingOperationsAsync(CancellationToken.None)
                .WaitAsync(TimeSpan.FromSeconds(5));
            await exchange.DisposeAsync();

            Assert.Equal(1, offered);
            Assert.Single(Directory.GetFiles(directory));
            Assert.Equal(2, sent.Count(packet => packet.Type == MessageType.FileAnswer));
            Assert.Equal(2, sent.Count(packet => packet.Type == MessageType.FileResult));
            Assert.All(
                sent.Where(packet => packet.Type == MessageType.FileResult),
                packet => Assert.True(Payloads.DecodeFileResult(packet.Payload)!.Value.Ok)
            );
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
    public async Task FileAnswerAckTimeout_DoesNotUndoFileAlreadySavedByReceiver()
    {
        string directory = Path.Combine(Path.GetTempPath(), $"protocol-answer-{Guid.NewGuid():N}");
        try
        {
            byte[] data = [1, 2, 3];
            var transport = new TestTransport();
            var remoteId = Guid.NewGuid();
            var fileId = Guid.NewGuid();
            var endpoint = new IPEndPoint(IPAddress.Loopback, 5001);
            await using var exchange = new FileExchange(
                transport,
                Guid.NewGuid(),
                _ => endpoint,
                CancellationToken.None
            );
            transport.OnSend = async (packet, _, _, ct) =>
            {
                if (packet.Type != MessageType.FileAnswer)
                {
                    return;
                }
                await exchange.HandlePacketAsync(
                    new Packet(
                        MessageType.Chunk,
                        2,
                        Payloads.EncodeChunk(remoteId, fileId, 0, data)
                    ),
                    endpoint,
                    remoteId,
                    ct
                );
                await exchange.HandlePacketAsync(
                    new Packet(MessageType.FileDone, 3, Payloads.EncodeFileDone(remoteId, fileId)),
                    endpoint,
                    remoteId,
                    ct
                );
                await exchange.WaitForPendingOperationsAsync(ct).WaitAsync(TimeSpan.FromSeconds(5));
                throw new TimeoutException("Decision ACK was lost after file completion");
            };
            await exchange.HandlePacketAsync(
                new Packet(
                    MessageType.FileOffer,
                    1,
                    Payloads.EncodeFileOffer(
                        remoteId,
                        fileId,
                        (ulong)data.Length,
                        SHA256.HashData(data),
                        "saved.bin"
                    )
                ),
                endpoint,
                remoteId,
                CancellationToken.None
            );

            var result = await exchange.AnswerFileOfferAsync(
                new TransferKey(remoteId, fileId),
                true,
                directory,
                CancellationToken.None
            );

            Assert.Equal(TransferState.Completed, result.State);
            Assert.Equal(data, await File.ReadAllBytesAsync(Path.Combine(directory, "saved.bin")));
            Assert.Empty(Directory.GetFiles(directory, "*.part"));
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
    public async Task FailedResumeProbes_DoNotExtendOriginalPauseDeadline()
    {
        string path = Path.Combine(Path.GetTempPath(), $"protocol-probe-{Guid.NewGuid():N}.bin");
        try
        {
            await File.WriteAllBytesAsync(path, [1]);
            var time = new ManualTimeProvider();
            var transport = new TestTransport();
            var remoteId = Guid.NewGuid();
            var endpoint = new IPEndPoint(IPAddress.Loopback, 5001);
            await using var exchange = new FileExchange(
                transport,
                Guid.NewGuid(),
                _ => endpoint,
                CancellationToken.None,
                timeProvider: time
            );
            int offers = 0;
            transport.OnSend = async (packet, _, _, ct) =>
            {
                if (packet.Type == MessageType.FileOffer && ++offers == 1)
                {
                    var offer = Payloads.DecodeFileOffer(packet.Payload)!.Value;
                    await exchange.HandlePacketAsync(
                        new Packet(
                            MessageType.FileAnswer,
                            100,
                            Payloads.EncodeFileAnswer(remoteId, offer.FileId, true, 0)
                        ),
                        endpoint,
                        remoteId,
                        ct
                    );
                    return;
                }
                throw new TimeoutException("Peer is unavailable");
            };
            var paused = await exchange.OfferFileAsync(remoteId, path, CancellationToken.None);
            await exchange.CheckCompletedOperationsAsync(CancellationToken.None);
            time.Advance(TimeSpan.FromSeconds(2));
            await exchange.CheckCompletedOperationsAsync(CancellationToken.None);
            await exchange
                .WaitForPendingOperationsAsync(CancellationToken.None)
                .WaitAsync(TimeSpan.FromSeconds(5));
            Assert.Equal(
                TimeSpan.FromSeconds(118),
                exchange.GetTransfer(new TransferKey(remoteId, paused.FileId)).PauseRemaining
            );

            time.Advance(TimeSpan.FromSeconds(117));
            await exchange.CheckCompletedOperationsAsync(CancellationToken.None);
            await exchange
                .WaitForPendingOperationsAsync(CancellationToken.None)
                .WaitAsync(TimeSpan.FromSeconds(5));
            Assert.Equal(
                TimeSpan.FromSeconds(1),
                exchange.GetTransfer(new TransferKey(remoteId, paused.FileId)).PauseRemaining
            );
            time.Advance(TimeSpan.FromSeconds(1));
            await exchange.CheckCompletedOperationsAsync(CancellationToken.None);

            Assert.Equal(
                TransferState.Failed,
                exchange.GetTransfer(new TransferKey(remoteId, paused.FileId)).State
            );
            Assert.Equal(3, offers);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task AutomaticAnswerAckTimeout_DoesNotPauseAfterNewChunkArrives()
    {
        string directory = Path.Combine(
            Path.GetTempPath(),
            $"protocol-auto-answer-{Guid.NewGuid():N}"
        );
        try
        {
            var transport = new TestTransport();
            var remoteId = Guid.NewGuid();
            var fileId = Guid.NewGuid();
            var endpoint = new IPEndPoint(IPAddress.Loopback, 5001);
            await using var exchange = new FileExchange(
                transport,
                Guid.NewGuid(),
                _ => endpoint,
                CancellationToken.None
            );
            byte[] offer = Payloads.EncodeFileOffer(
                remoteId,
                fileId,
                2048,
                new byte[32],
                "partial.bin"
            );
            await exchange.HandlePacketAsync(
                new Packet(MessageType.FileOffer, 1, offer),
                endpoint,
                remoteId,
                CancellationToken.None
            );
            await exchange.AnswerFileOfferAsync(
                new TransferKey(remoteId, fileId),
                true,
                directory,
                CancellationToken.None
            );
            exchange.Transfers.SetState(new TransferKey(remoteId, fileId), TransferState.Paused);
            transport.OnSend = async (packet, _, _, ct) =>
            {
                Assert.Equal(MessageType.FileAnswer, packet.Type);
                await exchange.HandlePacketAsync(
                    new Packet(
                        MessageType.Chunk,
                        3,
                        Payloads.EncodeChunk(remoteId, fileId, 0, new byte[1024])
                    ),
                    endpoint,
                    remoteId,
                    ct
                );
                throw new TimeoutException("Resumed decision ACK was lost after chunk delivery");
            };

            await exchange.HandlePacketAsync(
                new Packet(MessageType.FileOffer, 2, offer),
                endpoint,
                remoteId,
                CancellationToken.None
            );
            await exchange
                .WaitForPendingOperationsAsync(CancellationToken.None)
                .WaitAsync(TimeSpan.FromSeconds(5));

            var transfer = exchange.GetTransfer(new TransferKey(remoteId, fileId));
            Assert.Equal(TransferState.Transferring, transfer.State);
            Assert.Equal(1u, transfer.NextIndex);
            Assert.Null(transfer.PauseRemaining);
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
    public async Task DisableDuringPendingAck_PausesRegisteredOffer_AndIgnoresNewOffers()
    {
        using var exchangeCts = new CancellationTokenSource();
        var transport = new TestTransport();
        var remoteId = Guid.NewGuid();
        var fileId = Guid.NewGuid();
        var endpoint = new IPEndPoint(IPAddress.Loopback, 5001);
        await using var exchange = new FileExchange(
            transport,
            Guid.NewGuid(),
            _ => endpoint,
            CancellationToken.None,
            exchangeToken: exchangeCts.Token
        );
        var enteredAck = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        var releaseAck = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        transport.OnAck = async () =>
        {
            enteredAck.TrySetResult();
            await releaseAck.Task;
        };
        var handling = exchange.HandlePacketAsync(
            new Packet(
                MessageType.FileOffer,
                1,
                Payloads.EncodeFileOffer(remoteId, fileId, 0, SHA256.HashData([]), "empty.bin")
            ),
            endpoint,
            remoteId,
            CancellationToken.None
        );
        try
        {
            await enteredAck.Task.WaitAsync(TimeSpan.FromSeconds(5));
            await Task.Run(() =>
                {
                    exchangeCts.Cancel();
                    exchange.SetExchangeToken(exchangeCts.Token);
                })
                .WaitAsync(TimeSpan.FromSeconds(5));
            Assert.Equal(
                TransferState.Paused,
                exchange.GetTransfer(new TransferKey(remoteId, fileId)).State
            );
            await exchange.HandlePacketAsync(
                new Packet(
                    MessageType.FileOffer,
                    2,
                    Payloads.EncodeFileOffer(
                        remoteId,
                        Guid.NewGuid(),
                        0,
                        SHA256.HashData([]),
                        "ignored.bin"
                    )
                ),
                endpoint,
                remoteId,
                CancellationToken.None
            );
            Assert.Single(exchange.GetTransfers());
        }
        finally
        {
            releaseAck.TrySetResult();
            await handling.WaitAsync(TimeSpan.FromSeconds(5));
        }
    }

    [Fact]
    public async Task ConcurrentOffersAndDisable_LeaveNoActiveIncomingOffers()
    {
        var remoteId = Guid.NewGuid();
        var endpoint = new IPEndPoint(IPAddress.Loopback, 5001);
        await using var exchange = new FileExchange(
            new TestTransport(),
            Guid.NewGuid(),
            _ => endpoint,
            CancellationToken.None
        );
        for (int round = 0; round < 20; round++)
        {
            using var exchangeCts = new CancellationTokenSource();
            exchange.SetExchangeToken(exchangeCts.Token);
            var start = new TaskCompletionSource(
                TaskCreationOptions.RunContinuationsAsynchronously
            );
            var operations = Enumerable
                .Range(0, 40)
                .Select(index =>
                    Task.Run(async () =>
                    {
                        await start.Task;
                        await exchange.HandlePacketAsync(
                            new Packet(
                                MessageType.FileOffer,
                                (uint)(round * 40 + index),
                                Payloads.EncodeFileOffer(
                                    remoteId,
                                    Guid.NewGuid(),
                                    0,
                                    SHA256.HashData([]),
                                    "offer.bin"
                                )
                            ),
                            endpoint,
                            remoteId,
                            CancellationToken.None
                        );
                    })
                )
                .ToArray();
            var disabling = Task.Run(async () =>
            {
                await start.Task;
                exchangeCts.Cancel();
                exchange.SetExchangeToken(exchangeCts.Token);
            });
            start.SetResult();
            await Task.WhenAll(operations.Append(disabling)).WaitAsync(TimeSpan.FromSeconds(5));
            Assert.All(
                exchange.GetTransfers(),
                transfer => Assert.Equal(TransferState.Paused, transfer.State)
            );
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ResumeOffer_FinalizedBeforeAck_DoesNotFailBackgroundOperations(bool timeout)
    {
        var time = new ManualTimeProvider();
        var transport = new TestTransport();
        var remoteId = Guid.NewGuid();
        var endpoint = new IPEndPoint(IPAddress.Loopback, 5001);
        await using var exchange = new FileExchange(
            transport,
            Guid.NewGuid(),
            _ => endpoint,
            CancellationToken.None,
            timeProvider: time
        );
        var transfer = exchange.Transfers.CreateOutgoingTransfer(
            remoteId,
            "paused.bin",
            1,
            new byte[32]
        );
        var key = new TransferKey(remoteId, transfer.FileId);
        exchange.Transfers.SetState(key, TransferState.Paused);
        int offers = 0;
        transport.OnSend = (packet, _, _, _) =>
        {
            Assert.Equal(MessageType.FileOffer, packet.Type);
            offers++;
            if (timeout)
            {
                time.Advance(ProtocolConstants.PauseTimeout);
                exchange.Transfers.CheckTimeouts();
            }
            else
            {
                exchange.CancelPausedTransfer(new TransferKey(remoteId, transfer.FileId));
            }
            return Task.CompletedTask;
        };

        await exchange.CheckCompletedOperationsAsync(CancellationToken.None);
        time.Advance(ProtocolConstants.ProbeInterval);
        await exchange.CheckCompletedOperationsAsync(CancellationToken.None);
        await exchange
            .WaitForPendingOperationsAsync(CancellationToken.None)
            .WaitAsync(TimeSpan.FromSeconds(5));
        time.Advance(ProtocolConstants.ProbeInterval);
        await exchange.CheckCompletedOperationsAsync(CancellationToken.None);
        await exchange
            .WaitForPendingOperationsAsync(CancellationToken.None)
            .WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Equal(
            timeout ? TransferState.Failed : TransferState.Cancelled,
            exchange.GetTransfer(new TransferKey(remoteId, transfer.FileId)).State
        );
        Assert.Equal(1, offers);
    }

    private sealed class ManualTimeProvider : TimeProvider
    {
        private long _timestamp;
        public override long TimestampFrequency => TimeSpan.TicksPerSecond;

        public override long GetTimestamp() => _timestamp;

        public override DateTimeOffset GetUtcNow() => DateTimeOffset.UnixEpoch.AddTicks(_timestamp);

        public void Advance(TimeSpan elapsed) => _timestamp += elapsed.Ticks;
    }

    private sealed class TestTransport : IMessageTransport
    {
        private uint _seq;
        public ConcurrentQueue<uint> Acks { get; } = new();
        public Func<Packet, IPEndPoint, Guid, CancellationToken, Task> OnSend { get; set; } =
            (_, _, _, _) => Task.CompletedTask;
        public Func<Task> OnAck { get; set; } = () => Task.CompletedTask;

        public uint NextSeq() => _seq++;

        public Task SendReliableAsync(
            Packet packet,
            IPEndPoint remote,
            Guid remoteDeviceId,
            CancellationToken ct
        ) => OnSend(packet, remote, remoteDeviceId, ct);

        public Task SendAckAsync(uint ackedSeq, IPEndPoint remote, Guid deviceId)
        {
            Acks.Enqueue(ackedSeq);
            return OnAck();
        }
    }
}
