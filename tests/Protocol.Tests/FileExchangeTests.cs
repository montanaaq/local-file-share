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
            await using var exchange = new FileExchange(
                transport,
                localId,
                _ => endpoint,
                CancellationToken.None
            );
            var sentTypes = new List<MessageType>();
            Guid fileId = default;
            transport.OnSend = async (packet, destination, deviceId, ct) =>
            {
                Assert.Equal(remoteId, deviceId);
                Assert.Equal(endpoint, destination);
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
                            exchange.Transfers.GetTransfer(remoteId, fileId).NextIndex
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
                            exchange.Transfers.GetTransfer(remoteId, fileId).NextIndex
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
            await exchange.AnswerFileOfferAsync(remoteId, fileId, true, directory, cts.Token);
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
                    remoteId,
                    fileId,
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

    private sealed class TestTransport : IMessageTransport
    {
        private uint _seq;
        public ConcurrentQueue<uint> Acks { get; } = new();
        public Func<Packet, IPEndPoint, Guid, CancellationToken, Task> OnSend { get; set; } =
            (_, _, _, _) => Task.CompletedTask;

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
            return Task.CompletedTask;
        }
    }
}
