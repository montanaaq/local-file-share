using System.Security.Cryptography;

namespace Protocol.Tests;

public class FileTransferServiceTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task IncomingPause_DeletesPartOnTimeoutOrCancellation(bool cancel)
    {
        string directory = Path.Combine(Path.GetTempPath(), $"protocol-timeout-{Guid.NewGuid():N}");
        try
        {
            var clock = new TransferClock();
            await using var service = new FileTransferService(clock);
            var sender = Guid.NewGuid();
            var fileId = Guid.NewGuid();
            byte[] offer = Payloads.EncodeFileOffer(sender, fileId, 1, new byte[32], "timeout.bin");
            service.HandleFileOffer(offer);
            service.SetIncomingDecision(new TransferKey(sender, fileId), true, directory);
            service.PauseAll();
            if (cancel)
            {
                service.CancelPaused(new TransferKey(sender, fileId));
            }
            else
            {
                clock.Advance(ProtocolConstants.PauseTimeout);
                service.CheckTimeouts();
            }
            Assert.Equal(
                cancel ? TransferState.Cancelled : TransferState.Failed,
                service.GetTransfer(new TransferKey(sender, fileId)).State
            );
            Assert.Empty(Directory.GetFiles(directory));
        }
        finally
        {
            if (Directory.Exists(directory))
            {
                Directory.Delete(directory, true);
            }
        }
    }

    [Fact]
    public async Task CompleteIncomingTransferAsync_SanitizesPeerName_AndPreservesExistingFile()
    {
        string directory = Path.Combine(Path.GetTempPath(), $"protocol-save-{Guid.NewGuid():N}");
        try
        {
            Directory.CreateDirectory(directory);
            const string fileName = "../../report.bin";
            string safeName = fileName.SanitizeFileName();
            string existingPath = Path.Combine(directory, safeName.WithFileSuffix(0));
            await File.WriteAllBytesAsync(existingPath, [9]);
            byte[] data = [1, 2, 3];
            await using var service = new FileTransferService();
            var deviceId = Guid.NewGuid();
            var fileId = Guid.NewGuid();
            service.HandleFileOffer(
                Payloads.EncodeFileOffer(
                    deviceId,
                    fileId,
                    (ulong)data.Length,
                    SHA256.HashData(data),
                    fileName
                )
            );
            service.SetIncomingDecision(new TransferKey(deviceId, fileId), true, directory);
            Assert.True(service.HandleChunk(Payloads.EncodeChunk(deviceId, fileId, 0, data)));

            var key = new TransferKey(deviceId, fileId);
            var completion = service.CompleteIncomingTransferAsync(key, CancellationToken.None);
            Assert.True(await completion);

            Assert.Equal(
                TransferState.Completed,
                service.GetTransfer(new TransferKey(deviceId, fileId)).State
            );
            Assert.Equal(new byte[] { 9 }, await File.ReadAllBytesAsync(existingPath));
            Assert.Equal(
                data,
                await File.ReadAllBytesAsync(Path.Combine(directory, safeName.WithFileSuffix(1)))
            );
            Assert.Equal(2, Directory.GetFiles(directory).Length);
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
    public async Task CompleteIncomingTransferAsync_BadHash_FailsAndDeletesPartFile()
    {
        string directory = Path.Combine(Path.GetTempPath(), $"protocol-save-{Guid.NewGuid():N}");
        try
        {
            await using var service = new FileTransferService();
            var deviceId = Guid.NewGuid();
            var fileId = Guid.NewGuid();
            service.HandleFileOffer(
                Payloads.EncodeFileOffer(deviceId, fileId, 1, new byte[32], "report.bin")
            );
            service.SetIncomingDecision(new TransferKey(deviceId, fileId), true, directory);
            Assert.True(service.HandleChunk(Payloads.EncodeChunk(deviceId, fileId, 0, [1])));

            Assert.False(
                await service.CompleteIncomingTransferAsync(
                    new TransferKey(deviceId, fileId),
                    CancellationToken.None
                )
            );

            Assert.Equal(
                TransferState.Failed,
                service.GetTransfer(new TransferKey(deviceId, fileId)).State
            );
            Assert.Empty(Directory.GetFiles(directory));
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
    public async Task IncomingFile_IgnoresInvalidAndDuplicateChunks()
    {
        string directory = Path.Combine(Path.GetTempPath(), $"protocol-chunks-{Guid.NewGuid():N}");
        try
        {
            await using var service = new FileTransferService();
            var sender = Guid.NewGuid();
            var fileId = Guid.NewGuid();
            var key = new TransferKey(sender, fileId);
            byte[] data = [1, 2, 3];
            service.HandleFileOffer(
                Payloads.EncodeFileOffer(
                    sender,
                    fileId,
                    (ulong)data.Length,
                    SHA256.HashData(data),
                    "test.bin"
                )
            );
            service.SetIncomingDecision(key, true, directory);

            Assert.False(service.HandleChunk(Payloads.EncodeChunk(sender, fileId, 1, data)));
            Assert.False(service.HandleChunk(Payloads.EncodeChunk(sender, fileId, 0, [1])));
            Assert.Equal(0u, service.GetTransfer(key).NextIndex);

            byte[] chunk = Payloads.EncodeChunk(sender, fileId, 0, data);
            Assert.True(service.HandleChunk(chunk));
            Assert.True(service.HandleChunk(chunk));
            Assert.Equal(1u, service.GetTransfer(key).NextIndex);
            Assert.True(await service.CompleteIncomingTransferAsync(key, CancellationToken.None));
            Assert.Equal(data, await File.ReadAllBytesAsync(service.GetTransfer(key).SavedPath!));
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

    private sealed class TransferClock : TimeProvider
    {
        private long _timestamp;
        public override long TimestampFrequency => TimeSpan.TicksPerSecond;

        public override long GetTimestamp() => _timestamp;

        public override DateTimeOffset GetUtcNow() => DateTimeOffset.UnixEpoch.AddTicks(_timestamp);

        public void Advance(TimeSpan elapsed) => _timestamp += elapsed.Ticks;
    }
}
