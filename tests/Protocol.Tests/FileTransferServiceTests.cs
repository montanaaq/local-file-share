namespace Protocol.Tests;

public class FileTransferServiceTests
{
    [Theory]
    [InlineData(2500, 0, 1023, false)]
    [InlineData(2500, 2, 452, true)]
    [InlineData(2500, 2, 453, false)]
    [InlineData(2500, 3, 1024, false)]
    [InlineData(2500, uint.MaxValue, 1024, false)]
    [InlineData(0, 0, 0, false)]
    public async Task HandleChunk_ChecksIndexAndLength_AgainstFileSize(
        ulong fileSize,
        uint index,
        int dataLength,
        bool expected
    )
    {
        var directory = Path.Combine(Path.GetTempPath(), $"protocol-chunks-{Guid.NewGuid():N}");
        try
        {
            await using var service = new FileTransferService();
            var deviceId = Guid.NewGuid();
            var fileId = Guid.NewGuid();
            service.HandleFileOffer(
                Payloads.EncodeFileOffer(deviceId, fileId, fileSize, new byte[32], "test.bin")
            );
            var transfer = service.SetIncomingDecision(deviceId, fileId, true, directory);

            var result = service.HandleChunk(
                Payloads.EncodeChunk(deviceId, fileId, index, new byte[dataLength])
            );

            Assert.Equal(expected, result);
            Assert.Equal(0u, transfer.NextIndex);
        }
        finally
        {
            if (Directory.Exists(directory))
                Directory.Delete(directory, recursive: true);
        }
    }

    [Fact]
    public async Task HandleChunk_TracksMissingChunksAndDuplicates_AndCleansUpOnDispose()
    {
        var directory = Path.Combine(Path.GetTempPath(), $"protocol-chunks-{Guid.NewGuid():N}");
        try
        {
            await using (var service = new FileTransferService())
            {
                var deviceId = Guid.NewGuid();
                var fileId = Guid.NewGuid();
                service.HandleFileOffer(
                    Payloads.EncodeFileOffer(deviceId, fileId, 2500, new byte[32], "test.bin")
                );
                var transfer = service.SetIncomingDecision(deviceId, fileId, true, directory);
                var first = Payloads.EncodeChunk(deviceId, fileId, 0, new byte[1024]);

                Assert.True(service.HandleChunk(
                    Payloads.EncodeChunk(deviceId, fileId, 2, new byte[452])
                ));
                Assert.Equal(0u, transfer.NextIndex);

                Assert.True(service.HandleChunk(first));
                Assert.Equal(1u, transfer.NextIndex);
                Assert.True(service.HandleChunk(first));
                Assert.Equal(1u, transfer.NextIndex);

                Assert.True(service.HandleChunk(
                    Payloads.EncodeChunk(deviceId, fileId, 1, new byte[1024])
                ));
                Assert.Equal(3u, transfer.NextIndex);
                Assert.Equal(TransferState.Transferring, transfer.State);
            }

            Assert.Empty(Directory.GetFiles(directory));
        }
        finally
        {
            if (Directory.Exists(directory))
                Directory.Delete(directory, recursive: true);
        }
    }
}
