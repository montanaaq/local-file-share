using System.Buffers.Binary;
using System.Security.Cryptography;

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
            service.SetIncomingDecision(deviceId, fileId, true, directory);

            var result = service.HandleChunk(
                Payloads.EncodeChunk(deviceId, fileId, index, new byte[dataLength])
            );

            Assert.Equal(expected, result);
            Assert.Equal(0u, service.GetTransfer(deviceId, fileId).NextIndex);
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
                service.SetIncomingDecision(deviceId, fileId, true, directory);
                var first = Payloads.EncodeChunk(deviceId, fileId, 0, new byte[1024]);

                Assert.True(
                    service.HandleChunk(Payloads.EncodeChunk(deviceId, fileId, 2, new byte[452]))
                );
                Assert.Equal(0u, service.GetTransfer(deviceId, fileId).NextIndex);

                Assert.True(service.HandleChunk(first));
                Assert.Equal(1u, service.GetTransfer(deviceId, fileId).NextIndex);
                Assert.True(service.HandleChunk(first));
                Assert.Equal(1u, service.GetTransfer(deviceId, fileId).NextIndex);

                Assert.True(
                    service.HandleChunk(Payloads.EncodeChunk(deviceId, fileId, 1, new byte[1024]))
                );
                Assert.Equal(3u, service.GetTransfer(deviceId, fileId).NextIndex);
                Assert.Equal(
                    TransferState.Transferring,
                    service.GetTransfer(deviceId, fileId).State
                );
            }

            Assert.Empty(Directory.GetFiles(directory));
        }
        finally
        {
            if (Directory.Exists(directory))
                Directory.Delete(directory, recursive: true);
        }
    }

    [Fact]
    public async Task CreateOutgoingTransfer_ReturnsIndependentSnapshots()
    {
        await using var service = new FileTransferService();
        var deviceId = Guid.NewGuid();
        var hash = new byte[32];
        var transfer = service.CreateOutgoingTransfer(deviceId, "test.bin", 1, hash);
        var fileId = transfer.FileId;

        hash[0] = 1;
        transfer.Sha256[1] = 2;
        transfer.State = TransferState.Completed;
        transfer.FileId = Guid.NewGuid();

        var snapshot = service.GetTransfer(deviceId, fileId);
        Assert.Equal(TransferState.WaitingForAnswer, snapshot.State);
        Assert.Equal(new byte[32], snapshot.Sha256);
        snapshot.Sha256[2] = 3;
        Assert.Equal(new byte[32], service.GetTransfer(deviceId, fileId).Sha256);
    }

    [Fact]
    public async Task HandleFileOffer_RejectsOversizedFile_AndPublishesIndependentSnapshot()
    {
        await using var service = new FileTransferService();
        var deviceId = Guid.NewGuid();
        var fileId = Guid.NewGuid();
        FileTransfer? offered = null;
        service.FileOffered += transfer => offered = transfer;
        var payload = Payloads.EncodeFileOffer(deviceId, fileId, 1, new byte[32], "test.bin");
        var invalidPayload = (byte[])payload.Clone();
        BinaryPrimitives.WriteUInt64BigEndian(
            invalidPayload.AsSpan(PayloadsConstants.FileSizeOffset),
            (ulong)ProtocolConstants.MaxFileSize + 1
        );

        service.HandleFileOffer(invalidPayload);
        Assert.Null(offered);
        service.HandleFileOffer(payload);
        Assert.NotNull(offered);
        offered.State = TransferState.Completed;
        offered.Sha256[0] = 1;
        var snapshot = service.GetTransfer(deviceId, fileId);
        Assert.Equal(TransferState.WaitingForAnswer, snapshot.State);
        Assert.Equal(new byte[32], snapshot.Sha256);
    }

    [Fact]
    public async Task WaitForFileAnswerAsync_PreservesRejection_WhenTokenIsCancelled()
    {
        await using var service = new FileTransferService();
        var deviceId = Guid.NewGuid();
        var transfer = service.CreateOutgoingTransfer(deviceId, "test.bin", 1, new byte[32]);
        using var cts = new CancellationTokenSource();
        Assert.True(
            service.HandleFileAnswer(
                Payloads.EncodeFileAnswer(deviceId, transfer.FileId, false, uint.MaxValue)
            )
        );
        cts.Cancel();

        Assert.False(await service.WaitForFileAnswerAsync(deviceId, transfer.FileId, cts.Token));
        Assert.Equal(TransferState.Rejected, service.GetTransfer(deviceId, transfer.FileId).State);
    }

    [Fact]
    public async Task WaitForFileAnswerAsync_Cancellation_CancelsTransferAndRejectsLateAnswer()
    {
        await using var service = new FileTransferService();
        var deviceId = Guid.NewGuid();
        var transfer = service.CreateOutgoingTransfer(deviceId, "test.bin", 1, new byte[32]);
        using var cts = new CancellationTokenSource();
        var waiting = service.WaitForFileAnswerAsync(deviceId, transfer.FileId, cts.Token);
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => waiting);
        Assert.Equal(TransferState.Cancelled, service.GetTransfer(deviceId, transfer.FileId).State);
        Assert.False(
            service.HandleFileAnswer(Payloads.EncodeFileAnswer(deviceId, transfer.FileId, true, 0))
        );
    }

    [Fact]
    public async Task DisposeAsync_CancelsWaiters_AndRejectsFurtherOperations()
    {
        var service = new FileTransferService();
        var deviceId = Guid.NewGuid();
        var transfer = service.CreateOutgoingTransfer(deviceId, "test.bin", 1, new byte[32]);
        var waiting = service.WaitForFileAnswerAsync(
            deviceId,
            transfer.FileId,
            CancellationToken.None
        );

        await service.DisposeAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => waiting);
        await Assert.ThrowsAsync<ObjectDisposedException>(() =>
            service.WaitForFileAnswerAsync(deviceId, transfer.FileId, CancellationToken.None)
        );
        Assert.Throws<ObjectDisposedException>(() =>
            service.GetTransfer(deviceId, transfer.FileId)
        );
        Assert.False(
            service.HandleFileAnswer(Payloads.EncodeFileAnswer(deviceId, transfer.FileId, true, 0))
        );
        service.HandleFileOffer(
            Payloads.EncodeFileOffer(deviceId, Guid.NewGuid(), 1, new byte[32], "test.bin")
        );
        await service.DisposeAsync();
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task OpenOutgoingFileAsync_ChecksSourceHash_AndRewindsUnchangedFile(bool changed)
    {
        var path = Path.Combine(Path.GetTempPath(), $"protocol-source-{Guid.NewGuid():N}.bin");
        try
        {
            await File.WriteAllBytesAsync(path, [1, 2, 3]);
            await using var service = new FileTransferService();
            var deviceId = Guid.NewGuid();
            var transfer = await service.PrepareOutgoingTransferAsync(
                deviceId,
                path,
                CancellationToken.None
            );
            Assert.True(
                service.HandleFileAnswer(
                    Payloads.EncodeFileAnswer(deviceId, transfer.FileId, true, 0)
                )
            );
            var key = new TransferKey(deviceId, transfer.FileId);

            if (changed)
            {
                await File.WriteAllBytesAsync(path, [4, 5, 6]);
                await Assert.ThrowsAsync<IOException>(() =>
                    service.OpenOutgoingFileAsync(key, CancellationToken.None)
                );
            }
            else
            {
                using var cts = new CancellationTokenSource();
                cts.Cancel();
                await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
                    service.OpenOutgoingFileAsync(key, cts.Token)
                );

                await using var stream = await service.OpenOutgoingFileAsync(
                    key,
                    CancellationToken.None
                );
                Assert.Equal(0, stream.Position);
                Assert.Equal(1, stream.ReadByte());
            }
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task HandleChunk_ConcurrentWrites_PreserveProgressAndCleanUp()
    {
        var directory = Path.Combine(Path.GetTempPath(), $"protocol-chunks-{Guid.NewGuid():N}");
        try
        {
            await using (var service = new FileTransferService())
            {
                var deviceId = Guid.NewGuid();
                var fileId = Guid.NewGuid();
                service.HandleFileOffer(
                    Payloads.EncodeFileOffer(deviceId, fileId, 8192, new byte[32], "test.bin")
                );
                service.SetIncomingDecision(deviceId, fileId, true, directory);
                var chunks = Enumerable
                    .Range(0, 8)
                    .Select(index =>
                        Payloads.EncodeChunk(
                            deviceId,
                            fileId,
                            (uint)index,
                            Enumerable.Repeat((byte)index, 1024).ToArray()
                        )
                    )
                    .ToArray();

                await Task.WhenAll(
                    chunks
                        .Reverse()
                        .Concat(chunks)
                        .Select(payload =>
                            Task.Run(() => Assert.True(service.HandleChunk(payload)))
                        )
                );

                Assert.Equal(8u, service.GetTransfer(deviceId, fileId).NextIndex);
            }
            Assert.Empty(Directory.GetFiles(directory));
        }
        finally
        {
            if (Directory.Exists(directory))
                Directory.Delete(directory, recursive: true);
        }
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task WaitForFileResultAsync_ConsumesResultReceivedBeforeWaiting(bool ok)
    {
        await using var service = new FileTransferService();
        var deviceId = Guid.NewGuid();
        var transfer = service.CreateOutgoingTransfer(
            deviceId,
            "empty.bin",
            0,
            SHA256.HashData([])
        );
        var key = new TransferKey(deviceId, transfer.FileId);
        Exception? failure = null;
        service.TransferFailed += (_, error) => failure = error;
        Assert.True(
            service.HandleFileAnswer(Payloads.EncodeFileAnswer(deviceId, transfer.FileId, true, 0))
        );
        Assert.True(
            service.HandleFileResult(Payloads.EncodeFileResult(deviceId, transfer.FileId, ok))
        );

        Assert.Equal(ok, await service.WaitForFileResultAsync(key, CancellationToken.None));
        Assert.True(
            service.HandleFileResult(Payloads.EncodeFileResult(deviceId, transfer.FileId, ok))
        );
        Assert.False(
            service.HandleFileResult(Payloads.EncodeFileResult(deviceId, transfer.FileId, !ok))
        );
        Assert.Equal(
            ok ? TransferState.Completed : TransferState.Failed,
            service.GetTransfer(deviceId, transfer.FileId).State
        );
        if (ok)
            Assert.Null(failure);
        else
            Assert.IsType<InvalidDataException>(failure);
    }

    [Fact]
    public async Task DisposeAsync_CancelsFileResultWaiter()
    {
        var service = new FileTransferService();
        var deviceId = Guid.NewGuid();
        var transfer = service.CreateOutgoingTransfer(
            deviceId,
            "empty.bin",
            0,
            SHA256.HashData([])
        );
        Assert.True(
            service.HandleFileAnswer(Payloads.EncodeFileAnswer(deviceId, transfer.FileId, true, 0))
        );
        var waiting = service.WaitForFileResultAsync(
            new TransferKey(deviceId, transfer.FileId),
            CancellationToken.None
        );

        await service.DisposeAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => waiting);
    }

    [Fact]
    public async Task CompleteIncomingTransferAsync_SanitizesPeerName_AndPreservesExistingFile()
    {
        var directory = Path.Combine(Path.GetTempPath(), $"protocol-save-{Guid.NewGuid():N}");
        try
        {
            Directory.CreateDirectory(directory);
            const string fileName = "../../report.bin";
            var safeName = fileName.SanitizeFileName();
            var existingPath = Path.Combine(directory, safeName.WithFileSuffix(0));
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
            service.SetIncomingDecision(deviceId, fileId, true, directory);
            Assert.True(service.HandleChunk(Payloads.EncodeChunk(deviceId, fileId, 0, data)));

            var key = new TransferKey(deviceId, fileId);
            var completion = service.CompleteIncomingTransferAsync(key, CancellationToken.None);
            Assert.Same(
                completion,
                service.CompleteIncomingTransferAsync(key, CancellationToken.None)
            );
            Assert.True(await completion);
            Assert.Same(
                completion,
                service.CompleteIncomingTransferAsync(key, CancellationToken.None)
            );

            Assert.Equal(TransferState.Completed, service.GetTransfer(deviceId, fileId).State);
            Assert.True(service.HandleChunk(Payloads.EncodeChunk(deviceId, fileId, 0, data)));
            Assert.False(service.HandleChunk(Payloads.EncodeChunk(deviceId, fileId, 1, data)));
            Assert.False(service.HandleChunk(Payloads.EncodeChunk(deviceId, fileId, 0, [1])));
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
                Directory.Delete(directory, recursive: true);
        }
    }

    [Fact]
    public async Task CompleteIncomingTransferAsync_BadHash_FailsAndDeletesPartFile()
    {
        var directory = Path.Combine(Path.GetTempPath(), $"protocol-save-{Guid.NewGuid():N}");
        try
        {
            await using var service = new FileTransferService();
            var deviceId = Guid.NewGuid();
            var fileId = Guid.NewGuid();
            service.HandleFileOffer(
                Payloads.EncodeFileOffer(deviceId, fileId, 1, new byte[32], "report.bin")
            );
            service.SetIncomingDecision(deviceId, fileId, true, directory);
            Assert.True(service.HandleChunk(Payloads.EncodeChunk(deviceId, fileId, 0, [1])));

            Assert.False(
                await service.CompleteIncomingTransferAsync(
                    new TransferKey(deviceId, fileId),
                    CancellationToken.None
                )
            );

            Assert.Equal(TransferState.Failed, service.GetTransfer(deviceId, fileId).State);
            Assert.Empty(Directory.GetFiles(directory));
        }
        finally
        {
            if (Directory.Exists(directory))
                Directory.Delete(directory, recursive: true);
        }
    }

    [Fact]
    public async Task HandleChunk_ConcurrentCancellation_DoesNotThrowOrRetainPartFile()
    {
        var directory = Path.Combine(Path.GetTempPath(), $"protocol-cancel-{Guid.NewGuid():N}");
        try
        {
            await using var service = new FileTransferService();
            var deviceId = Guid.NewGuid();
            var fileId = Guid.NewGuid();
            service.HandleFileOffer(
                Payloads.EncodeFileOffer(deviceId, fileId, 1024, new byte[32], "report.bin")
            );
            service.SetIncomingDecision(deviceId, fileId, true, directory);
            var payload = Payloads.EncodeChunk(deviceId, fileId, 0, new byte[1024]);
            var writes = Enumerable
                .Range(0, 16)
                .Select(_ =>
                    Task.Run(() =>
                    {
                        for (var index = 0; index < 16; index++)
                            service.HandleChunk(payload);
                    })
                )
                .ToArray();

            service.SetState(new TransferKey(deviceId, fileId), TransferState.Cancelled);
            await Task.WhenAll(writes);

            Assert.False(service.HandleChunk(payload));
            Assert.Equal(TransferState.Cancelled, service.GetTransfer(deviceId, fileId).State);
            Assert.Empty(Directory.GetFiles(directory));
        }
        finally
        {
            if (Directory.Exists(directory))
                Directory.Delete(directory, recursive: true);
        }
    }

    [Fact]
    public async Task UnknownTransfer_UsesArgumentException_ForCallerOperations()
    {
        await using var service = new FileTransferService();
        var key = new TransferKey(Guid.NewGuid(), Guid.NewGuid());

        Assert.Throws<ArgumentException>(() => service.GetTransfer(key.PeerDeviceId, key.FileId));
        Assert.Throws<ArgumentException>(() =>
            service.SetIncomingDecision(key.PeerDeviceId, key.FileId, false, "unused")
        );
        await Assert.ThrowsAsync<ArgumentException>(() =>
            service.WaitForFileAnswerAsync(key.PeerDeviceId, key.FileId, CancellationToken.None)
        );
        await Assert.ThrowsAsync<ArgumentException>(() =>
            service.WaitForFileResultAsync(key, CancellationToken.None)
        );
        await Assert.ThrowsAsync<ArgumentException>(() =>
            service.CompleteIncomingTransferAsync(key, CancellationToken.None)
        );
        await Assert.ThrowsAsync<ArgumentException>(() =>
            service.OpenOutgoingFileAsync(key, CancellationToken.None)
        );
        Assert.Throws<ArgumentException>(() => service.ConfirmOutgoingChunk(key, 0));
    }
}
