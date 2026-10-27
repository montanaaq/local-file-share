using System.Buffers.Binary;
using System.Security.Cryptography;

namespace Protocol.Tests;

public class FileTransferServiceTests
{
    [Fact]
    public async Task IncomingTransfer_ResumesAtMissingChunk_AndRetainsCompletedResult()
    {
        string directory = Path.Combine(Path.GetTempPath(), $"protocol-resume-{Guid.NewGuid():N}");
        try
        {
            var clock = new TransferClock();
            await using var service = new FileTransferService(clock);
            var sender = Guid.NewGuid();
            var fileId = Guid.NewGuid();
            byte[] data = Enumerable.Range(0, 2048).Select(index => (byte)index).ToArray();
            byte[] offer = Payloads.EncodeFileOffer(
                sender,
                fileId,
                (ulong)data.Length,
                SHA256.HashData(data),
                "resume.bin"
            );
            int offered = 0;
            service.FileOffered += _ => offered++;
            Assert.Null(service.HandleFileOffer(offer));
            service.SetIncomingDecision(new TransferKey(sender, fileId), true, directory);
            Assert.True(
                service.HandleChunk(Payloads.EncodeChunk(sender, fileId, 0, data.AsSpan(0, 1024)))
            );

            clock.Advance(ProtocolConstants.IncomingChunkTimeout);
            service.CheckTimeouts();
            var paused = service.GetTransfer(new TransferKey(sender, fileId));
            Assert.Equal(TransferState.Paused, paused.State);
            Assert.Equal(1u, paused.NextIndex);
            Assert.Equal(ProtocolConstants.PauseTimeout, paused.PauseRemaining);
            Assert.Single(Directory.GetFiles(directory, "*.part"));
            Assert.Equal((true, 1u), service.HandleFileOffer(offer));
            Assert.Equal(1, offered);
            Assert.True(
                service.HandleChunk(Payloads.EncodeChunk(sender, fileId, 1, data.AsSpan(1024)))
            );
            var key = new TransferKey(sender, fileId);
            var completion = service.CompleteIncomingTransferAsync(key, CancellationToken.None);
            Assert.True(await completion);
            var completed = service.GetTransfer(new TransferKey(sender, fileId));
            Assert.Equal(data, await File.ReadAllBytesAsync(completed.SavedPath!));
            Assert.Null(completed.PauseRemaining);
            Assert.Equal(1, completed.PauseCount);
            Assert.Equal(1, completed.ResumeCount);
            Assert.Equal((true, 2u), service.HandleFileOffer(offer));
            Assert.Same(
                completion,
                service.HandleFileDone(
                    Payloads.EncodeFileDone(sender, fileId),
                    CancellationToken.None
                )
            );
            Assert.Single(Directory.GetFiles(directory));
        }
        finally
        {
            if (Directory.Exists(directory))
            {
                Directory.Delete(directory, true);
            }
        }
    }

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
            clock.Advance(ProtocolConstants.PauseTimeout - TimeSpan.FromSeconds(1));
            Assert.Equal((true, 0u), service.HandleFileOffer(offer));
            if (cancel)
            {
                service.PauseAll();
                service.CancelPaused(new TransferKey(sender, fileId));
            }
            else
            {
                clock.Advance(TimeSpan.FromSeconds(1));
                service.CheckTimeouts();
            }
            Assert.Equal(
                cancel ? TransferState.Cancelled : TransferState.Failed,
                service.GetTransfer(new TransferKey(sender, fileId)).State
            );
            Assert.Empty(Directory.GetFiles(directory));
            if (cancel)
            {
                Assert.Null(service.HandleFileOffer(offer));
            }
            else
            {
                Assert.Equal((false, 0u), service.HandleFileOffer(offer));
            }
        }
        finally
        {
            if (Directory.Exists(directory))
            {
                Directory.Delete(directory, true);
            }
        }
    }

    [Theory]
    [InlineData(2u, true)]
    [InlineData(3u, true)]
    [InlineData(4u, false)]
    public async Task OutgoingResume_UsesReceiverProgress_AndRejectsOutOfRangeIndex(
        uint index,
        bool accepted
    )
    {
        await using var service = new FileTransferService();
        var peer = Guid.NewGuid();
        var transfer = service.CreateOutgoingTransfer(peer, "resume.bin", 2500, new byte[32]);
        var key = new TransferKey(peer, transfer.FileId);
        Assert.True(
            service.HandleFileAnswer(Payloads.EncodeFileAnswer(peer, transfer.FileId, true, 0))
        );
        service.SetState(key, TransferState.Paused);
        Assert.True(service.PrepareResume(key));

        Assert.Equal(
            accepted,
            service.HandleFileAnswer(Payloads.EncodeFileAnswer(peer, transfer.FileId, true, index))
        );
        var snapshot = service.GetTransfer(new TransferKey(peer, transfer.FileId));
        Assert.Equal(accepted ? TransferState.Transferring : TransferState.Paused, snapshot.State);
        Assert.Equal(accepted ? index : 0u, snapshot.NextIndex);
        if (accepted)
        {
            Assert.True(
                await service.WaitForFileAnswerAsync(
                    new TransferKey(peer, transfer.FileId),
                    CancellationToken.None,
                    resuming: true
                )
            );
            Assert.Null(snapshot.PauseRemaining);
        }
    }

    [Fact]
    public async Task PendingDecision_IsNotAutoAcceptedAfterExchangePause()
    {
        await using var service = new FileTransferService();
        var peer = Guid.NewGuid();
        var fileId = Guid.NewGuid();
        byte[] offer = Payloads.EncodeFileOffer(peer, fileId, 0, SHA256.HashData([]), "empty.bin");
        int count = 0;
        service.FileOffered += _ => count++;
        service.HandleFileOffer(offer);
        service.PauseAll();

        Assert.Null(service.HandleFileOffer(offer));
        Assert.Equal(
            TransferState.WaitingForAnswer,
            service.GetTransfer(new TransferKey(peer, fileId)).State
        );
        Assert.Equal(1, count);
        Assert.Equal(
            (false, 0u),
            service.HandleFileOffer(
                Payloads.EncodeFileOffer(peer, fileId, 0, SHA256.HashData([]), "different.bin")
            )
        );
        Assert.Equal(
            TransferState.WaitingForAnswer,
            service.GetTransfer(new TransferKey(peer, fileId)).State
        );
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RepeatedFileOffer_RejectsChangedMetadata_WithoutResuming(bool changeSize)
    {
        string directory = Path.Combine(Path.GetTempPath(), $"protocol-offer-{Guid.NewGuid():N}");
        try
        {
            await using var service = new FileTransferService();
            var sender = Guid.NewGuid();
            var fileId = Guid.NewGuid();
            var key = new TransferKey(sender, fileId);
            byte[] data = new byte[2048];
            byte[] hash = SHA256.HashData(data);
            int offered = 0;
            int changed = 0;
            service.FileOffered += _ => offered++;
            service.TransferChanged += _ => changed++;
            service.HandleFileOffer(
                Payloads.EncodeFileOffer(sender, fileId, (ulong)data.Length, hash, "resume.bin")
            );
            service.SetIncomingDecision(key, true, directory);
            Assert.True(
                service.HandleChunk(Payloads.EncodeChunk(sender, fileId, 0, data.AsSpan(0, 1024)))
            );
            service.PauseAll();
            changed = 0;
            byte[] offeredHash = (byte[])hash.Clone();
            if (!changeSize)
            {
                offeredHash[0] ^= 0xFF;
            }

            var response = service.HandleFileOffer(
                Payloads.EncodeFileOffer(
                    sender,
                    fileId,
                    (ulong)data.Length + (changeSize ? 1ul : 0ul),
                    offeredHash,
                    "resume.bin"
                )
            );

            Assert.Equal((false, 0u), response);
            var transfer = service.GetTransfer(key);
            Assert.Equal(TransferState.Paused, transfer.State);
            Assert.Equal(1u, transfer.NextIndex);
            Assert.Equal(hash, transfer.Sha256);
            Assert.Equal(1, offered);
            Assert.Equal(0, changed);
            Assert.Single(Directory.GetFiles(directory, "*.part"));
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

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Pause_DoesNotOverrideReceivedFileResult(bool ok)
    {
        await using var service = new FileTransferService();
        var peer = Guid.NewGuid();
        var transfer = service.CreateOutgoingTransfer(peer, "empty.bin", 0, SHA256.HashData([]));
        var key = new TransferKey(peer, transfer.FileId);
        Assert.True(
            service.HandleFileAnswer(Payloads.EncodeFileAnswer(peer, transfer.FileId, true, 0))
        );
        Assert.True(service.HandleFileResult(Payloads.EncodeFileResult(peer, transfer.FileId, ok)));
        service.SetState(key, TransferState.Paused);
        Assert.False(service.PrepareResume(key));

        Assert.Equal(
            ok,
            await service
                .WaitForFileResultAsync(key, CancellationToken.None)
                .WaitAsync(TimeSpan.FromSeconds(1))
        );
        Assert.Equal(
            ok ? TransferState.Completed : TransferState.Failed,
            service.GetTransfer(new TransferKey(peer, transfer.FileId)).State
        );
    }

    [Fact]
    public async Task LateResult_CompletesPausedTransfer()
    {
        await using var service = new FileTransferService();
        var peer = Guid.NewGuid();
        var transfer = service.CreateOutgoingTransfer(peer, "empty.bin", 0, SHA256.HashData([]));
        var key = new TransferKey(peer, transfer.FileId);
        Assert.True(
            service.HandleFileAnswer(Payloads.EncodeFileAnswer(peer, transfer.FileId, true, 0))
        );
        service.SetState(key, TransferState.Paused);

        Assert.True(
            service.HandleFileResult(Payloads.EncodeFileResult(peer, transfer.FileId, true))
        );

        Assert.Equal(
            TransferState.Completed,
            service.GetTransfer(new TransferKey(peer, transfer.FileId)).State
        );
        Assert.True(await service.WaitForFileResultAsync(key, CancellationToken.None));
    }

    [Fact]
    public async Task IncomingDecision_UsesDecisionTimeout_InsteadOfChunkTimeout()
    {
        var clock = new TransferClock();
        await using var service = new FileTransferService(clock);
        var peer = Guid.NewGuid();
        var fileId = Guid.NewGuid();
        service.HandleFileOffer(
            Payloads.EncodeFileOffer(peer, fileId, 0, SHA256.HashData([]), "empty.bin")
        );
        clock.Advance(ProtocolConstants.IncomingChunkTimeout);
        service.CheckTimeouts();
        Assert.Equal(
            TransferState.WaitingForAnswer,
            service.GetTransfer(new TransferKey(peer, fileId)).State
        );

        clock.Advance(ProtocolConstants.FileAnswerTimeout - ProtocolConstants.IncomingChunkTimeout);
        service.CheckTimeouts();

        Assert.Equal(
            TransferState.Failed,
            service.GetTransfer(new TransferKey(peer, fileId)).State
        );
    }

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
        string directory = Path.Combine(Path.GetTempPath(), $"protocol-chunks-{Guid.NewGuid():N}");
        try
        {
            await using var service = new FileTransferService();
            var deviceId = Guid.NewGuid();
            var fileId = Guid.NewGuid();
            service.HandleFileOffer(
                Payloads.EncodeFileOffer(deviceId, fileId, fileSize, new byte[32], "test.bin")
            );
            service.SetIncomingDecision(new TransferKey(deviceId, fileId), true, directory);

            bool result = service.HandleChunk(
                Payloads.EncodeChunk(deviceId, fileId, index, new byte[dataLength])
            );

            Assert.Equal(expected, result);
            Assert.Equal(0u, service.GetTransfer(new TransferKey(deviceId, fileId)).NextIndex);
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
    public async Task HandleChunk_TracksMissingChunksAndDuplicates_AndCleansUpOnDispose()
    {
        string directory = Path.Combine(Path.GetTempPath(), $"protocol-chunks-{Guid.NewGuid():N}");
        try
        {
            await using (var service = new FileTransferService())
            {
                var deviceId = Guid.NewGuid();
                var fileId = Guid.NewGuid();
                service.HandleFileOffer(
                    Payloads.EncodeFileOffer(deviceId, fileId, 2500, new byte[32], "test.bin")
                );
                service.SetIncomingDecision(new TransferKey(deviceId, fileId), true, directory);
                byte[] first = Payloads.EncodeChunk(deviceId, fileId, 0, new byte[1024]);

                Assert.True(
                    service.HandleChunk(Payloads.EncodeChunk(deviceId, fileId, 2, new byte[452]))
                );
                Assert.Equal(0u, service.GetTransfer(new TransferKey(deviceId, fileId)).NextIndex);

                Assert.True(service.HandleChunk(first));
                Assert.Equal(1u, service.GetTransfer(new TransferKey(deviceId, fileId)).NextIndex);
                Assert.True(service.HandleChunk(first));
                Assert.Equal(1u, service.GetTransfer(new TransferKey(deviceId, fileId)).NextIndex);

                Assert.True(
                    service.HandleChunk(Payloads.EncodeChunk(deviceId, fileId, 1, new byte[1024]))
                );
                Assert.Equal(3u, service.GetTransfer(new TransferKey(deviceId, fileId)).NextIndex);
                Assert.Equal(
                    TransferState.Transferring,
                    service.GetTransfer(new TransferKey(deviceId, fileId)).State
                );
            }

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
    public async Task CreateOutgoingTransfer_ReturnsIndependentSnapshots()
    {
        await using var service = new FileTransferService();
        var deviceId = Guid.NewGuid();
        byte[] hash = new byte[32];
        var transfer = service.CreateOutgoingTransfer(deviceId, "test.bin", 1, hash);
        var fileId = transfer.FileId;

        hash[0] = 1;
        transfer.Sha256[1] = 2;
        transfer.State = TransferState.Completed;
        transfer.FileId = Guid.NewGuid();

        var snapshot = service.GetTransfer(new TransferKey(deviceId, fileId));
        Assert.Equal(TransferState.WaitingForAnswer, snapshot.State);
        Assert.Equal(new byte[32], snapshot.Sha256);
        snapshot.Sha256[2] = 3;
        Assert.Equal(new byte[32], service.GetTransfer(new TransferKey(deviceId, fileId)).Sha256);
    }

    [Fact]
    public async Task HandleFileOffer_RejectsOversizedFile_AndPublishesIndependentSnapshot()
    {
        await using var service = new FileTransferService();
        var deviceId = Guid.NewGuid();
        var fileId = Guid.NewGuid();
        FileTransfer? offered = null;
        service.FileOffered += transfer => offered = transfer;
        byte[] payload = Payloads.EncodeFileOffer(deviceId, fileId, 1, new byte[32], "test.bin");
        byte[] invalidPayload = (byte[])payload.Clone();
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
        var snapshot = service.GetTransfer(new TransferKey(deviceId, fileId));
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

        Assert.False(
            await service.WaitForFileAnswerAsync(
                new TransferKey(deviceId, transfer.FileId),
                cts.Token
            )
        );
        Assert.Equal(
            TransferState.Rejected,
            service.GetTransfer(new TransferKey(deviceId, transfer.FileId)).State
        );
    }

    [Fact]
    public async Task WaitForFileAnswerAsync_Cancellation_CancelsTransferAndRejectsLateAnswer()
    {
        await using var service = new FileTransferService();
        var deviceId = Guid.NewGuid();
        var transfer = service.CreateOutgoingTransfer(deviceId, "test.bin", 1, new byte[32]);
        using var cts = new CancellationTokenSource();
        var waiting = service.WaitForFileAnswerAsync(
            new TransferKey(deviceId, transfer.FileId),
            cts.Token
        );
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => waiting);
        Assert.Equal(
            TransferState.Cancelled,
            service.GetTransfer(new TransferKey(deviceId, transfer.FileId)).State
        );
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
            new TransferKey(deviceId, transfer.FileId),
            CancellationToken.None
        );

        await service.DisposeAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => waiting);
        await Assert.ThrowsAsync<ObjectDisposedException>(() =>
            service.WaitForFileAnswerAsync(
                new TransferKey(deviceId, transfer.FileId),
                CancellationToken.None
            )
        );
        Assert.Throws<ObjectDisposedException>(() =>
            service.GetTransfer(new TransferKey(deviceId, transfer.FileId))
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
        string path = Path.Combine(Path.GetTempPath(), $"protocol-source-{Guid.NewGuid():N}.bin");
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
        string directory = Path.Combine(Path.GetTempPath(), $"protocol-chunks-{Guid.NewGuid():N}");
        try
        {
            await using (var service = new FileTransferService())
            {
                var deviceId = Guid.NewGuid();
                var fileId = Guid.NewGuid();
                service.HandleFileOffer(
                    Payloads.EncodeFileOffer(deviceId, fileId, 8192, new byte[32], "test.bin")
                );
                service.SetIncomingDecision(new TransferKey(deviceId, fileId), true, directory);
                byte[][] chunks = Enumerable
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

                Assert.Equal(8u, service.GetTransfer(new TransferKey(deviceId, fileId)).NextIndex);
            }
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
            service.GetTransfer(new TransferKey(deviceId, transfer.FileId)).State
        );
        if (ok)
        {
            Assert.Null(failure);
        }
        else
        {
            Assert.IsType<InvalidDataException>(failure);
        }
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
            Assert.Same(
                completion,
                service.CompleteIncomingTransferAsync(key, CancellationToken.None)
            );
            Assert.True(await completion);
            Assert.Same(
                completion,
                service.CompleteIncomingTransferAsync(key, CancellationToken.None)
            );

            Assert.Equal(
                TransferState.Completed,
                service.GetTransfer(new TransferKey(deviceId, fileId)).State
            );
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
    public async Task HandleChunk_ConcurrentCancellation_DoesNotThrowOrRetainPartFile()
    {
        string directory = Path.Combine(Path.GetTempPath(), $"protocol-cancel-{Guid.NewGuid():N}");
        try
        {
            await using var service = new FileTransferService();
            var deviceId = Guid.NewGuid();
            var fileId = Guid.NewGuid();
            service.HandleFileOffer(
                Payloads.EncodeFileOffer(deviceId, fileId, 1024, new byte[32], "report.bin")
            );
            service.SetIncomingDecision(new TransferKey(deviceId, fileId), true, directory);
            byte[] payload = Payloads.EncodeChunk(deviceId, fileId, 0, new byte[1024]);
            var writes = Enumerable
                .Range(0, 16)
                .Select(_ =>
                    Task.Run(() =>
                    {
                        for (int index = 0; index < 16; index++)
                        {
                            service.HandleChunk(payload);
                        }
                    })
                )
                .ToArray();

            service.SetState(new TransferKey(deviceId, fileId), TransferState.Cancelled);
            await Task.WhenAll(writes);

            Assert.False(service.HandleChunk(payload));
            Assert.Equal(
                TransferState.Cancelled,
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
    public async Task UnknownTransfer_UsesArgumentException_ForCallerOperations()
    {
        await using var service = new FileTransferService();
        var key = new TransferKey(Guid.NewGuid(), Guid.NewGuid());

        Assert.Throws<ArgumentException>(() => service.GetTransfer(key));
        Assert.Throws<ArgumentException>(() => service.SetIncomingDecision(key, false, "unused"));
        await Assert.ThrowsAsync<ArgumentException>(() =>
            service.WaitForFileAnswerAsync(key, CancellationToken.None)
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
