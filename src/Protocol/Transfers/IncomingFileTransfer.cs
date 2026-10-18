using System;
using System.IO;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;

namespace Protocol;

internal sealed class IncomingFileTransfer : IDisposable, IAsyncDisposable
{
    private readonly SemaphoreSlim _fileLock = new(1, 1);
    private bool _disposed;
    private readonly string _partPath;
    private readonly FileStream _stream;
    private readonly ulong _fileSize;
    private readonly bool[] _receivedChunks;
    private bool _verified;

    public uint NextIndex { get; private set; }

    private IncomingFileTransfer(string partPath, FileStream fileStream, ulong fileSize)
    {
        _partPath = partPath;
        _stream = fileStream;
        _fileSize = fileSize;

        var chunkSize = (ulong)ProtocolConstants.ChunkSize;
        var chunkCount = (int)((fileSize + chunkSize - 1) / chunkSize);
        _receivedChunks = new bool[chunkCount];
    }

    public static IncomingFileTransfer Create(ulong fileSize, string directory)
    {
        Directory.CreateDirectory(directory);
        var partPath = Path.Combine(directory, $"{Guid.NewGuid():N}.part");
        var stream = new FileStream(
            partPath,
            FileMode.CreateNew,
            FileAccess.ReadWrite,
            FileShare.None
        );
        try
        {
            return new IncomingFileTransfer(partPath, stream, fileSize);
        }
        catch
        {
            stream.Dispose();
            File.Delete(partPath);
            throw;
        }
    }

    public bool TryWriteChunk(uint index, ReadOnlySpan<byte> data, out uint nextIndex)
    {
        _fileLock.Wait();
        try
        {
            return !_disposed && TryWriteChunkCore(index, data);
        }
        finally
        {
            nextIndex = NextIndex;
            _fileLock.Release();
        }
    }

    public async Task<bool> VerifyAsync(byte[] expectedSha256, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(expectedSha256);
        if (expectedSha256.Length != PayloadsConstants.Sha256Size)
            throw new ArgumentException("SHA-256 must contain 32 bytes", nameof(expectedSha256));

        await _fileLock.WaitAsync(ct).ConfigureAwait(false);

        try
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            _verified = false;

            if (NextIndex != (uint)_receivedChunks.Length)
                return false;

            await _stream.FlushAsync(ct);
            if ((ulong)_stream.Length != _fileSize)
                return false;

            _stream.Position = 0;
            var actualSha256 = await SHA256.HashDataAsync(_stream, ct).ConfigureAwait(false);

            _verified = actualSha256.AsSpan().SequenceEqual(expectedSha256);

            return _verified;
        }
        finally
        {
            _fileLock.Release();
        }
    }

    public async Task<string> SaveAsync(string fileName, CancellationToken ct)
    {
        var safeName = fileName.SanitizeFileName();
        await _fileLock.WaitAsync(ct).ConfigureAwait(false);

        try
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            if (!_verified)
                throw new InvalidOperationException("File must be verified before saving");

            var directory =
                Path.GetDirectoryName(_partPath)
                ?? throw new InvalidOperationException("Part file directory not found");

            ct.ThrowIfCancellationRequested();
            await _stream.DisposeAsync();
            _disposed = true;

            for (var number = 0; ; number++)
            {
                ct.ThrowIfCancellationRequested();

                var candidateName = safeName.WithFileSuffix(number);
                var destinationPath = Path.Combine(directory, candidateName);
                try
                {
                    File.Move(_partPath, destinationPath, overwrite: false);

                    return destinationPath;
                }
                catch (IOException)
                    when (File.Exists(_partPath)
                        && (File.Exists(destinationPath) || Directory.Exists(destinationPath))
                    )
                {
                    continue;
                }
            }
        }
        finally
        {
            _fileLock.Release();
        }
    }

    public void Dispose()
    {
        _fileLock.Wait();
        try
        {
            _stream.Dispose();
            _disposed = true;
            File.Delete(_partPath);
        }
        finally
        {
            _fileLock.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _fileLock.WaitAsync().ConfigureAwait(false);
        try
        {
            _disposed = true;
            await _stream.DisposeAsync().ConfigureAwait(false);
            File.Delete(_partPath);
        }
        finally
        {
            _fileLock.Release();
        }
    }

    private bool TryWriteChunkCore(uint index, ReadOnlySpan<byte> data)
    {
        if (index >= (uint)_receivedChunks.Length)
            return false;

        var offset = (long)index * ProtocolConstants.ChunkSize;
        var remaining = (long)_fileSize - offset;
        var expectedLength = (int)Math.Min(ProtocolConstants.ChunkSize, remaining);

        if (data.Length != expectedLength)
            return false;

        if (_receivedChunks[(int)index])
            return true;

        _stream.Position = offset;

        _verified = false;

        _stream.Write(data);
        _receivedChunks[(int)index] = true;

        while (NextIndex < (uint)_receivedChunks.Length && _receivedChunks[(int)NextIndex])
            NextIndex++;

        return true;
    }
}
