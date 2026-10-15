using System;
using System.IO;
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
        _stream.Write(data);
        _receivedChunks[(int)index] = true;

        while (NextIndex < (uint)_receivedChunks.Length && _receivedChunks[(int)NextIndex])
            NextIndex++;

        return true;
    }

    public void Dispose()
    {
        _fileLock.Wait();
        try
        {
            _disposed = true;
            _stream.Dispose();
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
}
