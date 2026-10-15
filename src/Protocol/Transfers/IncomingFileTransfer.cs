using System;
using System.IO;
using System.Threading.Tasks;

namespace Protocol;

internal sealed class IncomingFileTransfer : IDisposable, IAsyncDisposable
{
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

    public bool TryWriteChunk(uint index, ReadOnlySpan<byte> data)
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

        // shortcut: synchronous writes hold the service state lock; use async file locking if disk latency affects responsiveness.
        _stream.Position = offset;
        _stream.Write(data);
        _receivedChunks[(int)index] = true;

        while (NextIndex < (uint)_receivedChunks.Length && _receivedChunks[(int)NextIndex])
            NextIndex++;

        return true;
    }

    public void Dispose()
    {
        _stream.Dispose();
        File.Delete(_partPath);
    }

    public async ValueTask DisposeAsync()
    {
        await _stream.DisposeAsync().ConfigureAwait(false);
        File.Delete(_partPath);
    }
}
