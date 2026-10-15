using System;
using System.IO;
using System.Threading.Tasks;

namespace Protocol;

internal sealed class IncomingFileTransfer : IAsyncDisposable
{
    public string PartPath { get; }
    public FileStream Stream { get; }
    public bool[] ReceivedChunks { get; }

    public IncomingFileTransfer(string partPath, FileStream fileStream, int chunkCount)
    {
        PartPath = partPath;
        Stream = fileStream;
        ReceivedChunks = new bool[chunkCount];
    }

    public ValueTask DisposeAsync() => Stream.DisposeAsync();
}
