using System;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Protocol;

internal sealed class OutgoingFileTransfer
{
    private readonly string? _sourcePath;

    public FileTransfer Transfer { get; }

    public OutgoingFileTransfer(
        Guid peerDeviceId,
        string fileName,
        ulong fileSize,
        byte[] sha256,
        string? sourcePath = null
    )
    {
        if (fileSize > (ulong)ProtocolConstants.MaxFileSize)
        {
            throw new ArgumentOutOfRangeException(
                nameof(fileSize),
                fileSize,
                "File size exceeds the maximum allowed size"
            );
        }

        if (sha256 is null)
        {
            throw new ArgumentNullException(nameof(sha256), "Value cannot be null");
        }

        if (sha256.Length != PayloadsConstants.Sha256Size)
        {
            throw new ArgumentException("SHA-256 must contain 32 bytes", nameof(sha256));
        }

        if (
            string.IsNullOrEmpty(fileName)
            || Encoding.UTF8.GetByteCount(fileName) > PayloadsConstants.MaxNameBytes
        )
        {
            throw new ArgumentException(
                "File name must contain 1 to 255 UTF-8 bytes",
                nameof(fileName)
            );
        }

        _sourcePath = sourcePath;
        Transfer = new FileTransfer
        {
            FileId = Guid.NewGuid(),
            PeerDeviceId = peerDeviceId,
            FileName = fileName,
            FileSize = fileSize,
            Sha256 = (byte[])sha256.Clone(),
            Direction = TransferDirection.Outgoing,
            State = TransferState.WaitingForAnswer,
        };
    }

    public static async Task<OutgoingFileTransfer> PrepareAsync(
        Guid peerDeviceId,
        string filePath,
        CancellationToken ct
    )
    {
        ct.ThrowIfCancellationRequested();
        string fullPath = Path.GetFullPath(filePath);
        await using var stream = new FileStream(
            fullPath,
            FileMode.Open,
            FileAccess.Read,
            FileShare.Read
        );
        long fileSize = stream.Length;
        if (fileSize > ProtocolConstants.MaxFileSize)
        {
            throw new ArgumentOutOfRangeException(
                nameof(fileSize),
                fileSize,
                "File size exceeds the maximum allowed size"
            );
        }

        byte[] sha256 = await SHA256.HashDataAsync(stream, ct).ConfigureAwait(false);
        ct.ThrowIfCancellationRequested();
        return new OutgoingFileTransfer(
            peerDeviceId,
            Path.GetFileName(fullPath),
            (ulong)fileSize,
            sha256,
            fullPath
        );
    }

    public async Task<FileStream> OpenAsync(CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        if (_sourcePath is null)
        {
            throw new InvalidOperationException("Transfer has no source file path");
        }

        var stream = new FileStream(_sourcePath, FileMode.Open, FileAccess.Read, FileShare.Read);
        try
        {
            // The source may have changed while waiting for the receiver to decide.
            if (
                (ulong)stream.Length != Transfer.FileSize
                || !(await SHA256.HashDataAsync(stream, ct).ConfigureAwait(false))
                    .AsSpan()
                    .SequenceEqual(Transfer.Sha256)
            )
            {
                throw new IOException("Source file has changed since the offer was prepared");
            }

            ct.ThrowIfCancellationRequested();
            stream.Position = 0;
            return stream;
        }
        catch
        {
            await stream.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    public void ConfirmChunk(uint index)
    {
        if (index != Transfer.NextIndex)
        {
            throw new InvalidOperationException("Unexpected acknowledged chunk index");
        }

        if (index * (ulong)ProtocolConstants.ChunkSize >= Transfer.FileSize)
        {
            throw new ArgumentOutOfRangeException(
                nameof(index),
                index,
                "Chunk index is outside the file bounds"
            );
        }

        Transfer.NextIndex++;
    }
}
