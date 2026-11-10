using System;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Protocol.Messages;
using Protocol.Transfers.Models;

namespace Protocol.Transfers.Files;

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
        ArgumentOutOfRangeException.ThrowIfGreaterThan(
            fileSize,
            (ulong)ProtocolConstants.MaxFileSize
        );

        ArgumentNullException.ThrowIfNull(sha256);

        if (sha256.Length != PayloadsConstants.Sha256Size)
        {
            throw new ArgumentException("SHA-256 must contain 32 bytes", nameof(sha256));
        }

        ArgumentException.ThrowIfNullOrEmpty(fileName);

        if (Encoding.UTF8.GetByteCount(fileName) > PayloadsConstants.MaxNameBytes)
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

    /// <summary>
    /// Читает размер и SHA-256 существующего файла и подготавливает метаданные предложения.
    /// </summary>
    /// <param name="peerDeviceId">Идентификатор удалённого устройства из списка известных устройств.</param>
    /// <param name="filePath">Путь к файлу, который требуется отправить.</param>
    /// <param name="ct">Токен отмены операции.</param>
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

        ArgumentOutOfRangeException.ThrowIfGreaterThan(fileSize, ProtocolConstants.MaxFileSize);

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

    /// <summary>
    /// Повторно открывает исходный файл и сверяет размер и SHA-256: файл мог измениться за время ожидания решения.
    /// </summary>
    /// <param name="ct">Токен отмены операции.</param>
    /// <returns>Поток чтения с позицией 0; вызывающий код обязан его освободить.</returns>
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
            if (
                (ulong)stream.Length != Transfer.FileSize
                || !(await SHA256.HashDataAsync(stream, ct).ConfigureAwait(false))
                    .AsSpan()
                    .SequenceEqual(Transfer.Sha256)
            )
            {
                throw new FileTransferException(
                    TransferError.SourceFileChanged,
                    "Source file has changed since the offer was prepared"
                );
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

        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(index, Transfer.ChunkCount);

        Transfer.NextIndex++;
    }
}
