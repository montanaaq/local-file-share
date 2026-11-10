using System;
using System.IO;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using Protocol.Messages;

namespace Protocol.Transfers.Files;

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

        ulong chunkSize = ProtocolConstants.ChunkSize;
        int chunkCount = (int)((fileSize + chunkSize - 1) / chunkSize);
        _receivedChunks = new bool[chunkCount];
    }

    /// <summary>
    /// Создаёт уникальный .part и таблицу принятых чанков для файла объявленного размера.
    /// </summary>
    /// <param name="fileSize">Ожидаемый размер всего файла в байтах.</param>
    /// <param name="directory">Папка для временного и итогового файла.</param>
    public static IncomingFileTransfer Create(ulong fileSize, string directory)
    {
        Directory.CreateDirectory(directory);

        string partPath = Path.Combine(directory, $"{Guid.NewGuid():N}.part");
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

    /// <summary>
    /// Синхронно записывает допустимый чанк по его смещению; повтор принятого чанка не изменяет файл.
    /// </summary>
    /// <param name="index">Индекс чанка, начиная с нуля.</param>
    /// <param name="data">Байты чанка; длина должна соответствовать оставшейся части файла.</param>
    /// <param name="nextIndex">Первый недостающий индекс после обработки, включая пропуски ранее принятых чанков.</param>
    /// <returns>false при неверном индексе, длине или закрытом файле.</returns>
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

    /// <summary>
    /// Проверяет наличие всех чанков, фактическую длину .part и SHA-256 перед сохранением.
    /// </summary>
    /// <param name="expectedSha256">32 байта SHA-256 из принятого FILE_OFFER.</param>
    /// <param name="ct">Токен отмены операции.</param>
    /// <returns>true, если все данные получены и хеш совпал.</returns>
    public async Task<bool> VerifyAsync(byte[] expectedSha256, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(expectedSha256);

        if (expectedSha256.Length != PayloadsConstants.Sha256Size)
        {
            throw new ArgumentException("SHA-256 must contain 32 bytes", nameof(expectedSha256));
        }

        await _fileLock.WaitAsync(ct).ConfigureAwait(false);

        try
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            _verified = false;

            if (NextIndex != (uint)_receivedChunks.Length)
            {
                return false;
            }

            await _stream.FlushAsync(ct);

            if ((ulong)_stream.Length != _fileSize)
            {
                return false;
            }

            _stream.Position = 0;

            byte[] actualSha256 = await SHA256.HashDataAsync(_stream, ct).ConfigureAwait(false);

            _verified = actualSha256.AsSpan().SequenceEqual(expectedSha256);

            return _verified;
        }
        finally
        {
            _fileLock.Release();
        }
    }

    /// <summary>
    /// После успешной VerifyAsync переименовывает .part, очищая имя по правилам Windows и разрешая коллизии суффиксом.
    /// </summary>
    /// <param name="fileName">Предложенное удалённым устройством имя файла; путь использоваться не будет.</param>
    /// <param name="ct">Токен отмены операции.</param>
    /// <returns>Путь к сохранённому файлу. Существующие файлы не перезаписываются.</returns>
    public async Task<string> SaveAsync(string fileName, CancellationToken ct)
    {
        string safeName = fileName.SanitizeFileName();
        await _fileLock.WaitAsync(ct).ConfigureAwait(false);

        try
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            if (!_verified)
            {
                throw new InvalidOperationException("File must be verified before saving");
            }

            string directory =
                Path.GetDirectoryName(_partPath)
                ?? throw new InvalidOperationException("Temporary file directory not found");

            ct.ThrowIfCancellationRequested();

            await _stream.DisposeAsync();
            _disposed = true;

            for (int number = 0; ; number++)
            {
                ct.ThrowIfCancellationRequested();

                string candidateName = safeName.WithFileSuffix(number);
                string destinationPath = Path.Combine(directory, candidateName);

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
        {
            return false;
        }

        long offset = (long)index * ProtocolConstants.ChunkSize;
        long remaining = (long)_fileSize - offset;
        int expectedLength = (int)Math.Min(ProtocolConstants.ChunkSize, remaining);

        if (data.Length != expectedLength)
        {
            return false;
        }

        if (_receivedChunks[(int)index])
        {
            return true;
        }

        _stream.Position = offset;

        _verified = false;

        _stream.Write(data);
        _receivedChunks[(int)index] = true;

        while (NextIndex < (uint)_receivedChunks.Length && _receivedChunks[(int)NextIndex])
        {
            NextIndex++;
        }

        return true;
    }
}
