using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Protocol;

internal sealed class FileTransferService : IAsyncDisposable
{
    private readonly Lock _stateLock = new();

    private readonly Dictionary<TransferKey, TransferEntry> _transfers = [];

    private bool _disposed;
    private readonly TimeProvider _timeProvider;

    public FileTransferService(TimeProvider? timeProvider = null) =>
        _timeProvider = timeProvider ?? TimeProvider.System;

    public event Action<FileTransfer>? FileOffered;
    public event Action<FileTransfer>? TransferChanged;
    public event Action<FileTransfer, Exception>? TransferFailed;

    /// <summary>
    /// Создаёт входящее предложение или восстанавливает известную передачу с совпадающими метаданными.
    /// </summary>
    /// <param name="payload">Тело FILE_OFFER с deviceId отправителя, fileId, размером, SHA-256 и именем.</param>
    /// <returns>Автоматическое решение и первый недостающий чанк либо null, если ответ не готов или сообщение не принято.</returns>
    public (bool Accepted, uint NextIndex)? HandleFileOffer(ReadOnlySpan<byte> payload)
    {
        if (Payloads.DecodeFileOffer(payload) is not { } offer)
        {
            return null;
        }

        var transfer = new FileTransfer
        {
            FileId = offer.FileId,
            PeerDeviceId = offer.DeviceId,
            FileName = offer.Name,
            FileSize = offer.Size,
            Sha256 = offer.Sha256,
            Direction = TransferDirection.Incoming,
            State = TransferState.WaitingForAnswer,
        };
        FileTransfer? snapshot;
        (bool Accepted, uint NextIndex)? response;
        bool isNew;

        lock (_stateLock)
        {
            if (_disposed)
            {
                return null;
            }

            isNew = !_transfers.TryGetValue(transfer.Key, out var existing);
            if (existing is not null)
            {
                response = HandleRepeatedFileOffer(existing, transfer, out snapshot);
            }
            else
            {
                var entry = new TransferEntry(transfer)
                {
                    LastActivity = _timeProvider.GetTimestamp(),
                };
                _transfers.Add(transfer.Key, entry);
                snapshot = Snapshot(entry);
                response = null;
            }
        }

        if (snapshot is not null)
        {
            if (isNew)
            {
                FileOffered?.Invoke(snapshot.Snapshot());
            }
            TransferChanged?.Invoke(snapshot);
        }
        return response;
    }

    /// <summary>
    /// Сверяет повторное предложение offer с записью existing и определяет автоматический ответ.
    /// Вызывается под блокировкой состояния; changed содержит снимок только при выходе из паузы.
    /// </summary>
    private (bool Accepted, uint NextIndex)? HandleRepeatedFileOffer(
        TransferEntry existing,
        FileTransfer offer,
        out FileTransfer? changed
    )
    {
        changed = null;
        var transfer = existing.Transfer;
        if (
            transfer.Direction != TransferDirection.Incoming
            || transfer.FileName != offer.FileName
            || transfer.FileSize != offer.FileSize
            || !transfer.Sha256.AsSpan().SequenceEqual(offer.Sha256)
        )
        {
            return (false, 0);
        }

        if (existing.IsFinal)
        {
            return transfer.State switch
            {
                TransferState.Cancelled => null,
                TransferState.Completed => (true, transfer.ChunkCount),
                _ => (false, 0),
            };
        }

        if (transfer.State == TransferState.WaitingForAnswer)
        {
            return null;
        }

        if (transfer.State == TransferState.Paused)
        {
            if (existing.IncomingFile is not null)
            {
                transfer.NextIndex = Math.Max(transfer.NextIndex, existing.IncomingFile.NextIndex);
            }
            existing.ChangeState(
                existing.IncomingFile is null
                    ? TransferState.WaitingForAnswer
                    : TransferState.Transferring,
                _timeProvider.GetTimestamp()
            );
            changed = Snapshot(existing);
        }

        return existing.IncomingFile is null && existing.IncomingCompletion is null
            ? null
            : (true, transfer.NextIndex);
    }

    public FileTransfer CreateOutgoingTransfer(
        Guid peerDeviceId,
        string fileName,
        ulong fileSize,
        byte[] sha256
    ) =>
        RegisterOutgoingTransfer(
            new OutgoingFileTransfer(peerDeviceId, fileName, fileSize, sha256)
        );

    private FileTransfer RegisterOutgoingTransfer(OutgoingFileTransfer file)
    {
        FileTransfer snapshot;
        lock (_stateLock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            var transfer = file.Transfer;
            var key = transfer.Key;
            var entry = new TransferEntry(file) { LastActivity = _timeProvider.GetTimestamp() };
            _transfers.Add(key, entry);
            snapshot = Snapshot(entry);
        }
        TransferChanged?.Invoke(snapshot.Snapshot());

        return snapshot;
    }

    public async Task<FileTransfer> PrepareOutgoingTransferAsync(
        Guid peerDeviceId,
        string filePath,
        CancellationToken ct
    )
    {
        ct.ThrowIfCancellationRequested();
        lock (_stateLock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
        }

        var file = await OutgoingFileTransfer
            .PrepareAsync(peerDeviceId, filePath, ct)
            .ConfigureAwait(false);
        return RegisterOutgoingTransfer(file);
    }

    public FileTransfer GetTransfer(TransferKey key)
    {
        lock (_stateLock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            return Snapshot(GetEntry(key));
        }
    }

    public FileTransfer[] GetTransfers()
    {
        lock (_stateLock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            return _transfers.Values.Select(Snapshot).ToArray();
        }
    }

    private FileTransfer Snapshot(TransferEntry entry)
    {
        var snapshot = entry.Transfer.Snapshot();
        snapshot.PauseRemaining = entry.PauseStarted is long started
            ? TimeSpan.FromTicks(
                Math.Max(
                    0,
                    (ProtocolConstants.PauseTimeout - _timeProvider.GetElapsedTime(started)).Ticks
                )
            )
            : null;

        return snapshot;
    }

    /// <summary>
    /// Меняет состояние незавершённой передачи и публикует снимок. Окончательное состояние сохраняется при запоздавшем завершении фоновой операции.
    /// </summary>
    /// <param name="key">Пара идентификаторов удалённого устройства и файла.</param>
    /// <param name="state">Новое состояние передачи.</param>
    /// <param name="error">Причина ошибки для снимка и события TransferFailed, если она есть.</param>
    internal void SetState(TransferKey key, TransferState state, Exception? error = null)
    {
        IncomingFileTransfer? file;
        FileTransfer snapshot;
        lock (_stateLock)
        {
            // An operation may finish unwinding after the service has been disposed.
            if (_disposed)
            {
                return;
            }

            var entry = GetEntry(key);
            if (entry.IsFinal)
            {
                return;
            }

            if (state == TransferState.Paused && entry.ReceivedResult is bool receivedResult)
            {
                state = receivedResult ? TransferState.Completed : TransferState.Failed;
                if (!receivedResult)
                {
                    error = new InvalidDataException("Receiver could not verify or save the file");
                }
            }

            file = entry.ChangeState(state, _timeProvider.GetTimestamp());
            entry.Transfer.Error = error?.Message;
            snapshot = Snapshot(entry);
        }
        try
        {
            file?.Dispose();
        }
        finally
        {
            TransferChanged?.Invoke(snapshot.Snapshot());
            if (state == TransferState.Failed && error is not null)
            {
                TransferFailed?.Invoke(snapshot, error);
            }
        }
    }

    /// <summary>
    /// Применяет решение к ожидающему входящему предложению и при согласии создаёт .part.
    /// </summary>
    /// <param name="key">Идентификаторы отправителя предложения и файла.</param>
    /// <param name="accepted">Решение пользователя о приёме файла.</param>
    /// <param name="directory">Папка для .part и итогового файла при согласии.</param>
    /// <returns>Снимок состояния после решения.</returns>
    public FileTransfer SetIncomingDecision(TransferKey key, bool accepted, string directory)
    {
        TransferEntry entry;
        FileTransfer transfer;
        lock (_stateLock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            entry = GetEntry(key);
            RequirePendingDecision(entry);
            transfer = entry.Transfer;

            if (!accepted)
            {
                entry.ChangeState(TransferState.Rejected, _timeProvider.GetTimestamp());
                TransferChanged?.Invoke(Snapshot(entry));
                return transfer.Snapshot();
            }
        }
        var incomingFile = IncomingFileTransfer.Create(transfer.FileSize, directory);

        try
        {
            lock (_stateLock)
            {
                ObjectDisposedException.ThrowIf(_disposed, this);

                RequirePendingDecision(entry);

                entry.IncomingFile = incomingFile;
                entry.ChangeState(TransferState.Transferring, _timeProvider.GetTimestamp());
                if (entry.PauseStarted is not null)
                {
                    entry.Transfer.ResumeCount++;
                }
                entry.PauseStarted = null;
                var snapshot = Snapshot(entry);
                TransferChanged?.Invoke(snapshot.Snapshot());
                return snapshot;
            }
        }
        catch
        {
            incomingFile.Dispose();
            throw;
        }
    }

    /// <summary>
    /// Применяет решение получателя и проверяет nextIndex перед отправкой новых данных или возобновлением.
    /// </summary>
    /// <param name="payload">Тело FILE_ANSWER с идентификаторами получателя и файла, решением и первым недостающим индексом.</param>
    /// <returns>true, если ответ соответствует ожидающей исходящей передаче и может быть подтверждён ACK.</returns>
    public bool HandleFileAnswer(ReadOnlySpan<byte> payload)
    {
        var answer = Payloads.DecodeFileAnswer(payload);
        if (answer is null)
        {
            return false;
        }

        lock (_stateLock)
        {
            if (_disposed)
            {
                return false;
            }

            var key = new TransferKey(answer.Value.DeviceId, answer.Value.FileId);
            if (!_transfers.TryGetValue(key, out var entry))
            {
                return false;
            }

            var transfer = entry.Transfer;
            if (transfer.Direction != TransferDirection.Outgoing)
            {
                return false;
            }

            if (
                transfer.State != TransferState.WaitingForAnswer
                && !(transfer.State == TransferState.Paused && entry.Resuming)
            )
            {
                return false;
            }

            var completion = entry.Answer;
            if (completion is null)
            {
                return false;
            }

            if (!answer.Value.Accepted)
            {
                entry.ChangeState(TransferState.Rejected, _timeProvider.GetTimestamp());
                completion.TrySetResult(false);
                TransferChanged?.Invoke(Snapshot(entry));
                return true;
            }

            if (
                answer.Value.NextIndex > transfer.ChunkCount
                || (!entry.Resuming && answer.Value.NextIndex != 0)
            )
            {
                return false;
            }

            transfer.NextIndex = answer.Value.NextIndex;
            entry.ChangeState(TransferState.Transferring, _timeProvider.GetTimestamp());
            entry.Resuming = false;
            completion.TrySetResult(true);
            TransferChanged?.Invoke(Snapshot(entry));
        }
        return true;
    }

    /// <summary>
    /// Ожидает решение получателя, сохраняя уже полученный ответ при одновременной отмене.
    /// </summary>
    /// <param name="key">Идентификаторы получателя предложения и файла.</param>
    /// <param name="ct">Токен отмены операции.</param>
    /// <param name="resuming">true использует срок пробного запроса и сохраняет паузу при неудаче.</param>
    /// <returns>true при согласии, false при отказе; отмена и истечение ожидания приводят к исключению.</returns>
    public async Task<bool> WaitForFileAnswerAsync(
        TransferKey key,
        CancellationToken ct,
        bool resuming = false
    )
    {
        TransferEntry entry;
        TaskCompletionSource<bool> completion;

        lock (_stateLock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            entry = GetEntry(key);
            completion =
                entry.Answer
                ?? throw new InvalidOperationException("Transfer is not waiting for an answer");
        }
        try
        {
            return await completion.Task.WaitAsync(
                resuming ? ProtocolConstants.ProbeInterval : ProtocolConstants.FileAnswerTimeout,
                _timeProvider,
                ct
            );
        }
        catch (TimeoutException exception)
        {
            if (resuming)
            {
                lock (_stateLock)
                {
                    if (completion.Task.IsCompletedSuccessfully)
                    {
                        return completion.Task.Result;
                    }
                    SetState(key, TransferState.Paused);
                }
                throw;
            }
            bool? answer = StopWaiting(
                entry,
                completion,
                expectedState: TransferState.WaitingForAnswer,
                state: TransferState.Failed,
                exception
            );
            if (answer.HasValue)
            {
                return answer.Value;
            }

            throw;
        }
        catch (OperationCanceledException)
        {
            if (resuming)
            {
                lock (_stateLock)
                {
                    if (completion.Task.IsCompletedSuccessfully)
                    {
                        return completion.Task.Result;
                    }
                    SetState(key, TransferState.Paused);
                }
                throw;
            }
            bool? answer = StopWaiting(
                entry,
                completion,
                expectedState: TransferState.WaitingForAnswer,
                state: TransferState.Cancelled
            );
            if (answer.HasValue)
            {
                return answer.Value;
            }

            throw;
        }
        finally
        {
            lock (_stateLock)
            {
                if (entry.Answer == completion)
                {
                    entry.Answer = null;
                }
            }
        }
    }

    /// <summary>
    /// Ожидает результат проверки получателем; учитывает результат, пришедший до начала ожидания.
    /// </summary>
    /// <param name="key">Пара идентификаторов удалённого устройства и файла.</param>
    /// <param name="ct">Токен отмены операции.</param>
    /// <returns>Результат SHA-256 и сохранения файла на стороне получателя.</returns>
    internal async Task<bool> WaitForFileResultAsync(TransferKey key, CancellationToken ct)
    {
        TransferEntry entry;
        TaskCompletionSource<bool> completion;
        lock (_stateLock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            entry = GetEntry(key);
            if (entry.ReceivedResult is bool received && entry.IsFinal)
            {
                return received;
            }
            entry.Require(TransferDirection.Outgoing, TransferState.Transferring);

            completion =
                entry.Result
                ?? throw new InvalidOperationException("Transfer is not waiting for a result");
        }
        bool ok;
        try
        {
            ok = await completion.Task.WaitAsync(
                ProtocolConstants.FileResultTimeout,
                _timeProvider,
                ct
            );
        }
        catch (TimeoutException)
        {
            bool? received = StopWaiting(
                entry,
                completion,
                expectedState: TransferState.Transferring,
                state: TransferState.Paused
            );
            if (received.HasValue)
            {
                ok = received.Value;
            }
            else
            {
                throw;
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            bool? received = StopWaiting(
                entry,
                completion,
                TransferState.Transferring,
                TransferState.Cancelled
            );
            if (received.HasValue)
            {
                ok = received.Value;
            }
            else
            {
                throw;
            }
        }
        finally
        {
            lock (_stateLock)
            {
                if (entry.Result == completion)
                {
                    entry.Result = null;
                }
            }
        }
        if (ok)
        {
            SetState(key, TransferState.Completed);
        }
        else
        {
            SetState(
                key,
                TransferState.Failed,
                new InvalidDataException("Receiver could not verify or save the file")
            );
        }

        return ok;
    }

    /// <summary>
    /// Для записи entry завершает ожидание completion переходом из expectedState в state и публикует error, если он задан.
    /// Если ответ уже пришёл, возвращает его без изменения состояния; иначе возвращает null.
    /// </summary>
    private bool? StopWaiting(
        TransferEntry entry,
        TaskCompletionSource<bool> completion,
        TransferState expectedState,
        TransferState state,
        Exception? error = null
    )
    {
        FileTransfer snapshot;
        lock (_stateLock)
        {
            if (completion.Task.IsCompletedSuccessfully)
            {
                return completion.Task.Result;
            }

            if (_disposed || entry.Transfer.State != expectedState)
            {
                return null;
            }

            entry.ChangeState(state, _timeProvider.GetTimestamp());
            snapshot = Snapshot(entry);
        }
        TransferChanged?.Invoke(snapshot.Snapshot());
        if (error is not null)
        {
            TransferFailed?.Invoke(snapshot, error);
        }

        return null;
    }

    /// <summary>
    /// Проверяет индекс и точную длину чанка относительно файла, записывает новые данные и обновляет прогресс.
    /// Повтор уже принятого чанка не записывается заново.
    /// </summary>
    /// <param name="payload">Тело CHUNK с идентификаторами отправителя и файла, индексом и данными.</param>
    /// <returns>true, если сообщение допустимо и его можно подтвердить ACK.</returns>
    public bool HandleChunk(ReadOnlySpan<byte> payload)
    {
        var chunk = Payloads.DecodeChunk(payload);
        if (chunk is null)
        {
            return false;
        }

        var key = new TransferKey(chunk.Value.DeviceId, chunk.Value.FileId);
        TransferEntry entry;
        IncomingFileTransfer incomingFile;
        lock (_stateLock)
        {
            if (_disposed || !_transfers.TryGetValue(key, out var value))
            {
                return false;
            }

            entry = value;
            if (entry.Transfer.Direction != TransferDirection.Incoming)
            {
                return false;
            }

            if (entry.Transfer.State is TransferState.Verifying or TransferState.Completed)
            {
                if (chunk.Value.Index >= entry.Transfer.NextIndex)
                {
                    return false;
                }

                ulong remaining =
                    entry.Transfer.FileSize
                    - (ulong)chunk.Value.Index * (ulong)ProtocolConstants.ChunkSize;
                return chunk.Value.Data.Length
                    == (int)Math.Min((ulong)ProtocolConstants.ChunkSize, remaining);
            }

            if (entry.Transfer.State != TransferState.Transferring || entry.IncomingFile is null)
            {
                return false;
            }

            incomingFile = entry.IncomingFile;
        }
        try
        {
            if (
                !incomingFile.TryWriteChunk(chunk.Value.Index, chunk.Value.Data, out uint nextIndex)
            )
            {
                return false;
            }

            lock (_stateLock)
            {
                if (
                    _disposed
                    || entry.IncomingFile != incomingFile
                    || entry.Transfer.State != TransferState.Transferring
                )
                {
                    return false;
                }

                entry.Transfer.NextIndex = Math.Max(entry.Transfer.NextIndex, nextIndex);
                entry.LastActivity = _timeProvider.GetTimestamp();
                if (entry.PauseStarted is not null)
                {
                    entry.Transfer.ResumeCount++;
                }
                entry.PauseStarted = null;
                TransferChanged?.Invoke(Snapshot(entry));
                return true;
            }
        }
        catch (IOException exception)
        {
            SetState(key, TransferState.Failed, exception);
            return false;
        }
    }

    /// <summary>
    /// Сохраняет результат проверки получателем и завершает ожидающую либо приостановленную исходящую передачу.
    /// </summary>
    /// <param name="payload">Тело FILE_RESULT с идентификаторами получателя и файла и признаком успешного сохранения.</param>
    /// <returns>true для допустимого результата или его совпадающего повтора; противоречащий результат отклоняется.</returns>
    public bool HandleFileResult(ReadOnlySpan<byte> payload)
    {
        var fileResult = Payloads.DecodeFileResult(payload);

        if (fileResult is null)
        {
            return false;
        }

        lock (_stateLock)
        {
            var key = new TransferKey(fileResult.Value.DeviceId, fileResult.Value.FileId);

            if (_disposed || !_transfers.TryGetValue(key, out var entry))
            {
                return false;
            }

            if (entry.Transfer.Direction != TransferDirection.Outgoing)
            {
                return false;
            }

            if (entry.ReceivedResult.HasValue)
            {
                return entry.ReceivedResult.Value == fileResult.Value.Ok;
            }

            if (entry.Transfer.State is not (TransferState.Transferring or TransferState.Paused))
            {
                return false;
            }

            if (entry.Transfer.State == TransferState.Paused)
            {
                entry.Result = new TaskCompletionSource<bool>(
                    TaskCreationOptions.RunContinuationsAsynchronously
                );
            }
            var completion = entry.Result;
            if (completion is null)
            {
                return false;
            }

            if (!completion.TrySetResult(fileResult.Value.Ok))
            {
                return false;
            }

            entry.ReceivedResult = fileResult.Value.Ok;
            if (entry.Transfer.State == TransferState.Paused)
            {
                entry.ChangeState(
                    fileResult.Value.Ok ? TransferState.Completed : TransferState.Failed,
                    _timeProvider.GetTimestamp()
                );
                if (!fileResult.Value.Ok)
                {
                    var error = new InvalidDataException(
                        "Receiver could not verify or save the file"
                    );
                    entry.Transfer.Error = error.Message;
                    TransferFailed?.Invoke(Snapshot(entry), error);
                }
                TransferChanged?.Invoke(Snapshot(entry));
            }
            return true;
        }
    }

    /// <summary>
    /// Запускает проверку принятого файла или возвращает ранее начатую проверку для повторного FILE_DONE.
    /// </summary>
    /// <param name="payload">Тело FILE_DONE с идентификаторами отправителя и файла.</param>
    /// <param name="ct">Токен отмены операции.</param>
    /// <returns>Задача проверки и сохранения либо null, если завершение недопустимо.</returns>
    internal Task<bool>? HandleFileDone(ReadOnlySpan<byte> payload, CancellationToken ct)
    {
        var fileDone = Payloads.DecodeFileDone(payload);
        if (fileDone is null)
        {
            return null;
        }

        var key = new TransferKey(fileDone.Value.DeviceId, fileDone.Value.FileId);
        lock (_stateLock)
        {
            if (!_transfers.TryGetValue(key, out var entry))
            {
                return null;
            }

            if (entry.Transfer.Direction != TransferDirection.Incoming)
            {
                return null;
            }

            if (entry.IncomingCompletion is not null)
            {
                return entry.IncomingCompletion;
            }

            if (entry.Transfer.State != TransferState.Transferring || entry.IncomingFile == null)
            {
                return null;
            }

            return CompleteIncomingTransferAsync(key, ct);
        }
    }

    /// <summary>
    /// Однократно запускает фоновую проверку SHA-256 и сохранение входящей передачи; повторные вызовы разделяют задачу.
    /// </summary>
    /// <param name="key">Пара идентификаторов удалённого устройства и файла.</param>
    /// <param name="ct">Токен отмены операции.</param>
    /// <returns>Задача с результатом проверки и сохранения файла.</returns>
    internal Task<bool> CompleteIncomingTransferAsync(TransferKey key, CancellationToken ct)
    {
        lock (_stateLock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            var entry = GetEntry(key);

            if (entry.Transfer.Direction != TransferDirection.Incoming)
            {
                throw new InvalidOperationException("Transfer must be incoming");
            }

            if (entry.IncomingCompletion is not null)
            {
                return entry.IncomingCompletion;
            }

            entry.Require(TransferDirection.Incoming, TransferState.Transferring);

            var incomingFile =
                entry.IncomingFile
                ?? throw new InvalidOperationException("Transfer has no incoming file");
            byte[] expectedSha256 = entry.Transfer.Sha256;
            string fileName = entry.Transfer.FileName;

            if (entry.PauseStarted is not null)
            {
                entry.Transfer.ResumeCount++;
            }
            entry.ChangeState(TransferState.Verifying, _timeProvider.GetTimestamp());
            TransferChanged?.Invoke(Snapshot(entry));
            entry.IncomingCompletion = Task.Run(() =>
                CompleteIncomingTransferCoreAsync(key, incomingFile, expectedSha256, fileName, ct)
            );

            return entry.IncomingCompletion;
        }
    }

    private async Task<bool> CompleteIncomingTransferCoreAsync(
        TransferKey key,
        IncomingFileTransfer incomingFile,
        byte[] expectedSha256,
        string fileName,
        CancellationToken ct
    )
    {
        try
        {
            bool verified = await incomingFile
                .VerifyAsync(expectedSha256, ct)
                .ConfigureAwait(false);
            if (!verified)
            {
                SetState(
                    key,
                    TransferState.Failed,
                    new InvalidDataException("Incoming file verification failed")
                );
                return false;
            }
            string savedPath = await incomingFile.SaveAsync(fileName, ct).ConfigureAwait(false);
            lock (_stateLock)
            {
                if (!_disposed)
                {
                    GetEntry(key).Transfer.SavedPath = savedPath;
                }
            }
            SetState(key, TransferState.Completed);
            return true;
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            SetState(key, TransferState.Cancelled);
            throw;
        }
        catch (Exception exception) when (exception is IOException or UnauthorizedAccessException)
        {
            SetState(key, TransferState.Failed, exception);
            return false;
        }
        catch (Exception exception)
        {
            SetState(key, TransferState.Failed, exception);
            throw;
        }
    }

    internal async Task<FileStream> OpenOutgoingFileAsync(TransferKey key, CancellationToken ct)
    {
        OutgoingFileTransfer file;
        lock (_stateLock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            var entry = GetEntry(key);
            entry.Require(TransferDirection.Outgoing, TransferState.Transferring);
            file =
                entry.OutgoingFile
                ?? throw new InvalidOperationException("Transfer has no outgoing file");
        }

        var stream = await file.OpenAsync(ct).ConfigureAwait(false);
        try
        {
            lock (_stateLock)
            {
                ObjectDisposedException.ThrowIf(_disposed, this);

                GetEntry(key).Require(TransferDirection.Outgoing, TransferState.Transferring);
            }
            return stream;
        }
        catch
        {
            await stream.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    internal void ConfirmOutgoingChunk(TransferKey key, uint index)
    {
        lock (_stateLock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            var entry = GetEntry(key);
            entry.Require(TransferDirection.Outgoing, TransferState.Transferring);
            var file =
                entry.OutgoingFile
                ?? throw new InvalidOperationException("Transfer has no outgoing file");
            file.ConfirmChunk(index);
            entry.LastActivity = _timeProvider.GetTimestamp();
            TransferChanged?.Invoke(Snapshot(entry));
        }
    }

    /// <summary>
    /// Для исходящей передачи в паузе подготавливает новые ожидания решения и результата без сброса срока паузы.
    /// </summary>
    /// <param name="key">Пара идентификаторов удалённого устройства и файла.</param>
    /// <returns>false для завершённой передачи, другого состояния или уже запущенной пробы.</returns>
    internal bool PrepareResume(TransferKey key)
    {
        lock (_stateLock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            var entry = GetEntry(key);
            if (
                entry.Transfer.Direction != TransferDirection.Outgoing
                || entry.Transfer.State != TransferState.Paused
                || entry.Resuming
            )
            {
                return false;
            }
            entry.Answer = new TaskCompletionSource<bool>(
                TaskCreationOptions.RunContinuationsAsynchronously
            );
            entry.Result = new TaskCompletionSource<bool>(
                TaskCreationOptions.RunContinuationsAsynchronously
            );
            if (entry.ReceivedResult is bool result)
            {
                entry.Result.TrySetResult(result);
            }
            entry.Resuming = true;
            return true;
        }
    }

    public void PauseAll()
    {
        TransferKey[] keys;
        lock (_stateLock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            keys = _transfers
                .Where(pair =>
                    !pair.Value.IsFinal && pair.Value.Transfer.State != TransferState.Verifying
                )
                .Select(pair => pair.Key)
                .ToArray();
            foreach (var key in keys)
            {
                SetState(key, TransferState.Paused);
            }
        }
    }

    public void CancelPaused(TransferKey key)
    {
        lock (_stateLock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            var entry = GetEntry(key);
            entry.Require(entry.Transfer.Direction, TransferState.Paused);
            SetState(key, TransferState.Cancelled);
        }
    }

    /// <summary>
    /// Проверяет истёкшие сроки решения, приёма чанков и паузы по монотонному времени.
    /// После истечения паузы завершает передачу с ошибкой и удаляет входящий .part.
    /// </summary>
    public void CheckTimeouts()
    {
        List<(TransferKey Key, TransferState State, Exception? Error)> changes = [];
        lock (_stateLock)
        {
            if (_disposed)
            {
                return;
            }
            foreach (var pair in _transfers)
            {
                var entry = pair.Value;
                if (entry.IsFinal || entry.Transfer.State == TransferState.Verifying)
                {
                    continue;
                }
                if (
                    entry.PauseStarted is long started
                    && _timeProvider.GetElapsedTime(started) >= ProtocolConstants.PauseTimeout
                )
                {
                    changes.Add(
                        (
                            pair.Key,
                            TransferState.Failed,
                            new TimeoutException("Transfer pause timed out")
                        )
                    );
                }
                else if (entry.Transfer.Direction == TransferDirection.Incoming)
                {
                    var idle = _timeProvider.GetElapsedTime(entry.LastActivity);
                    if (
                        entry.Transfer.State == TransferState.Transferring
                        && idle >= ProtocolConstants.IncomingChunkTimeout
                    )
                    {
                        changes.Add((pair.Key, TransferState.Paused, null));
                    }
                    else if (
                        entry.Transfer.State == TransferState.WaitingForAnswer
                        && entry.PauseStarted is null
                        && idle >= ProtocolConstants.FileAnswerTimeout
                    )
                    {
                        changes.Add(
                            (
                                pair.Key,
                                TransferState.Failed,
                                new TimeoutException("File offer decision timed out")
                            )
                        );
                    }
                }
            }
            foreach (var change in changes)
            {
                SetState(change.Key, change.State, change.Error);
            }
        }
    }

    private static void RequirePendingDecision(TransferEntry entry)
    {
        if (
            entry.Transfer.Direction != TransferDirection.Incoming
            || entry.IncomingFile is not null
            || entry.Transfer.State is not (TransferState.WaitingForAnswer or TransferState.Paused)
        )
        {
            throw new InvalidOperationException(
                "Transfer is not waiting for an incoming file decision"
            );
        }
    }

    private TransferEntry GetEntry(TransferKey key) =>
        _transfers.TryGetValue(key, out var entry)
            ? entry
            : throw new ArgumentException("Transfer not found", nameof(key));

    public async ValueTask DisposeAsync()
    {
        List<IncomingFileTransfer> files;
        List<Task<bool>> incomingCompletions = [];
        lock (_stateLock)
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            files = [];
            foreach (var entry in _transfers.Values)
            {
                entry.Answer?.TrySetCanceled();
                entry.Result?.TrySetCanceled();

                if (entry.IncomingFile is not null)
                {
                    files.Add(entry.IncomingFile);
                }

                if (entry.IncomingCompletion is not null)
                {
                    incomingCompletions.Add(entry.IncomingCompletion);
                }
            }
            _transfers.Clear();
        }
        var errors = new List<Exception>();

        foreach (var completion in incomingCompletions)
        {
            try
            {
                await completion.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                continue;
            }
            catch (Exception exception)
            {
                errors.Add(exception);
            }
        }

        foreach (var file in files)
        {
            try
            {
                await file.DisposeAsync();
            }
            catch (Exception exception)
            {
                errors.Add(exception);
            }
        }

        if (errors.Count > 0)
        {
            throw new AggregateException("Failed to delete temporary files", errors);
        }
    }
}
