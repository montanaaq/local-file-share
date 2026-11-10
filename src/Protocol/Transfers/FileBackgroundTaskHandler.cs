using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Protocol.Extensions;
using Protocol.Messages;
using Protocol.Transfers.Models;

namespace Protocol.Transfers;

internal sealed class FileBackgroundTaskHandler
{
    private readonly Lock _stateLock = new();
    private readonly HashSet<Task> _backgroundTasks = [];
    private readonly HashSet<Task> _operationCompletions = [];
    private readonly HashSet<(TransferKey Key, MessageType Type)> _activeReplies = [];

    private bool _stopped;

    /// <summary>
    /// Запускает операцию, результат и ошибку которой ожидает вызывающий код.
    /// При закрытии обработчик ожидает только завершение её очистки ресурсов.
    /// </summary>
    /// <param name="operation">Файловая операция с уже зафиксированным токеном обмена.</param>
    public Task<FileTransfer> StartOperation(Func<Task<FileTransfer>> operation)
    {
        lock (_stateLock)
        {
            ObjectDisposedException.ThrowIf(_stopped, this);

            var task = Task.Run(operation);
            _operationCompletions.TrackCompletion(task);
            return task;
        }
    }

    /// <summary>
    /// Запускает фоновую операцию, ошибки которой наблюдаются при обслуживании обмена.
    /// Делегат запускается и при отменённом сетевом токене, чтобы выполнился его finally.
    /// </summary>
    /// <param name="operation">Операция, самостоятельно обрабатывающая отмену своей сессии.</param>
    public void Start(Func<Task> operation)
    {
        lock (_stateLock)
        {
            if (_stopped)
            {
                return;
            }

            _backgroundTasks.Add(Task.Run(operation));
        }
    }

    /// <summary>
    /// Запускает не более одного одновременного ответа типа type для передачи key.
    /// После завершения ответа разрешает повторный запуск, включая случай ошибки или отмены.
    /// </summary>
    /// <param name="key">Идентификатор передачи.</param>
    /// <param name="type">Тип фонового ответа.</param>
    /// <param name="send">Отправка ответа с обработкой ожидаемых сетевых ошибок.</param>
    public void StartReply(TransferKey key, MessageType type, Func<Task> send)
    {
        lock (_stateLock)
        {
            if (_stopped || !_activeReplies.Add((key, type)))
            {
                return;
            }

            _backgroundTasks.Add(
                Task.Run(async () =>
                {
                    try
                    {
                        await send().ConfigureAwait(false);
                    }
                    finally
                    {
                        lock (_stateLock)
                        {
                            _activeReplies.Remove((key, type));
                        }
                    }
                })
            );
        }
    }

    public Task ObserveCompletedAsync(CancellationToken ct) =>
        ObserveAsync(completedOnly: true, ct);

    public Task WaitForPendingAsync(CancellationToken ct) => ObserveAsync(completedOnly: false, ct);

    private async Task ObserveAsync(bool completedOnly, CancellationToken ct)
    {
        Task[] tasks;

        lock (_stateLock)
        {
            tasks = _backgroundTasks.Where(task => !completedOnly || task.IsCompleted).ToArray();

            foreach (var task in tasks)
            {
                _backgroundTasks.Remove(task);
            }
        }

        try
        {
            await Task.WhenAll(tasks).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested) { }
    }

    /// <summary>
    /// Запрещает новые задачи и ожидает завершения операций и фоновых ответов.
    /// Владелец должен заранее отменить сетевые операции; ct разрешает ожидаемую отмену задач.
    /// </summary>
    /// <param name="ct">Отменённый токен закрытия владельца.</param>
    public async Task StopAsync(CancellationToken ct)
    {
        Task[] operations;

        lock (_stateLock)
        {
            _stopped = true;
            operations = _operationCompletions.ToArray();
        }

        await Task.WhenAll(operations).ConfigureAwait(false);
        await WaitForPendingAsync(ct).ConfigureAwait(false);
    }
}
