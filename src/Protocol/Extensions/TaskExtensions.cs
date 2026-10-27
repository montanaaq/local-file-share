using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Protocol;

internal static class TaskExtensions
{
    /// <summary>
    /// Добавляет завершение операции в набор, чтобы при закрытии дождаться её очистки ресурсов.
    /// Ошибку исходной задачи получает её вызывающий код; набор хранит только факт завершения.
    /// Вызывающий код должен защищать набор своей блокировкой.
    /// </summary>
    /// <param name="completions">Набор завершений, ожидаемых при закрытии владельца.</param>
    /// <param name="operation">Исходная задача, которую вызывающий код ожидает для получения результата или ошибки.</param>
    internal static void TrackCompletion(this HashSet<Task> completions, Task operation)
    {
        completions.RemoveWhere(task => task.IsCompleted);
        completions.Add(
            operation.ContinueWith(
                _ => { },
                CancellationToken.None,
                TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default
            )
        );
    }
}
