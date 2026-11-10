using System;
using Protocol.Transfers.Models;

namespace Client;

public sealed record TransferItem(FileTransfer Transfer)
{
    public TransferKey Key => Transfer.Key;

    public string FileName => Transfer.FileName;

    public string DirectionText =>
        Transfer.Direction == TransferDirection.Incoming ? "Получение" : "Отправка";

    public double Progress =>
        Transfer.FileSize == 0
            ? Transfer.State == TransferState.Completed
                ? 100
                : 0
            : 100.0 * Transfer.TransferredBytes / Transfer.FileSize;

    public string ProgressText =>
        $"{Transfer.TransferredBytes.ToFileSize()} из {Transfer.FileSize.ToFileSize()} · {Progress:0}%";

    public string StateText =>
        Transfer.State switch
        {
            TransferState.WaitingForAnswer => "Ожидание решения",
            TransferState.Transferring => "Передаётся",
            TransferState.Paused => "На паузе",
            TransferState.Verifying => "Проверка файла",
            TransferState.Completed => "Завершено",
            TransferState.Rejected => "Отклонено",
            TransferState.Failed => "Ошибка",
            TransferState.Cancelled => "Отменено",
            _ => "Неизвестное состояние",
        };

    public bool IsPaused => Transfer.State == TransferState.Paused;

    public string ErrorText =>
        Transfer.State == TransferState.Failed ? Transfer.ErrorCode.ToUserMessage() : string.Empty;

    public string PauseTimeText
    {
        get
        {
            if (Transfer.PauseRemaining is not { } remaining)
            {
                return string.Empty;
            }

            double seconds = Math.Ceiling(Math.Max(0, remaining.TotalSeconds));

            return TimeSpan.FromSeconds(seconds).ToString(@"mm\:ss");
        }
    }
}
