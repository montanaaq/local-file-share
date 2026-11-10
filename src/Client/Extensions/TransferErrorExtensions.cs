using System;
using System.Net.Sockets;
using Protocol.Extensions;
using Protocol.Transfers.Models;

namespace Client;

internal static class TransferErrorExtensions
{
    internal static string ToUserMessage(this TransferError error) =>
        error switch
        {
            TransferError.None => string.Empty,
            TransferError.Timeout =>
                "Время ожидания истекло. Проверьте связь с другим устройством и повторите передачу.",
            TransferError.SourceFileChanged =>
                "Файл изменился после выбора. Выберите его заново и повторите отправку.",
            TransferError.IntegrityCheckFailed =>
                "Полученный файл повреждён и не сохранён. Повторите передачу.",
            TransferError.ReceiverSaveFailed =>
                "Получатель не смог проверить или сохранить файл. Попросите его проверить папку приёма и повторите передачу.",
            TransferError.AccessDenied =>
                "Нет доступа к файлу или папке. Проверьте права доступа или выберите другую папку.",
            TransferError.FileNotFound => "Файл или папка больше не доступны. Выберите их заново.",
            TransferError.FileIO =>
                "Не удалось прочитать или сохранить файл. Проверьте доступ к диску и свободное место.",
            TransferError.Network =>
                "Не удалось связаться с другим устройством. Проверьте подключение к локальной сети.",
            _ => "Не удалось выполнить передачу. Попробуйте повторить её.",
        };

    internal static string ToUserMessage(this Exception exception) =>
        exception switch
        {
            SocketException { SocketErrorCode: SocketError.AddressAlreadyInUse } =>
                "Выбранный порт уже занят. Укажите другой порт в настройках.",
            _ => exception.ToTransferError().ToUserMessage(),
        };
}
