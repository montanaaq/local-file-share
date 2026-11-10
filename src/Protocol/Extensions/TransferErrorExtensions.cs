using System;
using System.IO;
using System.Net.Sockets;
using Protocol.Transfers.Files;
using Protocol.Transfers.Models;

namespace Protocol.Extensions;

public static class TransferErrorExtensions
{
    /// <summary>
    /// Определяет категорию ошибки по типу исключения, сохраняя отдельные причины файлового сценария.
    /// </summary>
    /// <param name="exception">Исходное исключение операции.</param>
    public static TransferError ToTransferError(this Exception exception)
    {
        ArgumentNullException.ThrowIfNull(exception);

        return exception switch
        {
            FileTransferException transferException => transferException.ErrorCode,
            OperationCanceledException => TransferError.None,
            TimeoutException => TransferError.Timeout,
            UnauthorizedAccessException => TransferError.AccessDenied,
            FileNotFoundException or DirectoryNotFoundException => TransferError.FileNotFound,
            SocketException => TransferError.Network,
            IOException => TransferError.FileIO,
            _ => TransferError.Unknown,
        };
    }
}
