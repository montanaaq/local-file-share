using System.IO;
using Protocol.Transfers.Models;

namespace Protocol.Transfers.Files;

internal sealed class FileTransferException : IOException
{
    public TransferError ErrorCode { get; }

    public FileTransferException(TransferError errorCode, string message)
        : base(message)
    {
        ErrorCode = errorCode;
    }
}
