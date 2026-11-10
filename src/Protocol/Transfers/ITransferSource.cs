using System;
using Protocol.Transfers.Models;

namespace Protocol.Transfers;

/// <summary>
/// Снимки передач и события из фонового обработчика.
/// Подписчики UI должны переключать обработку на поток интерфейса.
/// </summary>
public interface ITransferSource
{
    FileTransfer GetTransfer(TransferKey key);
    FileTransfer[] GetTransfers();
    event Action<FileTransfer>? FileOffered;
    event Action<FileTransfer>? TransferChanged;
    event Action<FileTransfer, Exception>? TransferFailed;
}
