using System;

namespace Protocol;

/// <summary>
/// Transfer snapshots and notifications from a background worker.
/// UI subscribers must marshal updates to their UI thread.
/// </summary>
public interface ITransferSource
{
    FileTransfer GetTransfer(Guid peerDeviceId, Guid fileId);
    event Action<FileTransfer>? FileOffered;
    event Action<FileTransfer, Exception>? TransferFailed;
}
