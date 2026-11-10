namespace Protocol.Transfers.Models;

public enum TransferError
{
    None,
    Timeout,
    SourceFileChanged,
    IntegrityCheckFailed,
    ReceiverSaveFailed,
    AccessDenied,
    FileNotFound,
    FileIO,
    Network,
    Unknown,
}
