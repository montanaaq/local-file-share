namespace Protocol.Transfers.Models;

public enum TransferState
{
    WaitingForAnswer,
    Transferring,
    Paused,
    Verifying,
    Completed,
    Rejected,
    Failed,
    Cancelled,
}
