namespace Protocol;

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
