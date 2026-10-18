using System;
using System.Threading.Tasks;

namespace Protocol;

// Access to an entry is protected by FileTransferService's state lock.
internal sealed class TransferEntry
{
    public FileTransfer Transfer { get; }
    public TaskCompletionSource<bool>? Answer { get; set; }
    public TaskCompletionSource<bool>? Result { get; set; }
    public IncomingFileTransfer? IncomingFile { get; set; }
    public OutgoingFileTransfer? OutgoingFile { get; private set; }
    public Task<bool>? IncomingCompletion { get; set; }

    public bool IsFinal =>
        Transfer.State
            is TransferState.Completed
                or TransferState.Rejected
                or TransferState.Failed
                or TransferState.Cancelled;

    public TransferEntry(FileTransfer transfer) => Transfer = transfer;

    public TransferEntry(OutgoingFileTransfer file)
    {
        Transfer = file.Transfer;
        OutgoingFile = file;
        Answer = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        Result = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    public void Require(TransferDirection direction, TransferState state)
    {
        if (Transfer.Direction != direction)
            throw new InvalidOperationException($"Transfer direction must be {direction}");
        if (Transfer.State != state)
            throw new InvalidOperationException($"Transfer state must be {state}");
    }

    public IncomingFileTransfer? ChangeState(TransferState state)
    {
        Transfer.State = state;
        if (!IsFinal)
            return null;

        OutgoingFile = null;
        // Keep a received answer available until its waiter consumes it, including a rejection.
        if (state != TransferState.Rejected)
        {
            Answer?.TrySetCanceled();
            Answer = null;
        }
        Result?.TrySetCanceled();
        var file = IncomingFile;
        IncomingFile = null;
        return file;
    }
}
