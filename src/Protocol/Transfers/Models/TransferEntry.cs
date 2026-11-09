using System;
using System.Threading.Tasks;

namespace Protocol;

internal sealed class TransferEntry
{
    public FileTransfer Transfer { get; }
    public TaskCompletionSource<bool>? Answer { get; set; }
    public TaskCompletionSource<bool>? Result { get; set; }
    public IncomingFileTransfer? IncomingFile { get; set; }
    public OutgoingFileTransfer? OutgoingFile { get; private set; }
    public Task<bool>? IncomingCompletion { get; set; }
    public bool? ReceivedResult { get; set; }
    public long LastActivity { get; set; }
    public long? LastProgressNotification { get; set; }
    public long? PauseStarted { get; set; }
    public bool Resuming { get; set; }

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
        {
            throw new InvalidOperationException($"Transfer direction must be {direction}");
        }

        if (Transfer.State != state)
        {
            throw new InvalidOperationException($"Transfer state must be {state}");
        }
    }

    /// <summary>
    /// Устанавливает state и согласует ожидания ответа, счётчики паузы и файловые ресурсы.
    /// timestamp — монотонная отметка времени; возвращённый входящий файл должен освободить вызывающий код.
    /// </summary>
    public IncomingFileTransfer? ChangeState(TransferState state, long timestamp = 0)
    {
        if (state == TransferState.Paused)
        {
            if (PauseStarted is null)
            {
                PauseStarted = timestamp;
                Transfer.PauseCount++;
            }
            Resuming = false;
            Answer?.TrySetCanceled();
            Result?.TrySetCanceled();
        }
        else if (state == TransferState.Transferring)
        {
            if (
                Transfer.State == TransferState.Paused
                && Transfer.Direction == TransferDirection.Outgoing
            )
            {
                Transfer.ResumeCount++;
            }
            LastActivity = timestamp;
            if (Transfer.Direction == TransferDirection.Outgoing)
            {
                PauseStarted = null;
            }
        }
        else if (state == TransferState.Verifying)
        {
            PauseStarted = null;
        }
        Transfer.State = state;
        if (!IsFinal)
        {
            return null;
        }

        OutgoingFile = null;
        PauseStarted = null;
        Resuming = false;

        if (state != TransferState.Rejected)
        {
            Answer?.TrySetCanceled();
        }
        Result?.TrySetCanceled();
        var file = IncomingFile;
        IncomingFile = null;

        return file;
    }
}
