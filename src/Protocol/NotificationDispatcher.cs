using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace Protocol;

internal sealed class NotificationDispatcher : IAsyncDisposable
{
    private readonly Channel<Action> _queue = Channel.CreateUnbounded<Action>(
        new UnboundedChannelOptions { SingleReader = true }
    );
    private readonly Task _worker;

    public NotificationDispatcher() => _worker = Task.Run(DispatchAsync);

    public event Action<Exception>? NotificationFailed;

    public void Post<THandler>(THandler? handlers, Action<THandler> invoke)
        where THandler : Delegate
    {
        if (handlers is null)
        {
            return;
        }

        if (
            _queue.Writer.TryWrite(() =>
            {
                foreach (var handler in handlers.GetInvocationList().Cast<THandler>())
                {
                    try
                    {
                        invoke(handler);
                    }
                    catch (Exception error)
                    {
                        ReportFailure(error);
                    }
                }
            })
        )
        {
            return;
        }
        throw new ObjectDisposedException(
            nameof(NotificationDispatcher),
            "Object has been disposed"
        );
    }

    private async Task DispatchAsync()
    {
        await foreach (var notification in _queue.Reader.ReadAllAsync().ConfigureAwait(false))
        {
            notification();
        }
    }

    private void ReportFailure(Exception error)
    {
        var handlers = NotificationFailed;
        if (handlers is null)
        {
            Trace.TraceError("Notification handler failed: {0}", error);
            return;
        }

        foreach (var handler in handlers.GetInvocationList().Cast<Action<Exception>>())
        {
            try
            {
                handler(error);
            }
            catch (Exception reportingError)
            {
                Trace.TraceError("Notification handler failed: {0}", error);
                Trace.TraceError("Notification error handler failed: {0}", reportingError);
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        _queue.Writer.TryComplete();
        await _worker.ConfigureAwait(false);
    }
}
