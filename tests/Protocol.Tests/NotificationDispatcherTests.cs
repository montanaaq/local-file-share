namespace Protocol.Tests;

public class NotificationDispatcherTests
{
    [Fact]
    public async Task Post_DeliversOtherSubscribersAndReportsFailures_WithoutBlockingPublisher()
    {
        await using var dispatcher = new NotificationDispatcher();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        using var release = new ManualResetEventSlim();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var delivered = new TaskCompletionSource<int>(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        var reported = new TaskCompletionSource<Exception>(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        var failure = new InvalidOperationException("Subscriber failed");
        Action<int> handlers = _ =>
        {
            entered.TrySetResult();
            release.Wait();
            throw failure;
        };
        handlers += value => delivered.TrySetResult(value);
        dispatcher.NotificationFailed += _ =>
            throw new InvalidOperationException("Error subscriber failed");
        dispatcher.NotificationFailed += error => reported.TrySetResult(error);

        dispatcher.Post(handlers, handler => handler(42));
        try
        {
            await entered.Task.WaitAsync(timeout.Token);
            Assert.False(delivered.Task.IsCompleted);
        }
        finally
        {
            release.Set();
        }

        Assert.Equal(42, await delivered.Task.WaitAsync(timeout.Token));
        Assert.Same(failure, await reported.Task.WaitAsync(timeout.Token));
    }
}
