namespace Serilog.Tests.Support;

// Rejects every batch, including the ones offered during shutdown, and records whether it was disposed.
sealed class AlwaysFailingBatchedSink : IBatchedLogEventSink, IDisposable
#if FEATURE_ASYNCDISPOSABLE
    , IAsyncDisposable
#endif
{
    int _calls;
    int _disposed;

    public int Calls => Volatile.Read(ref _calls);
    public bool IsDisposed => Volatile.Read(ref _disposed) != 0;

    public Task EmitBatchAsync(IReadOnlyCollection<LogEvent> events)
    {
        Interlocked.Increment(ref _calls);
        throw new InvalidOperationException("The batch failed.");
    }

    public Task OnEmptyBatchAsync() => Task.CompletedTask;

    public void Dispose() => Interlocked.Exchange(ref _disposed, 1);

#if FEATURE_ASYNCDISPOSABLE
    public ValueTask DisposeAsync()
    {
        Dispose();
        return default;
    }
#endif
}
