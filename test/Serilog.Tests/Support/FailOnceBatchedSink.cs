namespace Serilog.Tests.Support;

// Fails the first batch it is given and accepts every batch after that.
sealed class FailOnceBatchedSink : IBatchedLogEventSink
{
    readonly object _stateLock = new();
    readonly List<IReadOnlyCollection<LogEvent>> _batches = new();
    int _calls;

    public IReadOnlyList<IReadOnlyCollection<LogEvent>> Batches
    {
        get
        {
            lock (_stateLock)
                return _batches.ToList();
        }
    }

    public Task EmitBatchAsync(IReadOnlyCollection<LogEvent> events)
    {
        if (Interlocked.Increment(ref _calls) == 1)
            throw new InvalidOperationException("The first batch failed.");

        lock (_stateLock)
            _batches.Add(events.ToList());

        return Task.CompletedTask;
    }

    public Task OnEmptyBatchAsync() => Task.CompletedTask;
}
