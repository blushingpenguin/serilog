using Serilog.Core.Sinks.Batching;

namespace Serilog.Core.Tests;

public class BatchingSinkTests
{
    static readonly TimeSpan TinyWait = TimeSpan.FromMilliseconds(500);
    static readonly TimeSpan MicroWait = TimeSpan.FromMilliseconds(1);

    [Fact]
    public void WhenAnEventIsEnqueuedItIsWrittenToABatchOnDispose()
    {
        var bs = new InMemoryBatchedSink(TimeSpan.Zero);
        var pbs = new BatchingSink(bs, new() { BatchSizeLimit = 2, BufferingTimeLimit = TinyWait, EagerlyEmitFirstEvent = true });
        var evt = Some.InformationEvent();
        pbs.Emit(evt);
        pbs.Dispose();
        var batch = Assert.Single(bs.Batches);
        var actual = Assert.Single(batch);
        Assert.Same(evt, actual);
        Assert.True(bs.IsDisposed);
        Assert.False(bs.WasCalledAfterDisposal);
    }

#if FEATURE_ASYNCDISPOSABLE
        [Fact]
        public async Task WhenAnEventIsEnqueuedItIsWrittenToABatchOnDisposeAsync()
        {
            var bs = new InMemoryBatchedSink(TimeSpan.Zero);
            var pbs = new BatchingSink(
                bs, new()
                {
                    BatchSizeLimit = 2, BufferingTimeLimit = TinyWait, EagerlyEmitFirstEvent = true
                });
            var evt = Some.InformationEvent();
            pbs.Emit(evt);
            await pbs.DisposeAsync();
            var batch = Assert.Single(bs.Batches);
            var actual = Assert.Single(batch);
            Assert.Same(evt, actual);
            Assert.True(bs.IsDisposed);
            Assert.True(bs.IsDisposedAsync);
            Assert.False(bs.WasCalledAfterDisposal);
        }
#endif

    [Fact]
    public void WhenAnEventIsEnqueuedItIsWrittenToABatchOnTimer()
    {
        var bs = new InMemoryBatchedSink(TimeSpan.Zero);
        var pbs = new BatchingSink(
            bs,
            new()
            {
                BatchSizeLimit = 2, BufferingTimeLimit = TinyWait, EagerlyEmitFirstEvent = true
            });
        var evt = Some.InformationEvent();
        pbs.Emit(evt);
        Thread.Sleep(TinyWait + TinyWait);
        bs.Stop();
        pbs.Dispose();
        Assert.Single(bs.Batches);
        Assert.True(bs.IsDisposed);
        Assert.False(bs.WasCalledAfterDisposal);
    }

    [Fact]
    public void WhenAnEventIsEnqueuedItIsWrittenToABatchOnDisposeWhileRunning()
    {
        var bs = new InMemoryBatchedSink(TinyWait + TinyWait);
        var pbs = new BatchingSink(bs, new() { BatchSizeLimit = 2, BufferingTimeLimit = MicroWait, EagerlyEmitFirstEvent = true });
        var evt = Some.InformationEvent();
        pbs.Emit(evt);
        Thread.Sleep(TinyWait);
        pbs.Dispose();
        Assert.Single(bs.Batches);
        Assert.True(bs.IsDisposed);
        Assert.False(bs.WasCalledAfterDisposal);
    }

    [Fact]
    public void AThrowingFailureListenerDoesNotStopTheLoop()
    {
        // Failure listeners are user-supplied and run in the read loop's failure path. One that throws must not
        // fault the loop task; `Emit()` would otherwise keep queueing events that are never read.
        var bs = new FailOnceBatchedSink();
        var pbs = new BatchingSink(bs, new() { BatchSizeLimit = 1, BufferingTimeLimit = MicroWait, EagerlyEmitFirstEvent = true });
        pbs.SetFailureListener(new ThrowingFailureListener());

        var evt = Some.InformationEvent();
        pbs.Emit(evt);

        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
        while (bs.Batches.Count == 0 && DateTime.UtcNow < deadline)
            Thread.Sleep(MicroWait);

        pbs.Dispose();

        var batch = Assert.Single(bs.Batches);
        Assert.Same(evt, Assert.Single(batch));
    }

    [Fact]
    public void AnExceptionOutsideBatchEmissionDoesNotStopTheLoop()
    {
        // `Task.Delay()` rejects intervals beyond roughly 49 days, so with this buffering time limit the first
        // statement of every loop iteration throws - standing in for the `OutOfMemoryException` that can surface
        // from any statement there under memory pressure. The loop must survive it, so that the queued event is
        // still flushed on disposal.
        var bs = new InMemoryBatchedSink(TimeSpan.Zero);
        var pbs = new BatchingSink(bs, new() { BatchSizeLimit = 2, BufferingTimeLimit = TimeSpan.FromDays(60) });

        var evt = Some.InformationEvent();
        pbs.Emit(evt);
        Thread.Sleep(TinyWait);
        pbs.Dispose();

        var batch = Assert.Single(bs.Batches);
        Assert.Same(evt, Assert.Single(batch));
    }

    [Fact]
    public void AThrowingFailureListenerDoesNotPreventDisposalWhenTheShutdownFlushFails()
    {
        // The shutdown flush reports a failed batch to the listener. A listener that throws from that report must
        // not fault the loop task, nor escape `Dispose()` before the target sink has been disposed.
        var bs = new AlwaysFailingBatchedSink();
        var pbs = new BatchingSink(bs, new() { BatchSizeLimit = 1, BufferingTimeLimit = TimeSpan.FromDays(1), EagerlyEmitFirstEvent = true });
        pbs.SetFailureListener(new ThrowingFailureListener());

        // With a batch size of one, the first event fills the batch and fails; the second waits in the queue for the
        // shutdown flush to find.
        pbs.Emit(Some.InformationEvent());
        pbs.Emit(Some.InformationEvent());

        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
        while (bs.Calls == 0 && DateTime.UtcNow < deadline)
            Thread.Sleep(MicroWait);

        pbs.Dispose();

        Assert.True(bs.Calls >= 2);
        Assert.True(bs.IsDisposed);
    }

#if FEATURE_ASYNCDISPOSABLE
    [Fact]
    public async Task AThrowingFailureListenerDoesNotPreventAsyncDisposalWhenTheShutdownFlushFails()
    {
        var bs = new AlwaysFailingBatchedSink();
        var pbs = new BatchingSink(bs, new() { BatchSizeLimit = 1, BufferingTimeLimit = TimeSpan.FromDays(1), EagerlyEmitFirstEvent = true });
        pbs.SetFailureListener(new ThrowingFailureListener());

        pbs.Emit(Some.InformationEvent());
        pbs.Emit(Some.InformationEvent());

        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
        while (bs.Calls == 0 && DateTime.UtcNow < deadline)
            Thread.Sleep(MicroWait);

        await pbs.DisposeAsync();

        Assert.True(bs.Calls >= 2);
        Assert.True(bs.IsDisposed);
    }
#endif

    [Fact]
    public void ExecutionContextDoesNotFlowToBatchedSink()
    {
        var local = new AsyncLocal<int>
        {
            Value = 5
        };

        var observed = 17;
        var bs = new CallbackBatchedSink(_ =>
        {
            observed = local.Value;
            return Task.CompletedTask;
        });

        var pbs = new BatchingSink(bs, new());
        var evt = Some.InformationEvent();
        pbs.Emit(evt);
        pbs.Dispose();

        Assert.Equal(default, observed);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task EagerlyEmitFirstEventCausesQuickInitialBatch(bool eagerlyEmit)
    {
        long emitted = 0;
        var bs = new CallbackBatchedSink(_ =>
        {
            // ReSharper disable once AccessToModifiedClosure
            Interlocked.Increment(ref emitted);
            return Task.CompletedTask;
        });

        var options = new BatchingOptions
        {
            BufferingTimeLimit = TimeSpan.FromSeconds(3),
            EagerlyEmitFirstEvent = eagerlyEmit,
            BatchSizeLimit = 10,
            QueueLimit = 1000
        };

        var pbs = new BatchingSink(bs, options);

        var evt = Some.InformationEvent();
        pbs.Emit(evt);

        await Task.Delay(1900);
        Assert.Equal(eagerlyEmit, Interlocked.Read(ref emitted) == 1);

#if FEATURE_ASYNCDISPOSABLE
        await pbs.DisposeAsync();
#else
        pbs.Dispose();
#endif

        Assert.Equal(1, Interlocked.Read(ref emitted));
    }
}
