namespace Serilog.Tests.Support;

sealed class ThrowingFailureListener : ILoggingFailureListener
{
    public void OnLoggingFailed(object sender, LoggingFailureKind kind, string message, IReadOnlyCollection<LogEvent>? events, Exception? exception)
    {
        throw new InvalidOperationException("The failure listener failed.");
    }
}
