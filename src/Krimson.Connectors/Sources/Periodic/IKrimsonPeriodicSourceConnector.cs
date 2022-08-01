namespace Krimson.Connectors;

public interface IKrimsonPeriodicSourceConnector<in TContext, TData> : ISourceConnector<TContext, TData> where TContext : ISourceConnectorContext {
    TimeSpan BackoffTime { get; }
}


[AttributeUsage(AttributeTargets.Class)]
public class BackOffTimeSecondsAttribute : Attribute {
    public BackOffTimeSecondsAttribute(int seconds) => Value = TimeSpan.FromSeconds(seconds);

    TimeSpan Value { get; }

    public static implicit operator TimeSpan(BackOffTimeSecondsAttribute self) => self.Value;
}

[AttributeUsage(AttributeTargets.Class)]
public class BackOffTimeAttribute : BackOffTimeSecondsAttribute {
    public BackOffTimeAttribute(int seconds) : base(seconds) { }
}