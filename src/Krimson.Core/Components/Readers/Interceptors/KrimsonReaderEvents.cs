using Krimson.Interceptors;

namespace Krimson.Readers.Interceptors;

public abstract record ReaderEvent(IKrimsonReaderInfo Reader) : InterceptorEvent;

/// <summary>
/// Record received, deserialized and waiting to be processed.
/// </summary>
public record RecordReceived(IKrimsonReaderInfo Reader, KrimsonRecord Record) : ReaderEvent(Reader);