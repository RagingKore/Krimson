using Confluent.Kafka;

namespace Krimson.Serializers;

public interface IDynamicSerializer : ISerializer<object?> { }

// it wont be accepted because reasons...
// public interface IDynamicSerializer : IAsyncSerializer<object?>, ISerializer<object?> { }