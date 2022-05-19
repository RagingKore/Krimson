using Confluent.Kafka;

namespace Krimson.SchemaRegistry;

public interface IDynamicDeserializer : IAsyncDeserializer<object?>, IDeserializer<object?> { }