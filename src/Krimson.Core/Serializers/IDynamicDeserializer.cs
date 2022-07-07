using Confluent.Kafka;

namespace Krimson.Serializers;

public interface IDynamicDeserializer : IAsyncDeserializer<object?>, IDeserializer<object?> { }