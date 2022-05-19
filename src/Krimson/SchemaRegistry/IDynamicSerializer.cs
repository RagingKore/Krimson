using Confluent.Kafka;

namespace Krimson.SchemaRegistry;

public interface IDynamicSerializer : 
    // IAsyncSerializer<object?>
  ISerializer<object?> 
{ }