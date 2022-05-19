using Confluent.Kafka;
using Krimson.Interceptors;

namespace Krimson.Producers.Interceptors;

public record OnProduce(string ProducerName, ProducerRequest Request) : InterceptorEvent;

public record OnProduceResult(string ProducerName, ProducerResult Result) : InterceptorEvent;

public record OnProduceResultUserException(string ProducerName, ProducerResult Result, Exception UserException) : InterceptorEvent;

public record OnConfluentProducerError(string ProducerName, KafkaException Exception) : InterceptorEvent;
