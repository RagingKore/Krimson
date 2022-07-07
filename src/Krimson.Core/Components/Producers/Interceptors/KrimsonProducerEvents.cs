using Confluent.Kafka;
using Krimson.Interceptors;

namespace Krimson.Producers.Interceptors;

public record BeforeProduce(string ProducerName, ProducerRequest Request) : InterceptorEvent;

public record ProduceResultReceived(string ProducerName, ProducerResult Result) : InterceptorEvent;

public record ProduceResultUserHandlingError(string ProducerName, ProducerResult Result, Exception UserException) : InterceptorEvent;

public record ProduceResultError(string ProducerName, Guid RequestId, DeliveryReport<byte[], object?> Report, Exception Exception) : InterceptorEvent;