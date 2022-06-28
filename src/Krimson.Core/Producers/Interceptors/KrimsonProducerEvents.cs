using Confluent.Kafka;
using Krimson.Interceptors;

namespace Krimson.Producers.Interceptors;

public record BeforeProduce(string ProducerName, ProducerRequest Request) : InterceptorEvent;

public record ProducerResultReceived(string ProducerName, ProducerResult Result) : InterceptorEvent;

public record ProducerResultUserHandlingError(string ProducerName, ProducerResult Result, Exception UserException) : InterceptorEvent;

public record ProducerResultError(string ProducerName, Guid RequestId, DeliveryReport<byte[], object?> Report, Exception Exception) : InterceptorEvent;
