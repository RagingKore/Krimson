using Confluent.Kafka;
using Krimson.Interceptors;

namespace Krimson.Client.Configuration;

[PublicAPI]
public record KrimsonClientOptions {
    public KrimsonClientOptions() {
        Interceptors  = new();
        Configuration = DefaultConfigs.DefaultClientConfig;
    }

    public InterceptorCollection Interceptors  { get; init; }
    public ClientConfig          Configuration { get; init; }
}