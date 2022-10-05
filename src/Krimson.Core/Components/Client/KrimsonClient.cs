// ReSharper disable MethodSupportsCancellation

using Confluent.Kafka;
using Krimson.Client.Configuration;
using Krimson.Consumers.Interceptors;
using Krimson.Interceptors;
using static Serilog.Core.Constants;

namespace Krimson;

[PublicAPI]
public sealed class KrimsonClient {
    public static KrimsonClientBuilder Builder => new();

    public KrimsonClient(KrimsonClientOptions options) {
        ClientId = options.Configuration.ClientId;
        
        Logger   = Log.ForContext(SourceContextPropertyName, ClientId);

        Intercept = options.Interceptors
            .Prepend(new ConfluentConsumerLogger().WithName("ConfluentAdminClient"))
            .Intercept;

        Client = new AdminClientBuilder(options.Configuration)
            .SetLogHandler((adm, log) => Intercept(new ConfluentConsumerLog(ClientId, adm.GetInstanceName(), log)))
            .SetErrorHandler((adm, err) => Intercept(new ConfluentConsumerError(ClientId, adm.GetInstanceName(), err)))
            .Build();
    }

    ILogger       Logger    { get; }
    IAdminClient  Client    { get; }
    Intercept     Intercept { get; }
    
    public string ClientId { get; set; }
}