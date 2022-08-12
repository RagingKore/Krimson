using Krimson.Producers;
using Krimson.Readers;

namespace Krimson.Connectors;

[PublicAPI]
public record PushSourceOptions(KrimsonReader Reader, KrimsonProducer Producer, bool SynchronousDelivery = true);