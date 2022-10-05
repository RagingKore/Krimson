using Krimson.Interceptors;

namespace Krimson.Processors.Interceptors;

public abstract record KrimsonClientEvent(KrimsonClient Client) : InterceptorEvent;

public record TopicCreated(KrimsonClient Client) : KrimsonClientEvent(Client);

public record TopicDeleted(KrimsonClient Client) : KrimsonClientEvent(Client);