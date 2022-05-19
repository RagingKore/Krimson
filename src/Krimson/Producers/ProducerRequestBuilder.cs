namespace Krimson.Producers;

[PublicAPI]
public record ProducerRequestBuilder {
    ProducerRequest Options { get; init; } = ProducerRequest.Empty with {
        RequestId = Guid.NewGuid(),
        Headers   = new()
    };

    public ProducerRequestBuilder Topic(string topic) =>
        this with {
            Options = Options with {
                Topic = Ensure.NotNullOrWhiteSpace(topic, nameof(topic))
            }
        };

    public ProducerRequestBuilder Message(object message) =>
        this with {
            Options = Options with {
                Message = Ensure.NotNull(message, nameof(message)),
                Type    = message.GetType()
            }
        };

    public ProducerRequestBuilder Key(MessageKey key) =>
        this with {
            Options = Options with {
                Key = Ensure.NotNull(key, nameof(key))
            }
        };

    public ProducerRequestBuilder Headers(Dictionary<string, string?> headers) =>
        this with {
            Options = Options with {
                Headers = Ensure.NotNull(headers, nameof(headers))
            }
        };

    public ProducerRequestBuilder Headers(Action<Dictionary<string, string?>> setHeaders) =>
        this with {
            Options = Options with {
                Headers = Options.Headers.With(Ensure.NotNull(setHeaders, nameof(setHeaders)))
            }
        };

    public ProducerRequestBuilder RequestId(Guid requestId) =>
        this with {
            Options = Options with {
                RequestId = Ensure.NotEmptyGuid(requestId, nameof(requestId))
            }
        };

    public ProducerRequest Create() {
        Ensure.NotNull(Options.Message, nameof(Options.Message));
        return Options with { }; // always a copy. play it safe.
    }
}