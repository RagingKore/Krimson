using Confluent.Kafka;
using static System.DateTimeOffset;

namespace Krimson.Producers;

[PublicAPI]
public record ProducerRequestBuilder {
    ProducerRequest Options { get; init; } = ProducerRequest.Empty with {
        RequestId = Guid.NewGuid(),
        Headers   = new()
    };

    public ProducerRequestBuilder Topic(string? topic) =>
        this with {
            Options = Options with {
                Topic = topic
            }
        };

    public ProducerRequestBuilder Message(object message) {
        Ensure.NotNull(message, nameof(message));

        return this with {
            Options = Options with {
                Message = message,
                Type    = message.GetType()
            }
        };
    }

    public ProducerRequestBuilder Key(MessageKey key) {
        return this with {
            Options = Options with {
                Key = key
            }
        };
    }

    public ProducerRequestBuilder Headers(Dictionary<string, string?> headers) {
        Ensure.NotNull(headers, nameof(headers));
        
        return this with {
            Options = Options with {
                Headers = headers
            }
        };
    }

    public ProducerRequestBuilder Headers(Action<Dictionary<string, string?>> setHeaders) {
        Ensure.NotNull(setHeaders, nameof(setHeaders));
        
        return this with {
            Options = Options with {
                Headers = Options.Headers.With(setHeaders)
            }
        };
    }

    public ProducerRequestBuilder RequestId(Guid requestId) {
        Ensure.NotEmptyGuid(requestId, nameof(requestId));
        
        return this with {
            Options = Options with {
                RequestId = requestId
            }
        };
    }

    public ProducerRequestBuilder Timestamp(Timestamp timestamp) =>
        this with {
            Options = Options with {
                Timestamp = timestamp
            }
        };

    public ProducerRequestBuilder Timestamp(DateTimeOffset timestamp) => Timestamp(new Timestamp(timestamp));
    public ProducerRequestBuilder Timestamp(long milliseconds)        => Timestamp(FromUnixTimeMilliseconds(milliseconds));

    public ProducerRequest Create() {
        Ensure.NotNull(Options.Message, nameof(Options.Message));
        return Options with { }; // always a copy. play it safe. don't be like dave.
    }
}