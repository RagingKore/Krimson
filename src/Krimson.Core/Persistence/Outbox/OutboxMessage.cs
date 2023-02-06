namespace Krimson.Persistence.Outbox;

[PublicAPI]
public record OutboxMessage {
    /// <summary>
    ///     The id of the outbox message. This is used to identify the message in the outbox.
    /// </summary>
    public object Id { get; init; }

    /// <summary>
    ///     The sequence number of the outbox message. This is used to order the messages in the outbox.
    /// </summary>
    public long SequenceNumber { get; init; }

    /// <summary>
    ///   The id of the client that sent the request message.
    /// </summary>
    public string ClientId { get; init; }

    /// <summary>
    ///     The id of the request message. This is used to correlate the message with the original request.
    /// </summary>
    public Guid RequestId { get; init; }

    /// <summary>
    ///     The partition key of the request message. This is used to determine which partition the message should be sent to.
    /// </summary>
    public byte[] Key { get; init; } = Array.Empty<byte>();

    /// <summary>
    ///     The serialized payload of the request message.
    /// </summary>
    public byte[] Data { get; init; }

    /// <summary>
    ///     The headers of the request message.
    /// </summary>
    public IDictionary<string, string?> Headers { get; init; } = new Dictionary<string, string?>();

    /// <summary>
    ///     The topic the request message should be sent to.
    /// </summary>
    public string DestinationTopic { get; init; }

    /// <summary>
    ///     The timestamp of the request message.
    /// </summary>
    public long Timestamp { get; init; }

    /// <summary>
    ///     When the message was created in the outbox.
    /// </summary>
    public DateTimeOffset CreatedOn { get; init; } = DateTimeOffset.UtcNow;

    /// <summary>
    ///     When the message was processed by the outbox.
    /// </summary>
    public DateTimeOffset? ProcessedOn { get; init; }
}