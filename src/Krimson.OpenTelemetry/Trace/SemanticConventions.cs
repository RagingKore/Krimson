namespace Krimson.OpenTelemetry.Trace; 

/// <summary>
/// Constants for semantic attribute names outlined by the OpenTelemetry specifications.
/// <see href="https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/semantic_conventions/README.md"/>.
/// </summary>
static class SemanticConventions {
    // The set of constants matches the specification as of this commit.
    // https://github.com/open-telemetry/opentelemetry-specification/tree/main/specification/trace/semantic_conventions
    // https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/semantic_conventions/exceptions.md
    
    public const string AttributeNetPeerIp    = "net.peer.ip";
    public const string AttributeNetPeerPort  = "net.peer.port";
    public const string AttributeNetPeerName  = "net.peer.name";

    public const string AttributePeerService = "peer.service";

    public const string AttributeMessageType             = "message.type";
    public const string AttributeMessageId               = "message.id";
    public const string AttributeMessageCompressedSize   = "message.compressed_size";
    public const string AttributeMessageUncompressedSize = "message.uncompressed_size";

    public const string AttributeMessagingSystem                = "messaging.system";
    public const string AttributeMessagingDestination           = "messaging.destination";
    public const string AttributeMessagingDestinationKind       = "messaging.destination_kind";
    public const string AttributeMessagingTempDestination       = "messaging.temp_destination";
    public const string AttributeMessagingProtocol              = "messaging.protocol";
    public const string AttributeMessagingProtocolVersion       = "messaging.protocol_version";
    public const string AttributeMessagingUrl                   = "messaging.url";
    public const string AttributeMessagingMessageId             = "messaging.message_id";
    public const string AttributeMessagingConversationId        = "messaging.conversation_id";
    public const string AttributeMessagingPayloadSize           = "messaging.message_payload_size_bytes";
    public const string AttributeMessagingPayloadCompressedSize = "messaging.message_payload_compressed_size_bytes";
    public const string AttributeMessagingOperation             = "messaging.operation";
}