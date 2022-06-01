using System.Buffers.Binary;

namespace Krimson.SchemaRegistry;

public static class SchemaRegistry {
    
    /// <summary>
    /// Serialization format:
    ///       byte 0:           A magic byte that identifies this as a message with
    ///                         Confluent Platform framing.
    ///       bytes 1-4:        Unique global id of the schema associated with
    ///                         the data (as registered in Confluent Schema Registry),
    ///                         big endian.
    /// </summary>
    public static int ParseSchemaId(ReadOnlyMemory<byte> data) {
        try {
            return BinaryPrimitives.ReadInt32BigEndian(data.Slice(1, 4).Span);
        }
        catch (Exception ex) {
            throw new("Failed to parse Confluent Platform Schema Id!", ex);
        }
    }
}