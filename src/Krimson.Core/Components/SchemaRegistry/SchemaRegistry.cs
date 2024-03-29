using System.Buffers.Binary;
using System.Runtime.Serialization;

namespace Krimson.SchemaRegistry;

public class KrimsonSchemaRegistry {
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
            throw new SerializationException("Failed to parse Confluent Platform Schema Id!", ex);
        }
    }
    
    /// <summary>
    ///     Magic byte that identifies a message with Confluent Platform framing.
    /// </summary>
    public const byte MagicByte = 0;
}