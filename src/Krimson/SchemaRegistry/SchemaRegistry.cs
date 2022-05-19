using System.Buffers.Binary;

namespace Krimson.SchemaRegistry;

public static class SchemaRegistry {
    public static int ParseSchemaId(ReadOnlyMemory<byte> data) {
        try {
            return BinaryPrimitives.ReadInt32BigEndian(data.Slice(1, 4).Span);
        }
        catch (Exception ex) {
            throw new("Failed to parse schema id!", ex);
        }
    }
}