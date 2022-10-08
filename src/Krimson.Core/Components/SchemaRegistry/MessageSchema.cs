namespace Krimson.SchemaRegistry;

public record MessageSchema(int SchemaId, string SubjectName, string ClrTypeName, int MessageIndex = -1, int? Version = null) {
    public static readonly MessageSchema Unknown = new(-1, "", "", -1);
}