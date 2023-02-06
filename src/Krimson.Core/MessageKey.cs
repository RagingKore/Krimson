using System.Text;
using static System.String;

namespace Krimson;

[PublicAPI]
public class MessageKey : IEquatable<MessageKey> {
    public static readonly MessageKey None = new(ReadOnlyMemory<byte>.Empty, null!, null!);

    MessageKey(ReadOnlyMemory<byte> bytes, Type type, string value) {
        Bytes = bytes;
        Type  = type;
        Value = value;
    }

    public ReadOnlyMemory<byte> Bytes { get; }
    public Type                 Type  { get; }
    public string               Value { get; }
    
    public static MessageKey From(string messageKey) {
        Ensure.NotNullOrWhiteSpace(messageKey, nameof(messageKey));

        return new(Encoding.UTF8.GetBytes(messageKey), typeof(string), messageKey);
    }

    public static MessageKey From(ulong messageKey) => From(messageKey.ToString());

    public static MessageKey From(ReadOnlyMemory<byte> messageKey) {
        if (messageKey.IsEmpty) return None;

        try {
            return new(messageKey, typeof(string), Encoding.UTF8.GetString(messageKey.Span));
        }
        catch (Exception) {
            try {
                return new(messageKey, typeof(ReadOnlyMemory<byte>), Empty);
            }
            catch (Exception ex) {
                throw new InvalidCastException("Invalid message key!", ex);
            }
        }
    }

    public override string ToString() => Value;

    public static implicit operator MessageKey(long value)                 => From((ulong)value);
    public static implicit operator MessageKey(ulong value)                => From(value);
    public static implicit operator MessageKey(string value)               => From(value);
    public static implicit operator MessageKey(ReadOnlySpan<byte> value)   => From(new ReadOnlyMemory<byte>(value.ToArray()));
    public static implicit operator MessageKey(ReadOnlyMemory<byte> value) => From(value);
    public static implicit operator MessageKey(byte[] value)               => From(value);
    public static implicit operator MessageKey(Guid value)                 => From(value.ToString());
    
    public static implicit operator string(MessageKey self)               => self.Value;
    public static implicit operator byte[](MessageKey self)               => self.Bytes.ToArray();
    public static implicit operator ReadOnlySpan<byte>(MessageKey self)   => self.Bytes.Span;
    public static implicit operator ReadOnlyMemory<byte>(MessageKey self) => self.Bytes;
    public static implicit operator Guid(MessageKey self)                 => Guid.Parse(self.Value);
    public bool Equals(MessageKey? other) {
        if (ReferenceEquals(null, other))
            return false;

        if (ReferenceEquals(this, other))
            return true;

        return Value == other.Value;
    }

    public override bool Equals(object? obj) {
        if (ReferenceEquals(null, obj))
            return false;

        if (ReferenceEquals(this, obj))
            return true;

        if (obj.GetType() != GetType())
            return false;

        return Equals((MessageKey)obj);
    }

    public override int GetHashCode() => Value.GetHashCode();

    public static bool operator ==(MessageKey? left, MessageKey? right) => Equals(left, right);
    public static bool operator !=(MessageKey? left, MessageKey? right) => !Equals(left, right);
}