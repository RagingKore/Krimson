using System.Buffers;
using System.Runtime.CompilerServices;

namespace Krimson; 

[PublicAPI]
public static class SequenceReaderExtensions {
    /// <summary>
    /// Read an integer stored in variable-length format using signed decoding from <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html"> Google Protocol Buffers</a>
    /// </summary>
    /// <returns>The integer read</returns>
    /// <exception cref="OverflowException">Thrown if variable-length value does not terminate after 5 bytes have been read</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int ReadVarInt(this ref SequenceReader<byte> reader) {
        var value = ReadUnsignedVarInt(ref reader);
        return (int)((value >> 1) ^ -(value & 1));
    }

    /// <summary>
    /// Read an integer stored in variable-length format using unsigned decoding from <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html"> Google Protocol Buffers</a>
    /// </summary>
    /// <exception cref="OverflowException">Thrown if variable-length value does not terminate after 5 bytes have been read</exception>
    public static uint ReadUnsignedVarInt(this ref SequenceReader<byte> reader) {
        var value = 0;
        var i     = 0;
        int b     = 0;
        
        while (reader.TryRead(out var bytes) && ((b = bytes) & 0x80) != 0) {
            value |= (b & 0x7f) << i;
            
            i += 7; // next
            
            if (i > 28) 
                throw new OverflowException("Encoded variable length int is larger than uint.MaxValue");
        }

        value |= b << i;
        
        return (uint)value;
    }
}


// /// <summary>Lightweight reader wrapper for <see cref="ReadOnlySpan{T}"/> that follows <see cref="System.IO.BinaryReader"/> logic</summary>
// public ref struct SpanBinaryReader {
//     readonly ReadOnlySpan<byte> _buffer;
//     int                         _position;
//
//     /// <summary>
//     /// Current position
//     /// </summary>
//     public int Position => _position;
//
//     /// <summary>
//     /// Length of underlying buffer
//     /// </summary>
//     public int Length => _buffer.Length;
//
//     /// <summary>
//     /// Initialize <see cref="SpanBinaryReader"/>
//     /// </summary>
//     /// <param name="buffer">Memory buffer to be used</param>
//     /// <param name="position">Starting position</param>
//     public SpanBinaryReader(ReadOnlySpan<byte> buffer, int position = 0) {
//         _buffer   = buffer;
//         _position = position;
//     }
//
//     /// <summary>
//     /// Reset current position to default (0)
//     /// </summary>
//     public void Reset() => _position = 0;
//
//     /// <summary>
//     /// Advance (or retreat) current position by given amount 
//     /// </summary>
//     /// <param name="offset">Offset to advance position by or retreat by in case of negative numbers</param>
//     public SpanBinaryReader Seek(int offset) {
//         var newPosition = _position + offset;
//         if (newPosition < 0)
//             throw new ArgumentOutOfRangeException(nameof(offset), offset, $"After advancing by {nameof(offset)} parameter, position should point to non-negative number");
//
//         _position = newPosition;
//
//         return this;
//     }
//
//     /// <summary>
//     /// Determines if end of buffer was reached 
//     /// </summary>
//     public bool IsEnd => _position >= _buffer.Length;
//
//     /// <summary>
//     /// Reads 1 byte from underlying stream
//     /// </summary>
//     /// <returns>Byte read ot -1 if EOB is reached</returns>
//     [MethodImpl(MethodImplOptions.AggressiveInlining)]
//     public int ReadByte() => _position >= _buffer.Length ? -1 : _buffer[_position++];
//
//     /// <summary>
//     /// Reads one little endian 16 bits integer from underlying stream
//     /// </summary>
//     [MethodImpl(MethodImplOptions.AggressiveInlining)]
//     public short ReadInt16() => BinaryPrimitives.ReadInt16LittleEndian(ReadExactly(2));
//
//     /// <summary>
//     /// Reads one little endian 32 bits integer from underlying stream
//     /// </summary>
//     [MethodImpl(MethodImplOptions.AggressiveInlining)]
//     public int ReadInt32() => BinaryPrimitives.ReadInt32LittleEndian(ReadExactly(4));
//
//     /// <summary>
//     /// Reads one little endian 64 bits integer from underlying stream
//     /// </summary>
//     [MethodImpl(MethodImplOptions.AggressiveInlining)]
//     public long ReadInt64() => BinaryPrimitives.ReadInt64LittleEndian(ReadExactly(8));
//
//     /// <summary>
//     /// Reads boolean value from underlying stream
//     /// </summary>
//     /// <returns><c>true</c> is byte read is non-zero, <c>false</c> otherwise</returns>
//     [MethodImpl(MethodImplOptions.AggressiveInlining)]
//     public bool ReadBoolean() => ReadExactly(1) is var slice && slice[0] != 0;
//
//     /// <summary>
//     /// Reads one little endian 32 bits floating number from underlying stream
//     /// </summary>
//     [MethodImpl(MethodImplOptions.AggressiveInlining)]
//     public float ReadSingle() => BinaryPrimitives.ReadSingleLittleEndian(ReadExactly(4));
//
//     /// <summary>
//     /// Reads one little endian 64 bits floating number from underlying stream
//     /// </summary>
//     [MethodImpl(MethodImplOptions.AggressiveInlining)]
//     public double ReadDouble() => BinaryPrimitives.ReadDoubleLittleEndian(ReadExactly(8));
//
//     /// <summary>
//     /// Reads one little endian 128 bits decimal number from underlying stream
//     /// </summary>
//     [MethodImpl(MethodImplOptions.AggressiveInlining)]
//     public decimal ReadDecimal() {
//         var lo    = ReadInt32();
//         var mid   = ReadInt32();
//         var hi    = ReadInt32();
//         var flags = ReadInt32();
//         return new(
//             lo, mid, hi,
//             (flags & 0b_1000_0000_0000_0000_0000_0000_0000_0000) != 0, (byte)((flags >> 16) & 0xFF)
//         );
//     }
//
//     /// <summary>
//     /// Reads a sequence of bytes from the current stream and advances the position within the stream by the number of bytes read.
//     /// </summary>
//     /// <param name="buffer">A region of memory. When this method returns, the contents of this region are replaced by the bytes read from the current source.</param>
//     /// <returns>The total number of bytes read into the buffer. This can be less than the number of bytes allocated in the buffer if that many bytes are not currently available, or zero (0) if the end of the stream has been reached.</returns>        
//     [MethodImpl(MethodImplOptions.AggressiveInlining)]
//     public int ReadTo(Span<byte> buffer) {
//         var n = Math.Min(Length - Position, buffer.Length);
//         if (n <= 0)
//             return 0;
//
//         _buffer.Slice(_position, n).CopyTo(buffer);
//
//         _position += n;
//         return n;
//     }
//
//     /// <summary>
//     /// Reads buffer of given size at most
//     /// </summary>
//     /// <param name="numBytes">Number of bytes to be read at most</param>
//     /// <returns>Buffer read from underlying buffer</returns>        
//     [MethodImpl(MethodImplOptions.AggressiveInlining)]
//     public ReadOnlySpan<byte> Read(int numBytes) {
//         if (numBytes < 0) throw new ArgumentOutOfRangeException(nameof(numBytes), $"'{numBytes}' should be non negative");
//
//         var n = Math.Min(Length - Position, numBytes);
//         if (n <= 0)
//             return ReadOnlySpan<byte>.Empty;
//
//         var result = _buffer.Slice(_position, n);
//
//         _position += n;
//
//         return result;
//     }
//
//     /// <summary>
//     /// Reads buffer of exact given size 
//     /// </summary>
//     /// <param name="numBytes">Number of bytes to be read</param>
//     /// <returns>Buffer read from underlying buffer</returns>
//     /// <exception cref="ArgumentOutOfRangeException">Thrown when there is not enough data to read</exception>
//     [MethodImpl(MethodImplOptions.AggressiveInlining)]
//     public ReadOnlySpan<byte> ReadExactly(int numBytes) {
//         if (numBytes < 1) throw new ArgumentOutOfRangeException(nameof(numBytes), $"'{numBytes}' should be at least 1");
//
//         var newPosition = _position + numBytes;
//
//         if (newPosition > _buffer.Length)
//             throw new ArgumentOutOfRangeException(nameof(numBytes), $"Not enough data to read {numBytes} bytes from underlying buffer");
//
//         var span = _buffer.Slice(_position, numBytes);
//         _position = newPosition;
//         return span;
//     }
//
//     /// <summary>
//     /// Returns remaining bytes from underlying buffer
//     /// </summary>
//     public ReadOnlySpan<byte> Remaining() => _buffer.Slice(_position, _buffer.Length - _position);
// }
//
// public static class SpanBufferReaderExtensions {
//     /// <summary>
//     /// Read an integer stored in variable-length format using signed decoding from <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html"> Google Protocol Buffers</a>
//     /// </summary>
//     /// <returns>The integer read</returns>
//     /// <exception cref="OverflowException">Thrown if variable-length value does not terminate after 5 bytes have been read</exception>
//     [MethodImpl(MethodImplOptions.AggressiveInlining)]
//     public static int ReadVarint(this ref SpanBinaryReader reader) {
//         var value = ReadUnsignedVarint(ref reader);
//         return (int)((value >> 1) ^ -(value & 1));
//     }
//
//     /// <summary>
//     /// Read an integer stored in variable-length format using unsigned decoding from <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html"> Google Protocol Buffers</a>
//     /// </summary>
//     /// /// <returns>The integer read</returns>
//     /// <remarks>
//     ///     Inspired by: https://github.com/apache/kafka/blob/2.5/clients/src/main/java/org/apache/kafka/common/utils/ByteUtils.java#L142
//     /// </remarks>
//     /// <exception cref="OverflowException">Thrown if variable-length value does not terminate after 5 bytes have been read</exception>
//     public static uint ReadUnsignedVarint(this ref SpanBinaryReader reader) {
//         var value = 0;
//         var i     = 0;
//        
//         int b;
//         while (true) {
//             b = reader.ReadByte();
//             
//             if (b == -1) throw new InvalidOperationException("Unexpected end of stream reading varint");
//
//             if ((b & 0x80) == 0) break;
//
//             value |= (b & 0x7f) << i;
//             
//             i     += 7;
//             
//             if (i > 28) throw new OverflowException("Encoded varint is larger than uint.MaxValue");
//         }
//
//         value |= b << i;
//         return (uint)value;
//     }
// }