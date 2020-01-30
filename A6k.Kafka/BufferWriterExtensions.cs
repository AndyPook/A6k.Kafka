using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Text;
using Bedrock.Framework.Protocols;

namespace A6k.Kafka
{
    public static class BufferWriterExtensions
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void WriteByte(this IBufferWriter<byte> output, byte num)
        {
            var buffer = output.GetSpan(1);
            buffer[0] = num;
            output.Advance(1);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void WriteBytes(this IBufferWriter<byte> output, byte[] array)
        {
            if (array == null || array.Length == 0)
            {
                output.WriteInt(0);
                return;
            }

            output.WriteInt(array.Length);
            output.Write(array);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void WriteShort(this IBufferWriter<byte> output, short num)
        {
            var buffer = output.GetSpan(2);
            BinaryPrimitives.WriteInt16BigEndian(buffer, num);
            output.Advance(2);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void WriteUShort(this IBufferWriter<byte> output, ushort num)
        {
            var buffer = output.GetSpan(2);
            BinaryPrimitives.WriteUInt16BigEndian(buffer, num);
            output.Advance(2);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void WriteInt(this IBufferWriter<byte> output, int num)
        {
            var buffer = output.GetSpan(4);
            BinaryPrimitives.WriteInt32BigEndian(buffer, num);
            output.Advance(4);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void WriteUInt(this IBufferWriter<byte> output, uint num)
        {
            var buffer = output.GetSpan(4);
            BinaryPrimitives.WriteUInt32BigEndian(buffer, num);
            output.Advance(4);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void WriteLong(this IBufferWriter<byte> output, long num)
        {
            var buffer = output.GetSpan(8);
            BinaryPrimitives.WriteInt64BigEndian(buffer, num);
            output.Advance(8);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void WriteULong(this IBufferWriter<byte> output, ulong num)
        {
            var buffer = output.GetSpan(8);
            BinaryPrimitives.WriteUInt64BigEndian(buffer, num);
            output.Advance(8);
        }

        /// <summary>
        /// Represents a sequence of characters. First the length N is given as an INT16.
        /// Then N bytes follow which are the UTF-8 encoding of the character sequence.
        /// Length must not be negative.
        /// </summary>
        /// <param name="output"></param>
        /// <param name="text"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void WriteString(this IBufferWriter<byte> output, string text)
        {
            if (string.IsNullOrEmpty(text))
            {
                output.WriteShort(0);
                return;
            }

            var textLength = Encoding.UTF8.GetByteCount(text);
            output.WriteShort((short)textLength);

            var textSpan = output.GetSpan(textLength);
            Encoding.UTF8.GetBytes(text, textSpan);
            output.Advance(textLength);
        }

        /// <summary>
        /// Represents a sequence of characters or null. For non-null strings, first the length N is given as an INT16.
        /// Then N bytes follow which are the UTF-8 encoding of the character sequence.
        /// A null value is encoded with length of -1 and there are no following bytes.
        /// </summary>
        /// <param name="output"></param>
        /// <param name="text"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void WriteNullableString(this IBufferWriter<byte> output, string text)
        {
            if (text == null)
            {
                output.WriteShort(-1);
                return;
            }
            else if (text.Length == 0)
            {
                output.WriteShort(0);
                return;
            }

            var textLength = Encoding.UTF8.GetByteCount(text);
            output.WriteShort((short)textLength);

            var textSpan = output.GetSpan(textLength);
            Encoding.UTF8.GetBytes(text, textSpan);
            output.Advance(textLength);
        }

        /// <summary>
        /// Represents a sequence of characters. First the length N + 1 is given as an UNSIGNED_VARINT.
        /// Then N bytes follow which are the UTF-8 encoding of the character sequence.
        /// </summary>
        /// <param name="output"></param>
        /// <param name="text"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void WriteCompactString(this IBufferWriter<byte> output, string text)
        {
            if (string.IsNullOrEmpty(text))
            {
                output.WriteVarInt(1);
                return;
            }

            var textLength = Encoding.UTF8.GetByteCount(text);
            output.WriteVarInt((uint)textLength + 1);

            var textSpan = output.GetSpan(textLength);
            Encoding.UTF8.GetBytes(text, textSpan);
            output.Advance(textLength);
        }

        /// <summary>
        /// Represents a sequence of characters. First the length N + 1 is given as an UNSIGNED_VARINT.
        /// Then N bytes follow which are the UTF-8 encoding of the character sequence.
        /// A null string is represented with a length of 0.
        /// </summary>
        /// <param name="output"></param>
        /// <param name="text"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void WriteCompactNullableString(this IBufferWriter<byte> output, string text)
        {
            if (text == null)
            {
                output.WriteVarInt(0);
                return;
            }
            else if (text.Length == 0)
            {
                output.WriteVarInt(1);
                return;
            }

            var textLength = Encoding.UTF8.GetByteCount(text);
            output.WriteVarInt((uint)textLength + 1);

            var textSpan = output.GetSpan(textLength);
            Encoding.UTF8.GetBytes(text, textSpan);
            output.Advance(textLength);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void WriteVarInt(this IBufferWriter<byte> output, int num)
        {
            var n = (uint)((num << 1) ^ (num >> 31));
            output.WriteVarInt((ulong)n);
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void WriteVarInt(this IBufferWriter<byte> output, long num)
        {
            var n = (ulong)((num << 1) ^ (num >> 63));
            output.WriteVarInt(n);
        }

        //public static void WriteVarInt(this IBufferWriter<byte> output, int num) => output.WriteVarInt((ulong)num);
        [MethodImpl(MethodImplOptions.AggressiveInlining)] 
        public static void WriteVarInt(this IBufferWriter<byte> output, uint num) => output.WriteVarInt((ulong)num);
        //public static void WriteVarInt(this IBufferWriter<byte> output, long num) => output.WriteVarInt((ulong)num);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void WriteVarInt(this IBufferWriter<byte> output, ulong num)
        {
            Span<byte> buffer = stackalloc byte[9];

            // This code writes length prefix of the message as a VarInt. Read the comment in
            // the BinaryMessageParser.TryParseMessage for details.
            var numBytes = 0;
            do
            {
                ref var current = ref buffer[numBytes];
                current = (byte)(num & 0x7f);
                num >>= 7;
                if (num > 0)
                    current |= 0x80;
                numBytes++;
            }
            while (num > 0);

            output.Write(buffer.Slice(0, numBytes));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void WriteArray<T>(this IBufferWriter<byte> output, T[] array, Action<T, IBufferWriter<byte>> writer)
        {
            if (array == null)
            {
                output.WriteInt(0);
                return;
            }

            output.WriteInt(array.Length);
            for (int i = 0; i < array.Length; i++)
                writer(array[i], output);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void WriteArray(this IBufferWriter<byte> output, int[] array)
        {
            if (array == null)
            {
                output.WriteInt(0);
                return;
            }

            output.WriteInt(array.Length);
            for (int i = 0; i < array.Length; i++)
                output.WriteInt(array[i]);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void WriteArray(this IBufferWriter<byte> output, string[] array)
        {
            if (array == null)
            {
                output.WriteInt(0);
                return;
            }

            output.WriteInt(array.Length);
            for (int i = 0; i < array.Length; i++)
                output.WriteString(array[i]);
        }

        public enum PrefixType
        {
            Int,
            VarInt,
            UnsignedVarInt,
            Crc
        }
        public static void WritePrefixed<T>(this IBufferWriter<byte> output, ISerializer<T> writer, T item, PrefixType prefixType)
        {
            var buffer = new MemoryBufferWriter<byte>();
            writer.WriteMessage(item, buffer);

            switch (prefixType)
            {
                case PrefixType.Int:
                    output.WriteInt((int)buffer.Length);
                    break;
                case PrefixType.VarInt:
                    output.WriteVarInt(buffer.Length);
                    break;
                case PrefixType.UnsignedVarInt:
                    output.WriteVarInt((ulong)buffer.Length);
                    break;
                case PrefixType.Crc:
                    var crc = Crc32C.Compute(buffer);
                    output.WriteUInt(crc);
                    break;
            }

            buffer.CopyTo(output);
        }
        public static void WritePrefixed<T>(this IBufferWriter<byte> output, IMessageWriter<T> writer, T item, PrefixType prefixType)
        {
            var buffer = new MemoryBufferWriter<byte>();
            writer.WriteMessage(item, buffer);

            switch (prefixType)
            {
                case PrefixType.Int:
                    output.WriteInt((int)buffer.Length);
                    break;
                case PrefixType.VarInt:
                    output.WriteVarInt(buffer.Length);
                    break;
                case PrefixType.UnsignedVarInt:
                    output.WriteVarInt((ulong)buffer.Length);
                    break;
                case PrefixType.Crc:
                    var crc = Crc32C.Compute(buffer);
                    output.WriteUInt(crc);
                    break;
            }

            buffer.CopyTo(output);
        }
        public static void WritePrefixed<T>(this IBufferWriter<byte> output, Action<T, IBufferWriter<byte>> writer, T item, PrefixType prefixType)
        {
            var buffer = new MemoryBufferWriter<byte>();
            writer(item, buffer);

            switch (prefixType)
            {
                case PrefixType.Int:
                    output.WriteInt((int)buffer.Length);
                    break;
                case PrefixType.VarInt:
                    output.WriteVarInt(buffer.Length);
                    break;
                case PrefixType.UnsignedVarInt:
                    output.WriteVarInt((ulong)buffer.Length);
                    break;
                case PrefixType.Crc:
                    var crc = Crc32C.Compute(buffer);
                    output.WriteUInt(crc);
                    break;
            }

            buffer.CopyTo(output);
        }
        public static void WritePrefixed(this IBufferWriter<byte> output, ReadOnlySpan<byte> item, PrefixType prefixType)
        {
            switch (prefixType)
            {
                case PrefixType.Int:
                    output.WriteInt((int)item.Length);
                    break;
                case PrefixType.VarInt:
                    output.WriteVarInt(item.Length);
                    break;
                case PrefixType.UnsignedVarInt:
                    output.WriteVarInt((ulong)item.Length);
                    break;
                case PrefixType.Crc:
                    throw new InvalidOperationException("CRC not supported here");
            }

            // Try to minimize segments in the target writer by hinting at the total size.
            var buffer = output.GetSpan(item.Length);
            item.CopyTo(buffer);
            output.Advance(item.Length);
        }
    }
}
