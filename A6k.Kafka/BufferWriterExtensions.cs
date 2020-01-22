using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Text;
using Bedrock.Framework.Protocols;

namespace A6k.Kafka
{
    public static class BufferWriterExtensions
    {
        public static void WriteByte(this IBufferWriter<byte> output, byte num)
        {
            var buffer = output.GetSpan(1);
            buffer[0] = num;
            output.Advance(1);
        }

        public static void WriteShort(this IBufferWriter<byte> output, short num)
        {
            var buffer = output.GetSpan(2);
            BinaryPrimitives.WriteInt16BigEndian(buffer, num);
            output.Advance(2);
        }

        public static void WriteUShort(this IBufferWriter<byte> output, ushort num)
        {
            var buffer = output.GetSpan(2);
            BinaryPrimitives.WriteUInt16BigEndian(buffer, num);
            output.Advance(2);
        }

        public static void WriteInt(this IBufferWriter<byte> output, int num)
        {
            var buffer = output.GetSpan(4);
            BinaryPrimitives.WriteInt32BigEndian(buffer, num);
            output.Advance(4);
        }

        public static void WriteUInt(this IBufferWriter<byte> output, uint num)
        {
            var buffer = output.GetSpan(4);
            BinaryPrimitives.WriteUInt32BigEndian(buffer, num);
            output.Advance(4);
        }

        public static void WriteLong(this IBufferWriter<byte> output, long num)
        {
            var buffer = output.GetSpan(8);
            BinaryPrimitives.WriteInt64BigEndian(buffer, num);
            output.Advance(8);
        }

        public static void WriteULong(this IBufferWriter<byte> output, ulong num)
        {
            var buffer = output.GetSpan(8);
            BinaryPrimitives.WriteUInt64BigEndian(buffer, num);
            output.Advance(8);
        }

        public static void WriteString(this IBufferWriter<byte> output, string text)
        {
            var lengthBuffer = output.GetSpan(2);
            if (string.IsNullOrEmpty(text))
            {
                BinaryPrimitives.WriteInt16BigEndian(lengthBuffer, 0);
                output.Advance(2);
                return;
            }

            var bytes = Encoding.UTF8.GetBytes(text);
            output.WriteShort((short)bytes.Length);
            output.Write(bytes);
        }
        public static void WriteNullableString(this IBufferWriter<byte> output, string text)
        {
            var lengthBuffer = output.GetSpan(2);
            if (text == null)
            {
                BinaryPrimitives.WriteInt16BigEndian(lengthBuffer, -1);
                output.Advance(2);
                return;
            }
            else if (text.Length == 0)
            {
                BinaryPrimitives.WriteInt16BigEndian(lengthBuffer, 0);
                output.Advance(2);
                return;
            }

            var bytes = Encoding.UTF8.GetBytes(text);
            output.WriteShort((short)bytes.Length);
            output.Write(bytes);
        }

        public static void WriteCompactString(this IBufferWriter<byte> output, string text)
        {
            var bytes = Encoding.UTF8.GetBytes(text);
            output.WriteVarInt((ulong)bytes.Length);
            output.Write(bytes);
        }

        public static void WriteVarInt(this IBufferWriter<byte> output, int num)
        {
            var n = (uint)((num << 1) ^ (num >> 31));
            output.WriteVarInt((ulong)n);
        }
        public static void WriteVarInt(this IBufferWriter<byte> output, long num)
        {
            var n = (ulong)((num << 1) ^ (num >> 63));
            output.WriteVarInt(n);
        }

        //public static void WriteVarInt(this IBufferWriter<byte> output, int num) => output.WriteVarInt((ulong)num);
        public static void WriteVarInt(this IBufferWriter<byte> output, uint num) => output.WriteVarInt((ulong)num);
        //public static void WriteVarInt(this IBufferWriter<byte> output, long num) => output.WriteVarInt((ulong)num);

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

        public static void WriteArray<T>(this IBufferWriter<byte> output, T[] array, Action<T, IBufferWriter<byte>> writer)
        {
            if(array==null)
            {
                output.WriteInt(0);
                return;
            }

            output.WriteInt(array.Length);
            for(int i = 0; i < array.Length; i++)
                writer(array[i], output);
        }

        public enum PrefixType
        {
            Int,
            VarInt,
            UnsignedVarInt,
            Crc
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
