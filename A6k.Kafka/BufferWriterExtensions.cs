using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Text;

namespace A6k.Kafka
{
    public static class BufferWriterExtensions
    {
        public static void Write(this IBufferWriter<byte> output, short num)
        {
            var buffer = output.GetSpan(2);
            BinaryPrimitives.WriteInt16BigEndian(buffer, num);
            output.Advance(2);
        }

        public static void Write(this IBufferWriter<byte> output, int num)
        {
            var buffer = output.GetSpan(4);
            BinaryPrimitives.WriteInt32BigEndian(buffer, num);
            output.Advance(4);
        }

        public static void Write(this IBufferWriter<byte> output, string text)
        {
            var lengthBuffer = output.GetSpan(2);
            if (string.IsNullOrEmpty(text))
            {
                BinaryPrimitives.WriteInt16BigEndian(lengthBuffer, 0);
                output.Advance(2);
                return;
            }

            var bytes = Encoding.UTF8.GetBytes(text);
            output.Write((short)bytes.Length);
            output.Write(bytes);
        }

        public static void WriteVarInt(this IBufferWriter<byte> output, int num)
        {
            Span<byte> buffer = stackalloc byte[5];

            // This code writes length prefix of the message as a VarInt. Read the comment in
            // the BinaryMessageParser.TryParseMessage for details.
            var numBytes = 0;
            do
            {
                ref var current = ref buffer[numBytes];
                current = (byte)(num & 0x7f);
                num >>= 7;
                if (num > 0)
                {
                    current |= 0x80;
                }
                numBytes++;
            }
            while (num > 0);

            output.Write(buffer.Slice(0, numBytes));
        }

        public static int WriteVarInt(Span<byte> buffer, long length)
        {
            // This code writes length prefix of the message as a VarInt. Read the comment in
            // the BinaryMessageParser.TryParseMessage for details.
            var numBytes = 0;
            do
            {
                ref var current = ref buffer[numBytes];
                current = (byte)(length & 0x7f);
                length >>= 7;
                if (length > 0)
                {
                    current |= 0x80;
                }
                numBytes++;
            }
            while (length > 0);

            return numBytes;
        }
    }
}
