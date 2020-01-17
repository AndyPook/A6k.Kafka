using System.Buffers;
using System.Buffers.Binary;
using System.Text;

namespace TestConsole
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
    }
}
