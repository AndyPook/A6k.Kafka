using System;
using System.Buffers;
using System.Text;
using Bedrock.Framework.Protocols;

namespace A6k.Kafka
{
    ///// <summary>
    ///// A collection of <see cref="IMessageWriter{TMessage}"/> impl to do basic decoding of intrinsic types
    ///// Similar to the Deerializer class in Confluent.Kafka
    ///// </summary>
    //public class IntrinsicReader : 
    //    IMessageReader<string>
    //{
    //    public static readonly IntrinsicReader Instance = new IntrinsicReader();


    //    //public static readonly IMessageReader<object> Null = Instance;
    //    //public static readonly IMessageReader<int> Int = Instance;
    //    //public static readonly IMessageReader<short> Short = Instance;
    //    //public static readonly IMessageReader<long> Long = Instance;
    //    //public static readonly IMessageReader<float> Float = Instance;
    //    //public static readonly IMessageReader<double> Double = Instance;
    //    public static readonly IMessageReader<string> String = Instance;
    //    //public static readonly IMessageReader<byte[]> Byte = Instance;

    //    public bool TryParseMessage(in ReadOnlySequence<byte> input, ref SequencePosition consumed, ref SequencePosition examined, out string message)
    //    {
    //        message = Encoding.UTF8.GetString(input);
    //    }
    //}

    /// <summary>
    /// A collection of <see cref="IMessageReader{TMessage}"/> impl to do basic encoding of intrinsic types
    /// Similar to the Serializer class in Confluent.Kafka
    /// </summary>
    public class IntrinsicWriter :
        IMessageWriter<object>,
        IMessageWriter<int>,
        IMessageWriter<short>,
        IMessageWriter<long>,
        IMessageWriter<float>,
        IMessageWriter<double>,
        IMessageWriter<string>,
        IMessageWriter<byte[]>
    {
        public static readonly IntrinsicWriter Instance = new IntrinsicWriter();

        public static readonly IMessageWriter<object> Null = Instance;
        public static readonly IMessageWriter<object> Ignore = Instance;
        public static readonly IMessageWriter<int> Int = Instance;
        public static readonly IMessageWriter<short> Short = Instance;
        public static readonly IMessageWriter<long> Long = Instance;
        public static readonly IMessageWriter<float> Float = Instance;
        public static readonly IMessageWriter<double> Double = Instance;
        public static readonly IMessageWriter<string> String = Instance;
        public static readonly IMessageWriter<byte[]> Byte = Instance;

        public void WriteMessage(object message, IBufferWriter<byte> output) { }

        public void WriteMessage(int message, IBufferWriter<byte> output) => output.WriteInt(message);

        public void WriteMessage(short message, IBufferWriter<byte> output) => output.WriteShort(message);

        public void WriteMessage(long message, IBufferWriter<byte> output) => output.WriteLong(message);

        public void WriteMessage(float message, IBufferWriter<byte> output)
        {
            // got to be a "nicer" way of doing this??

            if (BitConverter.IsLittleEndian)
            {
                unsafe
                {
                    byte[] result = new byte[4];
                    byte* p = (byte*)(&message);
                    result[3] = *p++;
                    result[2] = *p++;
                    result[1] = *p++;
                    result[0] = *p++;
                    output.Write(result);
                }
            }
            else
            {
                output.Write(BitConverter.GetBytes(message));
            }
        }

        public void WriteMessage(double message, IBufferWriter<byte> output) => output.WriteLong(BitConverter.DoubleToInt64Bits(message));

        public void WriteMessage(string message, IBufferWriter<byte> output)
        {
            // there are "better" ways of doing this
            // but this  is simple, for now
            var bytes = Encoding.UTF8.GetBytes(message);
            output.Write(bytes);
        }

        public void WriteMessage(byte[] message, IBufferWriter<byte> output)
        {
            var buffer = output.GetSpan(message.Length);
            message.CopyTo(buffer);
            output.Advance(message.Length);
        }
    }
}
