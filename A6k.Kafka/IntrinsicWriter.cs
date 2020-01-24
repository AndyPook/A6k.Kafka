using System;
using System.Buffers;
using System.Collections.Generic;
using System.Text;
using Bedrock.Framework.Protocols;

namespace A6k.Kafka
{
    /// <summary>
    /// A collection of <see cref="IMessageReader{TMessage}"/> impl to do basic encoding of intrinsic types
    /// Similar to the Serializer class in Confluent.Kafka
    /// </summary>
    public class IntrinsicWriter :
        ISerializer<object>,
        ISerializer<string>,
        ISerializer<byte>,
        ISerializer<byte[]>,
        ISerializer<bool>,
        ISerializer<int>,
        ISerializer<uint>,
        ISerializer<short>,
        ISerializer<ushort>,
        ISerializer<long>,
        ISerializer<ulong>,
        ISerializer<float>,
        ISerializer<double>,
        ISerializer<decimal>
    {
        public static readonly IntrinsicWriter Instance = new IntrinsicWriter();

        public static readonly ISerializer<object> Null = Instance;
        public static readonly ISerializer<object> Ignore = Instance;
        public static readonly ISerializer<string> String = Instance;
        public static readonly ISerializer<byte> Byte = Instance;
        public static readonly ISerializer<byte[]> Bytes = Instance;
        public static readonly ISerializer<bool> Bool = Instance;
        public static readonly ISerializer<short> Short = Instance;
        public static readonly ISerializer<ushort> UShort = Instance;
        public static readonly ISerializer<int> Int = Instance;
        public static readonly ISerializer<uint> UInt = Instance;
        public static readonly ISerializer<long> Long = Instance;
        public static readonly ISerializer<ulong> ULong = Instance;
        public static readonly ISerializer<float> Float = Instance;
        public static readonly ISerializer<double> Double = Instance;
        public static readonly ISerializer<decimal> Decimal = Instance;

        private static readonly Dictionary<Type, object> serializers = new Dictionary<Type, object>
        {
            { typeof(string), String },
            { typeof(byte), Byte },
            { typeof(byte[]), Bytes },
            { typeof(bool), Bool },
            { typeof(short), Short },
            { typeof(ushort), UShort },
            { typeof(int), Int },
            { typeof(uint), UInt },
            { typeof(long), Long },
            { typeof(ulong), ULong },
            { typeof(float), Float },
            { typeof(double), Double },
            { typeof(decimal), Decimal }
        };

        public static bool TryGetSerializer<T>(out ISerializer<T> serializer)
        {
            if (serializers.TryGetValue(typeof(T), out var d))
            {
                serializer = (ISerializer<T>)d;
                return true;
            }

            serializer = default;
            return false;
        }

        public void WriteMessage(object message, IBufferWriter<byte> output) { }

        public void WriteMessage(string message, IBufferWriter<byte> output)
        {
            // there are "better" ways of doing this
            // but this  is simple, for now
            var bytes = Encoding.UTF8.GetBytes(message);
            output.Write(bytes);
        }

        public void WriteMessage(byte message, IBufferWriter<byte> output)
        {
            var buffer = output.GetSpan(1);
            buffer[0] = message;
            output.Advance(1);
        }
        public void WriteMessage(byte[] message, IBufferWriter<byte> output)
        {
            var buffer = output.GetSpan(message.Length);
            message.CopyTo(buffer);
            output.Advance(message.Length);
        }
        public void WriteMessage(bool message, IBufferWriter<byte> output)
        {
            var buffer = output.GetSpan(1);
            buffer[0] = (byte)(message ? 1 : 0);
            output.Advance(1);
        }

        public void WriteMessage(short message, IBufferWriter<byte> output) => output.WriteShort(message);
        public void WriteMessage(ushort message, IBufferWriter<byte> output) => output.WriteUShort(message);

        public void WriteMessage(int message, IBufferWriter<byte> output) => output.WriteInt(message);
        public void WriteMessage(uint message, IBufferWriter<byte> output) => output.WriteUInt(message);

        public void WriteMessage(long message, IBufferWriter<byte> output) => output.WriteLong(message);
        public void WriteMessage(ulong message, IBufferWriter<byte> output) => output.WriteULong(message);

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

        public void WriteMessage(double message, IBufferWriter<byte> output)
        {
            // got to be a "nicer" way of doing this??
          
            if (BitConverter.IsLittleEndian)
            {
                unsafe
                {
                    byte[] result = new byte[8];
                    byte* p = (byte*)(&message);
                    result[7] = *p++;
                    result[6] = *p++;
                    result[5] = *p++;
                    result[4] = *p++;
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

        public void WriteMessage(decimal message, IBufferWriter<byte> output) => throw new NotImplementedException();
    }
}
