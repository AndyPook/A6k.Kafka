using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Bedrock.Framework.Protocols;

namespace A6k.Kafka
{
    /// <summary>
    /// A collection of <see cref="IMessageWriter{TMessage}"/> impl to do basic decoding of intrinsic types
    /// Similar to the Deerializer class in Confluent.Kafka
    /// </summary>
    public class IntrinsicReader
    {
        //public static readonly IDeserializer<object> Null   = new IntDeserializer();

        public static readonly IDeserializer<string> String = new StringDeserializer();
        public static readonly IDeserializer<byte> Byte = new ByteDeserializer();
        public static readonly IDeserializer<byte[]> Bytes = new BytesDeserializer();
        public static readonly IDeserializer<bool> Bool = new BoolDeserializer();
        public static readonly IDeserializer<short> Short = new ShortDeserializer();
        public static readonly IDeserializer<ushort> UShort = new UShortDeserializer();
        public static readonly IDeserializer<int> Int = new IntDeserializer();
        public static readonly IDeserializer<uint> UInt = new UIntDeserializer();
        public static readonly IDeserializer<long> Long = new LongDeserializer();
        public static readonly IDeserializer<ulong> ULong = new ULongDeserializer();

        public static readonly IDeserializer<float> Float = new FloatDeserializer();
        public static readonly IDeserializer<double> Double = new DoubleDeserializer();
        public static readonly IDeserializer<decimal> Decimal = new DecimalDeserializer();

        private static readonly Dictionary<Type, object> deserializers = new Dictionary<Type, object>
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

        public bool TryGetDeserializer<T>(out IDeserializer<T> deserializer)
        {
            if(deserializers.TryGetValue(typeof(T), out var d))
            {
                deserializer = (IDeserializer<T>)d;
                return true;
            }

            deserializer = default;
            return false;
        }

        public class StringDeserializer : IDeserializer<string>
        {
            public string Deserialize(in ReadOnlySpan<byte> input) => Encoding.UTF8.GetString(input);
        }

        public class ByteDeserializer : IDeserializer<byte>
        {
            public byte Deserialize(in ReadOnlySpan<byte> input)
            {
                if (input.Length != 1)
                    throw new ArgumentException($"Deserializer<byte> encountered data of length {input.Length}. Expecting data length to be 1.");

                return input[0];
            }
        }
        public class BytesDeserializer : IDeserializer<byte[]>
        {
            public byte[] Deserialize(in ReadOnlySpan<byte> input)
            {
                return input.ToArray();
            }
        }
        //public ref struct SpanDeserializer : IDeserializer<ReadOnlySpan<byte>>
        //{
        //    public ReadOnlySpan<byte> Deserialize(in ReadOnlySpan<byte> input)
        //    {
        //        return input;
        //    }
        //}
        public class BoolDeserializer : IDeserializer<bool>
        {
            public bool Deserialize(in ReadOnlySpan<byte> input)
            {
                if (input.Length != 1)
                    throw new ArgumentException($"Deserializer<bool> encountered data of length {input.Length}. Expecting data length to be 1.");

                return input[0] > 0;
            }
        }

        public class ShortDeserializer : IDeserializer<short>
        {
            public short Deserialize(in ReadOnlySpan<byte> input)
            {
                if (input.Length != 2)
                    throw new ArgumentException($"Deserializer<short> encountered data of length {input.Length}. Expecting data length to be 2.");

                return Unsafe.ReadUnaligned<short>(ref MemoryMarshal.GetReference(input));
            }
        }
        public class UShortDeserializer : IDeserializer<ushort>
        {
            public ushort Deserialize(in ReadOnlySpan<byte> input)
            {
                if (input.Length != 2)
                    throw new ArgumentException($"Deserializer<short> encountered data of length {input.Length}. Expecting data length to be 2.");

                return Unsafe.ReadUnaligned<ushort>(ref MemoryMarshal.GetReference(input));
            }
        }
        public class IntDeserializer : IDeserializer<int>
        {
            public int Deserialize(in ReadOnlySpan<byte> input)
            {
                if (input.Length != 4)
                    throw new ArgumentException($"Deserializer<int> encountered data of length {input.Length}. Expecting data length to be 4.");

                return Unsafe.ReadUnaligned<int>(ref MemoryMarshal.GetReference(input));
            }
        }
        public class UIntDeserializer : IDeserializer<uint>
        {
            public uint Deserialize(in ReadOnlySpan<byte> input)
            {
                if (input.Length != 4)
                    throw new ArgumentException($"Deserializer<int> encountered data of length {input.Length}. Expecting data length to be 4.");

                return Unsafe.ReadUnaligned<uint>(ref MemoryMarshal.GetReference(input));
            }
        }
        public class LongDeserializer : IDeserializer<long>
        {
            public long Deserialize(in ReadOnlySpan<byte> input)
            {
                if (input.Length != 8)
                    throw new ArgumentException($"Deserializer<long> encountered data of length {input.Length}. Expecting data length to be 8.");

                return Unsafe.ReadUnaligned<long>(ref MemoryMarshal.GetReference(input));
            }
        }
        public class ULongDeserializer : IDeserializer<ulong>
        {
            public ulong Deserialize(in ReadOnlySpan<byte> input)
            {
                if (input.Length != 8)
                    throw new ArgumentException($"Deserializer<long> encountered data of length {input.Length}. Expecting data length to be 8.");

                return Unsafe.ReadUnaligned<ulong>(ref MemoryMarshal.GetReference(input));
            }
        }

        public class FloatDeserializer : IDeserializer<float>
        {
            public float Deserialize(in ReadOnlySpan<byte> input)
            {
                if (input.Length != 4)
                    throw new ArgumentException($"Deserializer<float> encountered data of length {input.Length}. Expecting data length to be 4.");

                return Unsafe.ReadUnaligned<float>(ref MemoryMarshal.GetReference(input));
            }
        }
        public class DoubleDeserializer : IDeserializer<double>
        {
            public double Deserialize(in ReadOnlySpan<byte> input)
            {
                if (input.Length != 8)
                    throw new ArgumentException($"Deserializer<double> encountered data of length {input.Length}. Expecting data length to be 8.");

                return Unsafe.ReadUnaligned<double>(ref MemoryMarshal.GetReference(input));
            }
        }
        public class DecimalDeserializer : IDeserializer<decimal>
        {
            public decimal Deserialize(in ReadOnlySpan<byte> input)
            {
                if (input.Length != 16)
                    throw new ArgumentException($"Deserializer<decimal> encountered data of length {input.Length}. Expecting data length to be 16.");

                return Unsafe.ReadUnaligned<decimal>(ref MemoryMarshal.GetReference(input));
            }
        }
    }
}
