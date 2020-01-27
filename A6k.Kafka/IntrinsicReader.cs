using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
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

        public static bool TryGetDeserializer<T>(out IDeserializer<T> deserializer)
        {
            if (deserializers.TryGetValue(typeof(T), out var d))
            {
                deserializer = (IDeserializer<T>)d;
                return true;
            }

            deserializer = default;
            return false;
        }

        public class StringDeserializer : IDeserializer<string>
        {
            public ValueTask<string> Deserialize(in ReadOnlySpan<byte> input) => new ValueTask<string>(Encoding.UTF8.GetString(input));
        }

        public class ByteDeserializer : IDeserializer<byte>
        {
            public ValueTask<byte> Deserialize(in ReadOnlySpan<byte> input)
            {
                if (input.Length != 1)
                    throw new ArgumentException($"Deserializer<byte> encountered data of length {input.Length}. Expecting data length to be 1.");

                return new ValueTask<byte>(input[0]);
            }
        }
        public class BytesDeserializer : IDeserializer<byte[]>
        {
            public ValueTask<byte[]> Deserialize(in ReadOnlySpan<byte> input)
            {
                return new ValueTask<byte[]>(input.ToArray());
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
            private readonly static ValueTask<bool> True = new ValueTask<bool>(true);
            private readonly static ValueTask<bool> False = new ValueTask<bool>(false);

            public ValueTask<bool> Deserialize(in ReadOnlySpan<byte> input)
            {
                if (input.Length != 1)
                    throw new ArgumentException($"Deserializer<bool> encountered data of length {input.Length}. Expecting data length to be 1.");

                return input[0] > 0 ? True : False;
            }
        }

        public class ShortDeserializer : IDeserializer<short>
        {
            public ValueTask<short> Deserialize(in ReadOnlySpan<byte> input)
            {
                if (input.Length != 2)
                    throw new ArgumentException($"Deserializer<short> encountered data of length {input.Length}. Expecting data length to be 2.");

                return new ValueTask<short>(Unsafe.ReadUnaligned<short>(ref MemoryMarshal.GetReference(input)));
            }
        }
        public class UShortDeserializer : IDeserializer<ushort>
        {
            public ValueTask<ushort> Deserialize(in ReadOnlySpan<byte> input)
            {
                if (input.Length != 2)
                    throw new ArgumentException($"Deserializer<short> encountered data of length {input.Length}. Expecting data length to be 2.");

                return new ValueTask<ushort>(Unsafe.ReadUnaligned<ushort>(ref MemoryMarshal.GetReference(input)));
            }
        }
        public class IntDeserializer : IDeserializer<int>
        {
            public ValueTask<int> Deserialize(in ReadOnlySpan<byte> input)
            {
                if (input.Length != 4)
                    throw new ArgumentException($"Deserializer<int> encountered data of length {input.Length}. Expecting data length to be 4.");

                return new ValueTask<int>(Unsafe.ReadUnaligned<int>(ref MemoryMarshal.GetReference(input)));
            }
        }
        public class UIntDeserializer : IDeserializer<uint>
        {
            public ValueTask<uint> Deserialize(in ReadOnlySpan<byte> input)
            {
                if (input.Length != 4)
                    throw new ArgumentException($"Deserializer<int> encountered data of length {input.Length}. Expecting data length to be 4.");

                return new ValueTask<uint>(Unsafe.ReadUnaligned<uint>(ref MemoryMarshal.GetReference(input)));
            }
        }
        public class LongDeserializer : IDeserializer<long>
        {
            public ValueTask<long> Deserialize(in ReadOnlySpan<byte> input)
            {
                if (input.Length != 8)
                    throw new ArgumentException($"Deserializer<long> encountered data of length {input.Length}. Expecting data length to be 8.");

                return new ValueTask<long>(Unsafe.ReadUnaligned<long>(ref MemoryMarshal.GetReference(input)));
            }
        }
        public class ULongDeserializer : IDeserializer<ulong>
        {
            public ValueTask<ulong> Deserialize(in ReadOnlySpan<byte> input)
            {
                if (input.Length != 8)
                    throw new ArgumentException($"Deserializer<long> encountered data of length {input.Length}. Expecting data length to be 8.");

                return new ValueTask<ulong>(Unsafe.ReadUnaligned<ulong>(ref MemoryMarshal.GetReference(input)));
            }
        }

        public class FloatDeserializer : IDeserializer<float>
        {
            public ValueTask<float> Deserialize(in ReadOnlySpan<byte> input)
            {
                if (input.Length != 4)
                    throw new ArgumentException($"Deserializer<float> encountered data of length {input.Length}. Expecting data length to be 4.");

                return new ValueTask<float>(Unsafe.ReadUnaligned<float>(ref MemoryMarshal.GetReference(input)));
            }
        }
        public class DoubleDeserializer : IDeserializer<double>
        {
            public ValueTask<double> Deserialize(in ReadOnlySpan<byte> input)
            {
                if (input.Length != 8)
                    throw new ArgumentException($"Deserializer<double> encountered data of length {input.Length}. Expecting data length to be 8.");

                return new ValueTask<double>(Unsafe.ReadUnaligned<double>(ref MemoryMarshal.GetReference(input)));
            }
        }
        public class DecimalDeserializer : IDeserializer<decimal>
        {
            public ValueTask<decimal> Deserialize(in ReadOnlySpan<byte> input)
            {
                if (input.Length != 16)
                    throw new ArgumentException($"Deserializer<decimal> encountered data of length {input.Length}. Expecting data length to be 16.");

                return new ValueTask<decimal>(Unsafe.ReadUnaligned<decimal>(ref MemoryMarshal.GetReference(input)));
            }
        }
    }
}
