using System;
using System.Buffers;
using System.Diagnostics;
using System.Text;

namespace A6k.Kafka
{
    public static class SequenceReaderExtensions
    {
        public delegate bool TryParse<T>(ref SequenceReader<byte> reader, out T item);

        public static bool TryReadArrayOfInt(ref this SequenceReader<byte> reader, out int[] value) => reader.TryReadArray(TryReadInt, out value);
        public static bool TryReadArrayOfShort(ref this SequenceReader<byte> reader, out short[] value) => reader.TryReadArray(TryReadShort, out value);
        public static bool TryReadArrayOfString(ref this SequenceReader<byte> reader, out string[] value) => reader.TryReadArray(TryReadString, out value);

        public static bool TryReadByte(ref this SequenceReader<byte> reader, out byte value)
        {
            if (!reader.TryRead(out value))
                return false;
            return true;
        }
        public static bool TryReadShort(ref this SequenceReader<byte> reader, out short value)
        {
            if (!reader.TryReadBigEndian(out value))
                return false;
            return true;
        }
        public static bool TryReadInt(ref this SequenceReader<byte> reader, out int value)
        {
            if (!reader.TryReadBigEndian(out value))
                return false;
            return true;
        }
        public static bool TryReadLong(ref this SequenceReader<byte> reader, out long value)
        {
            if (!reader.TryReadBigEndian(out value))
                return false;
            return true;
        }
        public static bool TryReadBool(ref this SequenceReader<byte> reader, out bool value)
        {
            value = false;
            if (!reader.TryRead(out byte v))
                return false;
            value = v != 0;
            return true;
        }

        public static bool TryReadBytes(ref this SequenceReader<byte> reader, out byte[] value)
        {
            value = null;
            if (!reader.TryReadInt(out int length))
                return false;

            value = new byte[length];
            if (!reader.TryCopyTo(value))
                return false;

            reader.Advance(length);
            return true;
        }

        public static bool TryReadVarint64(ref this SequenceReader<byte> reader, out long result)
        {
            result = 0;
            if (!reader.TryReadVarint64(out ulong value))
                return false;

            if ((value & 0x1) == 0x1)
                result = (-1 * ((long)(value >> 1) + 1));
            else
                result = (long)(value >> 1);
            return true;
        }
        public static bool TryReadVarint32(ref this SequenceReader<byte> reader, out int result)
        {
            result = 0;
            if (!reader.TryReadVarint32(out uint value))
                return false;

            if ((value & 0x1) == 0x1)
                result = (-1 * ((int)(value >> 1) + 1));
            else
                result = (int)(value >> 1);
            return true;
        }
        public static bool TryReadVarint64(ref this SequenceReader<byte> reader, out ulong result)
        {
            int shift = 0;
            result = 0;
            while (shift < 64)
            {
                if (!reader.TryRead(out byte b))
                    return false;
                result |= (ulong)(b & 0x7F) << shift;
                if ((b & 0x80) == 0)
                {
                    return true;
                }
                shift += 7;
            }
            throw new InvalidOperationException("MalformedVarint");
        }
        public static bool TryReadVarint32(ref this SequenceReader<byte> reader, out uint result)
        {
            int shift = 0;
            result = 0;
            while (shift < 32)
            {
                if (!reader.TryRead(out byte b))
                    return false;
                result |= (uint)(b & 0x7F) << shift;
                if ((b & 0x80) == 0)
                {
                    return true;
                }
                shift += 7;
            }
            throw new InvalidOperationException("MalformedVarint");
        }

        public static bool TryReadString(ref this SequenceReader<byte> reader, out string value)
        {
            value = null;
            if (!reader.TryReadBigEndian(out short length))
                return false;
            if (length == -1)
                return true;
            if (length == 0)
            {
                value = string.Empty;
                return true;
            }

            var span = reader.UnreadSpan;
            if (span.Length < length)
                return TryReadMultisegmentUtf8String(ref reader, length, out value);

            var slice = span.Slice(0, length);
            value = Encoding.UTF8.GetString(slice);
            reader.Advance(length);
            return true;
        }
        public static bool TryReadCompactString(ref this SequenceReader<byte> reader, out string value)
        {
            // Represents a sequence of characters. First the length N + 1 is given as an UNSIGNED_VARINT.
            // Then N bytes follow which are the UTF-8 encoding of the character sequence.

            value = null;
            if (!reader.TryReadVarint32(out int length))
                return false;
            if (length == 0)
            {
                value = string.Empty;
                return true;
            }

            var span = reader.UnreadSpan;
            if (span.Length < length)
                return TryReadMultisegmentUtf8String(ref reader, length, out value);

            var slice = span.Slice(0, length);
            value = Encoding.UTF8.GetString(slice);
            reader.Advance(length);
            return true;
        }

        private static unsafe bool TryReadMultisegmentUtf8String(ref SequenceReader<byte> reader, int length, out string value)
        {
            Debug.Assert(reader.UnreadSpan.Length < length);

            // Not enough data in the current segment, try to peek for the data we need.
            // In my use case, these strings cannot be more than 64kb, so stack memory is fine.
            byte* buffer = stackalloc byte[length];
            // Hack because the compiler thinks reader.TryCopyTo could store the span.
            var tempSpan = new Span<byte>(buffer, length);

            if (!reader.TryCopyTo(tempSpan))
            {
                value = default;
                return false;
            }

            value = Encoding.UTF8.GetString(tempSpan);
            reader.Advance(length);
            return true;
        }

        public static bool TryReadCompactBytes(ref this SequenceReader<byte> reader, out ReadOnlySpan<byte> value)
        {
            // Represents a sequence of characters. First the length N + 1 is given as an UNSIGNED_VARINT.
            // Then N bytes follow which are the UTF-8 encoding of the character sequence.

            value = null;
            if (!reader.TryReadVarint32(out int length))
                return false;
            if (length <= 0)
            {
                value = Span<byte>.Empty;
                return true;
            }

            var span = reader.UnreadSpan;
            if (span.Length < length)
                return TryReadMultisegmentBytes(ref reader, length, out value);

            var slice = span.Slice(0, length);
            value = slice;
            reader.Advance(length);
            return true;
        }

        private static unsafe bool TryReadMultisegmentBytes(ref SequenceReader<byte> reader, int length, out ReadOnlySpan<byte> value)
        {
            Debug.Assert(reader.UnreadSpan.Length < length);

            // Not enough data in the current segment, try to peek for the data we need.
            // In my use case, these strings cannot be more than 64kb, so stack memory is fine.
            byte* buffer = stackalloc byte[length];
            // Hack because the compiler thinks reader.TryCopyTo could store the span.
            var tempSpan = new Span<byte>(buffer, length);

            if (!reader.TryCopyTo(tempSpan))
            {
                value = default;
                return false;
            }

            value = tempSpan;
            reader.Advance(length);
            return true;
        }


        public static bool TryReadArray<T>(ref this SequenceReader<byte> reader, TryParse<T> readItem, Action<T[]> setter)
        {
            if (reader.TryReadArray(readItem, out var arr))
                return false;
            setter(arr);
            return true;
        }
        public static bool TryReadArray<T>(ref this SequenceReader<byte> reader, TryParse<T> readItem, out T[] value, bool useVarIntLength = false)
        {
            value = default;
            int count;
            if (useVarIntLength)
            {
                if (!reader.TryReadVarint32(out count))
                    return false;
            }
            else
            {
                if (!reader.TryReadBigEndian(out count))
                    return false;
            }

            if (count == -1)
                return true;
            if (count == 0)
            {
                value = new T[0];
                return true;
            }

            var items = new T[count];
            for (int i = 0; i < count; i++)
            {
                if (!readItem(ref reader, out var item))
                    return false;
                items[i] = item;
            }
            value = items;
            return true;
        }
    }
}
