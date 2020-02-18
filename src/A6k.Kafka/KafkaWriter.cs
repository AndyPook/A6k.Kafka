using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Text;
using Bedrock.Framework.Protocols;

namespace A6k.Kafka
{
    public delegate void KafkaWriterDelegate<TItem>(TItem item, KafkaWriter writer);

    public ref partial struct KafkaWriter
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteByte(byte num)
        {
            var buffer = GetSpan(1);
            buffer[0] = num;
            Advance(1);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteBytes(byte[] array)
        {
            if (array == null || array.Length == 0)
            {
                WriteInt(0);
                return;
            }

            WriteInt(array.Length);
            Write(array);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteShort(short num)
        {
            var buffer = GetSpan(2);
            BinaryPrimitives.WriteInt16BigEndian(buffer, num);
            Advance(2);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteUShort(ushort num)
        {
            var buffer = GetSpan(2);
            BinaryPrimitives.WriteUInt16BigEndian(buffer, num);
            Advance(2);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteInt(int num)
        {
            var buffer = GetSpan(4);
            BinaryPrimitives.WriteInt32BigEndian(buffer, num);
            Advance(4);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteUInt(uint num)
        {
            var buffer = GetSpan(4);
            BinaryPrimitives.WriteUInt32BigEndian(buffer, num);
            Advance(4);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteLong(long num)
        {
            var buffer = GetSpan(8);
            BinaryPrimitives.WriteInt64BigEndian(buffer, num);
            Advance(8);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteULong(ulong num)
        {
            var buffer = GetSpan(8);
            BinaryPrimitives.WriteUInt64BigEndian(buffer, num);
            Advance(8);
        }

        /// <summary>
        /// Represents a sequence of characters. First the length N is given as an INT16.
        /// Then N bytes follow which are the UTF-8 encoding of the character sequence.
        /// Length must not be negative.
        /// </summary>
        /// <param name="output"></param>
        /// <param name="text"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteString(string text)
        {
            if (string.IsNullOrEmpty(text))
            {
                WriteShort(0);
                return;
            }

            var textLength = Encoding.UTF8.GetByteCount(text);
            WriteShort((short)textLength);

            var textSpan = GetSpan(textLength);
            Encoding.UTF8.GetBytes(text, textSpan);
            Advance(textLength);
        }

        /// <summary>
        /// Represents a sequence of characters or null. For non-null strings, first the length N is given as an INT16.
        /// Then N bytes follow which are the UTF-8 encoding of the character sequence.
        /// A null value is encoded with length of -1 and there are no following bytes.
        /// </summary>
        /// <param name="output"></param>
        /// <param name="text"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteNullableString(string text)
        {
            if (text == null)
            {
                WriteShort(-1);
                return;
            }
            else if (text.Length == 0)
            {
                WriteShort(0);
                return;
            }

            var textLength = Encoding.UTF8.GetByteCount(text);
            WriteShort((short)textLength);

            var textSpan = GetSpan(textLength);
            Encoding.UTF8.GetBytes(text, textSpan);
            Advance(textLength);
        }

        /// <summary>
        /// Represents a sequence of characters. First the length N + 1 is given as an UNSIGNED_VARINT.
        /// Then N bytes follow which are the UTF-8 encoding of the character sequence.
        /// </summary>
        /// <param name="output"></param>
        /// <param name="text"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteCompactString(string text)
        {
            if (string.IsNullOrEmpty(text))
            {
                WriteVarInt(1);
                return;
            }

            var textLength = Encoding.UTF8.GetByteCount(text);
            WriteVarInt((uint)textLength + 1);

            var textSpan = GetSpan(textLength);
            Encoding.UTF8.GetBytes(text, textSpan);
            Advance(textLength);
        }

        /// <summary>
        /// Represents a sequence of characters. First the length N + 1 is given as an UNSIGNED_VARINT.
        /// Then N bytes follow which are the UTF-8 encoding of the character sequence.
        /// A null string is represented with a length of 0.
        /// </summary>
        /// <param name="output"></param>
        /// <param name="text"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteCompactNullableString(string text)
        {
            if (text == null)
            {
                WriteVarInt(0);
                return;
            }
            else if (text.Length == 0)
            {
                WriteVarInt(1);
                return;
            }

            var textLength = Encoding.UTF8.GetByteCount(text);
            WriteVarInt((uint)textLength + 1);

            var textSpan = GetSpan(textLength);
            Encoding.UTF8.GetBytes(text, textSpan);
            Advance(textLength);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteVarInt(int num)
        {
            var n = (uint)((num << 1) ^ (num >> 31));
            WriteVarInt((ulong)n);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteVarInt(long num)
        {
            var n = (ulong)((num << 1) ^ (num >> 63));
            WriteVarInt(n);
        }

        //public void WriteVarInt(int num) => WriteVarInt((ulong)num);
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteVarInt(uint num) => WriteVarInt((ulong)num);
        //public void WriteVarInt(long num) => WriteVarInt((ulong)num);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteVarInt(ulong num)
        {
            Span<byte> buffer = GetSpan(9);

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

            Advance(numBytes);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteArray<TItem>(TItem[] array, KafkaWriterDelegate<TItem> writer)
        {
            if (array == null)
            {
                WriteInt(0);
                return;
            }

            WriteInt(array.Length);
            for (int i = 0; i < array.Length; i++)
                writer(array[i], this);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteArray(int[] array)
        {
            if (array == null)
            {
                WriteInt(0);
                return;
            }

            WriteInt(array.Length);
            for (int i = 0; i < array.Length; i++)
                WriteInt(array[i]);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteArray(string[] array)
        {
            if (array == null)
            {
                WriteInt(0);
                return;
            }

            WriteInt(array.Length);
            for (int i = 0; i < array.Length; i++)
                WriteString(array[i]);
        }

        public enum PrefixType
        {
            Int,
            VarInt,
            UnsignedVarInt,
            Crc
        }
        public void WritePrefixed<TItem>(ISerializer<TItem> writer, TItem item, PrefixType prefixType)
        {
            using var buffer = new MemoryBufferWriter();
            writer.WriteMessage(item, buffer);

            switch (prefixType)
            {
                case PrefixType.Int:
                    WriteInt(buffer.Length);
                    break;
                case PrefixType.VarInt:
                    WriteVarInt(buffer.Length);
                    break;
                case PrefixType.UnsignedVarInt:
                    WriteVarInt((ulong)buffer.Length);
                    break;
                case PrefixType.Crc:
                    var crc = Hash.Crc32C.Compute(buffer);
                    WriteUInt(crc);
                    break;
            }

            Write(buffer.AsReadOnlySequence);
        }
        public void WritePrefixed<TItem>(IMessageWriter<TItem> writer, TItem item, PrefixType prefixType)
        {
            using var buffer = new MemoryBufferWriter();
            writer.WriteMessage(item, buffer);

            switch (prefixType)
            {
                case PrefixType.Int:
                    WriteInt(buffer.Length);
                    break;
                case PrefixType.VarInt:
                    WriteVarInt(buffer.Length);
                    break;
                case PrefixType.UnsignedVarInt:
                    WriteVarInt((ulong)buffer.Length);
                    break;
                case PrefixType.Crc:
                    var crc = Hash.Crc32C.Compute(buffer);
                    WriteUInt(crc);
                    break;
            }

            Write(buffer.AsReadOnlySequence);
        }
        public void WritePrefixed<TItem>(Action<TItem, IBufferWriter<byte>> writer, TItem item, PrefixType prefixType)
        {
            using var buffer = new MemoryBufferWriter();
            writer(item, buffer);

            switch (prefixType)
            {
                case PrefixType.Int:
                    WriteInt(buffer.Length);
                    break;
                case PrefixType.VarInt:
                    WriteVarInt(buffer.Length);
                    break;
                case PrefixType.UnsignedVarInt:
                    WriteVarInt((ulong)buffer.Length);
                    break;
                case PrefixType.Crc:
                    var crc = Hash.Crc32C.Compute(buffer);
                    WriteUInt(crc);
                    break;
            }

            Write(buffer.AsReadOnlySequence);
        }
        public void WritePrefixed(ReadOnlySpan<byte> item, PrefixType prefixType)
        {
            switch (prefixType)
            {
                case PrefixType.Int:
                    WriteInt(item.Length);
                    break;
                case PrefixType.VarInt:
                    WriteVarInt(item.Length);
                    break;
                case PrefixType.UnsignedVarInt:
                    WriteVarInt((ulong)item.Length);
                    break;
                case PrefixType.Crc:
                    throw new InvalidOperationException("CRC not supported here");
            }

            // Try to minimize segments in the target writer by hinting at the total size.
            var buffer = GetSpan(item.Length);
            item.CopyTo(buffer);
            Advance(item.Length);
        }
    }

    /// <summary>
    /// A fast access struct that wraps <see cref="IBufferWriter{T}"/>.
    /// </summary>
    /// <typeparam name="T">The type of element to be written.</typeparam>
    public ref partial struct KafkaWriter
    {
        /// <summary>
        /// The underlying <see cref="IBufferWriter{T}"/>.
        /// </summary>
        private readonly IBufferWriter<byte> output;

        /// <summary>
        /// The result of the last call to <see cref="IBufferWriter{T}.GetSpan(int)"/>, less any bytes already "consumed" with <see cref="Advance(int)"/>.
        /// Backing field for the <see cref="Span"/> property.
        /// </summary>
        private Span<byte> span;

        /// <summary>
        /// The number of uncommitted bytes (all the calls to <see cref="Advance(int)"/> since the last call to <see cref="Commit"/>).
        /// </summary>
        private int buffered;

        /// <summary>
        /// The total number of bytes written with this writer.
        /// Backing field for the <see cref="BytesCommitted"/> property.
        /// </summary>
        private long bytesCommitted;

        /// <summary>
        /// Initializes a new instance of the <see cref="BufferWriter{T}"/> struct.
        /// </summary>
        /// <param name="output">The <see cref="IBufferWriter{T}"/> to be wrapped.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public KafkaWriter(IBufferWriter<byte> output)
        {
            buffered = 0;
            bytesCommitted = 0;
            this.output = output;
            span = output.GetSpan();
        }

        /// <summary>
        /// Calls <see cref="IBufferWriter{T}.Advance(int)"/> on the underlying writer
        /// with the number of uncommitted bytes.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Commit()
        {
            var buffered = this.buffered;
            if (buffered > 0)
            {
                bytesCommitted += buffered;
                this.buffered = 0;
                output.Advance(buffered);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="count"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Span<byte> GetSpan(int count)
        {
            Ensure(count);
            return span.Slice(0, count);
        }

        /// <summary>
        /// Used to indicate that part of the buffer has been written to.
        /// </summary>
        /// <param name="count">The number of bytes written to.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Advance(int count)
        {
            buffered += count;
            span = span.Slice(count);
        }

        /// <summary>
        /// Copies the caller's buffer into this writer and calls <see cref="Advance(int)"/> with the length of the source buffer.
        /// </summary>
        /// <param name="source">The buffer to copy in.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Write(in ReadOnlySpan<byte> source)
        {
            if (span.Length >= source.Length)
            {
                source.CopyTo(span);
                Advance(source.Length);
            }
            else
            {
                WriteMultiBuffer(source);
            }
        }
        public void Write(in ReadOnlySequence<byte> source)
        {
            foreach (var span in source)
                Write(span.Span);
        }

        /// <summary>
        /// Acquires a new buffer if necessary to ensure that some given number of bytes can be written to a single buffer.
        /// </summary>
        /// <param name="count">The number of bytes that must be allocated in a single buffer.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Ensure(int count = 1)
        {
            if (span.Length < count)
            {
                EnsureMore(count);
            }
        }

        /// <summary>
        /// Gets a fresh span to write to, with an optional minimum size.
        /// </summary>
        /// <param name="count">The minimum size for the next requested buffer.</param>
        [MethodImpl(MethodImplOptions.NoInlining)]
        private void EnsureMore(int count = 0)
        {
            if (buffered > 0)
            {
                Commit();
            }

            span = output.GetSpan(count);
        }

        /// <summary>
        /// Copies the caller's buffer into this writer, potentially across multiple buffers from the underlying writer.
        /// </summary>
        /// <param name="source">The buffer to copy into this writer.</param>
        private void WriteMultiBuffer(ReadOnlySpan<byte> source)
        {
            while (source.Length > 0)
            {
                if (span.Length == 0)
                {
                    EnsureMore();
                }

                var writable = Math.Min(source.Length, span.Length);
                source.Slice(0, writable).CopyTo(span);
                source = source.Slice(writable);
                Advance(writable);
            }
        }
    }

}
