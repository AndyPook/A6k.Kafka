using System;
using System.Buffers;

namespace A6k.Kafka.Messages
{
    public class ProducerRecord : IDisposable
    {
        public static ProducerRecord Create<TKey, TValue>(Message<TKey, TValue> message, ISerializer<TKey> keySerializer, ISerializer<TValue> valueSerializer)
        {
            // Record...
            // length: varint
            // attributes: int8
            //     bit 0~7: unused
            // timestampDelta: varint
            // offsetDelta: varint
            // keyLength: varint
            // key: byte[]
            // valueLen: varint
            // value: byte[]
            // Headers => [Header]

            // Header...
            // headerKeyLength: varint
            // headerKey: String
            // headerValueLength: varint
            // Value: byte[]

            var record = new ProducerRecord
            {
                Topic = message.Topic,
                PartitionId = message.PartitionId
            };

            var buffer = record.buffer;

            buffer.WriteByte(0);         // attributes: int8 - bit 0~7: unused
            buffer.WriteVarInt((uint)0); // timestampDelta: varint
            buffer.WriteVarInt((uint)0); // offsetDelta: varint

            record.keyBuffer = WritePrefixed(buffer, keySerializer, message.Key);
            WritePrefixed(buffer, valueSerializer, message.Value);

            // headers
            buffer.WriteVarInt((ulong)message.HeadersCount);
            message.ForEachHeader(h =>
            {
                buffer.WriteCompactString(h.Key);
                buffer.WritePrefixed(h.Value.AsSpan(), BufferWriterExtensions.PrefixType.VarInt);
            });

            return record;

            MemoryBufferWriter WritePrefixed<T>(IBufferWriter<byte> output, ISerializer<T> serializer, T item)
            {
                var buffer = new MemoryBufferWriter();
                serializer.WriteMessage(item, buffer);
                output.WriteVarInt(buffer.Length);

                buffer.CopyTo(output);
                return buffer;
            }
        }

        private MemoryBufferWriter buffer = new MemoryBufferWriter();
        private MemoryBufferWriter keyBuffer = new MemoryBufferWriter();

        public string Topic { get; set; }
        public int? PartitionId { get; set; }

        public ReadOnlySequence<byte> KeyBytes => keyBuffer.AsReadOnlySequence;

        public void WriteTo(IBufferWriter<byte> output)
        {
            output.WriteVarInt(buffer.Length);
            buffer.CopyTo(output);
        }

        public void Dispose()
        {
            buffer?.Dispose();
            keyBuffer?.Dispose();
        }
    }
}
