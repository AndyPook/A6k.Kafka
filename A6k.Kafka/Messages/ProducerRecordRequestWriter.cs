﻿using System;
using System.Buffers;
using Bedrock.Framework.Protocols;

namespace A6k.Kafka.Messages
{
    public class ProducerRecord : IDisposable
    {
        public string Topic { get; set; }
        public int? PartitionId { get; set; }

        public ReadOnlySequence<byte> KeyBytes { get; private set; }

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

            record.KeyBytes = WritePrefixed(buffer, keySerializer, message.Key);
            WritePrefixed(buffer, valueSerializer, message.Value);

            // headers
            buffer.WriteVarInt((ulong)message.HeadersCount);
            message.ForEachHeader(h =>
            {
                buffer.WriteCompactString(h.Key);
                buffer.WritePrefixed(h.Value.AsSpan(), BufferWriterExtensions.PrefixType.VarInt);
            });

            return record;

            ReadOnlySequence<byte> WritePrefixed<T>(IBufferWriter<byte> output, ISerializer<T> serializer, T item)
            {
                using var buffer = new MemoryBufferWriter();
                serializer.WriteMessage(item, buffer);
                output.WriteVarInt(buffer.Length);

                buffer.CopyTo(output);
                return buffer.AsReadOnlySequence;
            }
        }

        private MemoryBufferWriter buffer = new MemoryBufferWriter();

        public int Length => buffer.Length;

        public void CopyTo(IBufferWriter<byte> output)
        {
            output.WriteVarInt(buffer.Length);
            buffer.CopyTo(output);
        }

        public void Dispose() => buffer.Dispose();
    }

    public class ProducerRecordRequestWriter : IMessageWriter<ProducerRecord>
    {
        private string topic;

        public ProducerRecordRequestWriter(string topic)
        {
            this.topic = topic;
        }

        public void WriteMessage(ProducerRecord message, IBufferWriter<byte> output)
        {
            //Produce Request(Version: 7) => transactional_id acks timeout[topic_data]
            //  transactional_id => NULLABLE_STRING
            //  acks => INT16
            //  timeout => INT32
            //  topic_data => topic[data]
            //    topic => STRING
            //    data => partition record_set
            //      partition => INT32
            //      record_set => RECORDS

            output.WriteNullableString(null); // transactional_id => NULLABLE_STRING
            output.WriteShort(-1);            // acks => INT16
            output.WriteInt(5000);            // timeout => INT32
            output.WriteInt(1);            // ???
            output.WriteString(topic);        // topic => STRING

            output.WriteInt(1); // only one message
            output.WriteInt(0); // partitionId

            WriteRecordBatch(message, output);
        }

        public void WriteRecordBatch(ProducerRecord message, IBufferWriter<byte> output)
        {
            // baseOffset: int64
            // batchLength: int32
            // partitionLeaderEpoch: int32
            // magic: int8(current magic value is 2)
            // crc: int32
            // attributes: int16
            //     bit 0~2:
            //         0: no compression
            //         1: gzip
            //         2: snappy
            //         3: lz4
            //         4: zstd
            //     bit 3: timestampType
            //     bit 4: isTransactional(0 means not transactional)
            //     bit 5: isControlBatch(0 means not a control batch)
            //     bit 6~15: unused
            // lastOffsetDelta: int32
            // firstTimestamp: int64
            // maxTimestamp: int64
            // producerId: int64
            // producerEpoch: int16
            // baseSequence: int32
            // records: [Record]


            // crc
            using var buffer = new MemoryBufferWriter();

            buffer.WriteUShort(0b0000_0000_0000_0000); // attributes: int16
            buffer.WriteInt(0);  // lastOffsetDelta: int32

            var now = DateTime.UtcNow.DateTimeToUnixTimestampMs();
            buffer.WriteLong(now); // firstTimestamp: int64
            buffer.WriteLong(now); // maxTimestamp: int64
            buffer.WriteLong(-1);  // producerId: int64
            buffer.WriteShort(-1); // producerEpoch: int16
            buffer.WriteInt(-1);   // baseSequence: int32

            buffer.WriteInt(1); // one record in the batch

            WriteRecord(message, buffer);

            output.WriteInt(12 + 4 + 1 + 4 + (int)buffer.Length); // size of records + "header" bytes (not documented)

            output.WriteLong(0); // baseOffset: int64
            output.WriteInt(4 + 1 + 4 + (int)buffer.Length); // batchLength: int32
            output.WriteInt(0);  // partitionLeaderEpoch: int32
            output.WriteByte(2); // magic: int8(current magic value is 2)

            var crc = Hash.Crc32C.Compute(buffer);
            output.WriteUInt(crc);  // crc: int32
            buffer.CopyTo(output);
        }

        public void WriteRecord(ProducerRecord record, IBufferWriter<byte> output)
        {
            output.WriteVarInt(record.Length);
            record.CopyTo(output);
        }
    }
}
