﻿using System;
using System.Buffers;
using Bedrock.Framework.Protocols;

namespace A6k.Kafka.Messages
{
    public class ProducerRecordRequestWriter : IMessageWriter<ProducerRecord>
    {
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

            output.WriteNullableString(null);  // transactional_id => NULLABLE_STRING
            output.WriteShort(-1);             // acks => INT16
            output.WriteInt(5000);             // timeout => INT32
            output.WriteInt(1);                // ???
            output.WriteString(message.Topic); // topic => STRING

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

            message.WriteTo(buffer);

            output.WriteInt(12 + 4 + 1 + 4 + buffer.Length); // size of records + "header" bytes (not documented)

            output.WriteLong(0); // baseOffset: int64
            output.WriteInt(4 + 1 + 4 + buffer.Length); // batchLength: int32
            output.WriteInt(0);  // partitionLeaderEpoch: int32
            output.WriteByte(2); // magic: int8(current magic value is 2)

            var crc = Hash.Crc32C.Compute(buffer);
            output.WriteUInt(crc);  // crc: int32
            buffer.CopyTo(output);
        }
    }
}
