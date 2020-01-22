using System;
using System.Buffers;
using System.Collections.Generic;
using System.Text;
using Bedrock.Framework.Protocols;

namespace A6k.Kafka
{
    public class FetchRequest
    {
        //Fetch Request(Version: 11) => replica_id max_wait_time min_bytes max_bytes isolation_level session_id session_epoch[topics] [forgotten_topics_data]
        //  rack_id
        //  replica_id => INT32
        //  max_wait_time => INT32
        //  min_bytes => INT32
        //  max_bytes => INT32
        //  isolation_level => INT8
        //  session_id => INT32
        //  session_epoch => INT32
        //  topics => topic[partitions]
        //    topic => STRING
        //    partitions => partition current_leader_epoch fetch_offset log_start_offset partition_max_bytes
        //      partition => INT32
        //      current_leader_epoch => INT32
        //      fetch_offset => INT64
        //      log_start_offset => INT64
        //      partition_max_bytes => INT32
        //  forgotten_topics_data => topic[partitions]
        //    topic => STRING
        //    partitions => INT32
        //  rack_id => STRING

        public int ReplicaId { get; set; }
        public int MaxWaitTime { get; set; }
        public int MinBytes { get; set; }
        public int MaxBytes { get; set; }
        public byte IsolationLevel { get; set; }
        public int SessionId { get; set; }
        public int SessionEpoc { get; set; }
        public Topic[] Topics { get; set; }
        public ForgottenTopicsData[] ForgottenTopics { get; set; }
        public string Rack { get; set; }

        public class Topic
        {
            public string TopicName { get; set; }
            public Partition[] Partitions { get; set; }

            public class Partition
            {
                public int PartitionId { get; set; }
                public int CurrentLeaderEpoc { get; set; }
                public long FetchOffset { get; set; }
                public long LogStartOffset { get; set; }
                public int PartitionMaxBytes { get; set; }
            }
        }

        public class ForgottenTopicsData
        {
            public string TopicName { get; set; }
            public int[] Partitions { get; set; }
        }
    }

    public class FetchResponse
    {
        //Fetch Response(Version: 11) => throttle_time_ms error_code session_id[responses]
        //  throttle_time_ms => INT32
        //  error_code => INT16
        //  session_id => INT32
        //  responses => topic[partition_responses]
        //    topic => STRING
        //    partition_responses => partition_header record_set
        //      partition_header => partition error_code high_watermark last_stable_offset log_start_offset[aborted_transactions] preferred_read_replica
        //       partition => INT32
        //        error_code => INT16
        //        high_watermark => INT64
        //        last_stable_offset => INT64
        //        log_start_offset => INT64
        //        aborted_transactions => producer_id first_offset
        //          producer_id => INT64
        //          first_offset => INT64
        //        preferred_read_replica => INT32
        //      record_set => RECORDS

        public int ThrottleTime { get; set; }
        public short ErrorCode { get; set; }
        public int SessionId { get; set; }
        public Response[] Responses { get; set; }

        public class Response
        {
            public string TopicName { get; set; }
            public PartitionResponse[] PartitionResponses { get; set; }

            public class PartitionResponse
            {
                public int PartitionId { get; set; }
                public short ErrorCode { get; set; }
                public long HighWaterMark { get; set; }
                public long LastStableOffset { get; set; }
                public long LogStartOffset { get; set; }
                public AbortedTransaction[] AbortedTransactions { get; set; }
                public int PreferredReadReplica { get; set; }

                public RecordBatch[] RecordBatches { get; set; }

                public class AbortedTransaction
                {
                    public long ProducerId { get; set; }
                    public long FirstOffset { get; set; }
                }
            }
        }
    }

    public class FetchRequestWriter : IMessageWriter<FetchRequest>
    {
        public void WriteMessage(FetchRequest message, IBufferWriter<byte> output)
        {
            output.WriteInt(message.ReplicaId);
            output.WriteInt(message.MaxWaitTime);
            output.WriteInt(message.MinBytes);
            output.WriteInt(message.MaxBytes);
            output.WriteByte(message.IsolationLevel);
            output.WriteInt(message.SessionId);
            output.WriteInt(message.SessionEpoc);
            output.WriteArray(message.Topics, WriteTopic);
            output.WriteArray(message.ForgottenTopics, WriteForgottenTopic);
            output.WriteString(message.Rack);
        }

        public void WriteTopic(FetchRequest.Topic message, IBufferWriter<byte> output)
        {
            output.WriteString(message.TopicName);
            output.WriteArray(message.Partitions, WriteTopicPartition);
        }

        public void WriteTopicPartition(FetchRequest.Topic.Partition message, IBufferWriter<byte> output)
        {
            output.WriteInt(message.PartitionId);
            output.WriteInt(message.CurrentLeaderEpoc);
            output.WriteLong(message.FetchOffset);
            output.WriteLong(message.LogStartOffset);
            output.WriteInt(message.PartitionMaxBytes);
        }

        public void WriteForgottenTopic(FetchRequest.ForgottenTopicsData message, IBufferWriter<byte> output)
        {
            output.WriteString(message.TopicName);
            output.WriteArray(message.Partitions, (x, o) => o.WriteInt(x));
        }
    }

    public class FetchResponseReader : KafkaResponseReader<FetchResponse>
    {
        protected override bool TryParseMessage(ref SequenceReader<byte> reader, out FetchResponse message)
        {
            message = default;
            if (!reader.TryReadInt(out var throttleTime))
                return false;
            if (!reader.TryReadShort(out var errorCode))
                return false;
            if (!reader.TryReadInt(out var sessionId))
                return false;

            if (!reader.TryReadArray<FetchResponse.Response>(TryParseReponse, out var responses))
                return false;

            message = new FetchResponse
            {
                ThrottleTime = throttleTime,
                ErrorCode = errorCode,
                SessionId = sessionId,
                Responses = responses
            };
            return true;
        }

        private bool TryParseReponse(ref SequenceReader<byte> reader, out FetchResponse.Response message)
        {
            message = default;
            if (!reader.TryReadString(out var topicName))
                return false;
            if (!reader.TryReadArray<FetchResponse.Response.PartitionResponse>(TryParsePartitionResponse, out var responses))
                return false;

            message = new FetchResponse.Response
            {
                TopicName = topicName,
                PartitionResponses = responses
            };
            return true;
        }

        private bool TryParsePartitionResponse(ref SequenceReader<byte> reader, out FetchResponse.Response.PartitionResponse message)
        {
            message = default;
            if (!reader.TryReadInt(out var partitionId))
                return false;
            if (!reader.TryReadShort(out var errorCode))
                return false;
            if (!reader.TryReadLong(out var highWaterMark))
                return false;
            if (!reader.TryReadLong(out var lastStableOffset))
                return false;
            if (!reader.TryReadLong(out var logStartOffset))
                return false;
            if (!reader.TryReadArray<FetchResponse.Response.PartitionResponse.AbortedTransaction>(TryParseAbortedTx, out var aborted))
                return false;
            if (!reader.TryReadInt(out var perferredReadReplica))
                return false;

            if (!reader.TryReadInt(out var messageSetSize))
                return false;

            var startPos = reader.Consumed;
            var batches = new List<RecordBatch>();
            while (reader.Consumed - startPos < messageSetSize)
            {
                if (!TryParseRecordBatch(ref reader, out var recordBatch))
                    return false;
                batches.Add(recordBatch);
            }

            message = new FetchResponse.Response.PartitionResponse
            {
                PartitionId = partitionId,
                ErrorCode = errorCode,
                HighWaterMark = highWaterMark,
                LastStableOffset = lastStableOffset,
                LogStartOffset = logStartOffset,
                PreferredReadReplica = perferredReadReplica,
                RecordBatches = batches.ToArray()
            };
            return true;
        }

        private bool TryParseAbortedTx(ref SequenceReader<byte> reader, out FetchResponse.Response.PartitionResponse.AbortedTransaction message)
        {
            message = default;
            if (!reader.TryReadLong(out var producerId))
                return false; ;
            if (!reader.TryReadLong(out var firstOffset))
                return false;

            message = new FetchResponse.Response.PartitionResponse.AbortedTransaction
            {
                ProducerId = producerId,
                FirstOffset = firstOffset
            };
            return true;
        }

        private bool TryParseRecordBatch(ref SequenceReader<byte> reader, out RecordBatch message)
        {
            message = default;
            if (!reader.TryReadLong(out var baseOffset))
                return false;
            if (!reader.TryReadInt(out var batchLength))
                return false;
            if (reader.Remaining < batchLength)
                return false;
            var startPos = reader.Consumed;

            if (!reader.TryReadInt(out var partitionLeaderEpoc))
                return false;
            if (!reader.TryReadByte(out var magic))
                return false;
            if (!reader.TryReadInt(out var crc))
                return false;
            if (!reader.TryReadShort(out var attributes))
                return false;
            if (!reader.TryReadInt(out var lastOffsetDelta))
                return false;
            if (!reader.TryReadLong(out var firstTimeStamp))
                return false;
            if (!reader.TryReadLong(out var maxTimeStamp))
                return false;
            if (!reader.TryReadLong(out var producerId))
                return false;
            if (!reader.TryReadShort(out var producerEpoc))
                return false;
            if (!reader.TryReadInt(out var baseSequence))
                return false;


            if (!reader.TryReadInt(out var x))
                return false;

            var records = new List<RecordBatch.Record>();
            while (reader.Consumed - startPos < batchLength)
            {
                if (!TryParseRecord(ref reader, baseOffset, out var r))
                    return false;
                records.Add(r);
            }

            message = new RecordBatch
            {
                BaseOffset = baseOffset,
                BatchLength = batchLength,
                PartitionLeaderEpoc = partitionLeaderEpoc,
                Magic = magic,
                Crc = crc,
                Attributes = attributes,
                LastOffsetDelta = lastOffsetDelta,
                FirstTimeStamp = firstTimeStamp,
                MaxTimeStamp = maxTimeStamp,
                ProducerId = producerId,
                ProducerEpoc = producerEpoc,
                BaseSequence = baseSequence,
                Records = records.ToArray()
            };
            return true;
        }

        private bool TryParseRecord(ref SequenceReader<byte> reader, long baseOffset, out RecordBatch.Record message)
        {
            message = default;
            if (!reader.TryReadVarint64(out long length))
                return false;
            if (!reader.TryReadByte(out var attributes))
                return false;
            if (!reader.TryReadVarint64(out long timestampDelta))
                return false;
            if (!reader.TryReadVarint64(out long offsetDelta))
                return false;
            if (!reader.TryReadCompactBytes(out var key))
                return false;
            if (!reader.TryReadCompactBytes(out var value))
                return false;
            if (!reader.TryReadArray<RecordBatch.Record.Header>(TryParseRecordHeader, out var headers, useVarIntLength: true))
                return false;

            message = new RecordBatch.Record
            {
                Attributes = attributes,
                TimeStampDelta = timestampDelta,
                OffsetDelta = offsetDelta,
                Offset = baseOffset + offsetDelta,
                Key = key.ToArray(),
                Value = value.ToArray(),
                Headers = headers
            };
            return true;
        }

        private bool TryParseRecordHeader(ref SequenceReader<byte> reader, out RecordBatch.Record.Header message)
        {
            message = default;
            if (!reader.TryReadCompactString(out string key))
                return false;
            if (!reader.TryReadCompactBytes(out var value))
                return false;

            message = new RecordBatch.Record.Header
            {
                Key = key,
                Value = value.ToArray()
            };
            return true;
        }
    }

    public class RecordBatch
    {
        public long BaseOffset { get; set; }
        public int BatchLength { get; set; }
        public int PartitionLeaderEpoc { get; set; }
        public int Magic { get; set; }
        public int Crc { get; set; }
        public short Attributes { get; set; }
        public int LastOffsetDelta { get; set; }
        public long FirstTimeStamp { get; set; }
        public long MaxTimeStamp { get; set; }
        public long ProducerId { get; set; }
        public short ProducerEpoc { get; set; }
        public int BaseSequence { get; set; }
        public Record[] Records { get; set; }

        public class Record
        {
            public byte Attributes { get; set; }
            public long TimeStampDelta { get; set; }
            public long OffsetDelta { get; set; }
            public long Offset { get; set; }
            public byte[] Key { get; set; }
            public byte[] Value { get; set; }
            public Header[] Headers { get; set; }

            public class Header
            {
                public string Key { get; set; }
                public byte[] Value { get; set; }
            }
        }
    }
}
