using System.Buffers;
using System.Collections.Generic;

namespace A6k.Kafka.Messages
{
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
}
