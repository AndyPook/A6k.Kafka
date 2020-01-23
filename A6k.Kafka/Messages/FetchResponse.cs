using System.Collections.Generic;

namespace A6k.Kafka.Messages
{
    public class FetchResponse
    {
        public FetchResponse(int throttleTime, short errorCode, int sessionId, IReadOnlyCollection<Response> responses)
        {
            ThrottleTime = throttleTime;
            ErrorCode = errorCode;
            SessionId = sessionId;
            Responses = responses;
        }

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

        public int ThrottleTime { get; }
        public short ErrorCode { get; }
        public int SessionId { get; }
        public IReadOnlyCollection<Response> Responses { get; }

        public class Response
        {
            public Response(string topicName, IReadOnlyCollection<PartitionResponse> partitionResponses)
            {
                TopicName = topicName;
                PartitionResponses = partitionResponses;
            }

            public string TopicName { get; }
            public IReadOnlyCollection<PartitionResponse> PartitionResponses { get; }

            public class PartitionResponse
            {
                public PartitionResponse(
                    int partitionId,
                    short errorCode)
                {
                    PartitionId = partitionId;
                    ErrorCode = errorCode;
                }

                public PartitionResponse(
                    int partitionId,
                    long highWaterMark,
                    long lastStableOffset,
                    long logStartOffset,
                    AbortedTransaction[] abortedTransactions,
                    int preferredReadReplica,
                    IReadOnlyCollection<RecordBatch> recordBatches)
                {
                    PartitionId = partitionId;
                    HighWaterMark = highWaterMark;
                    LastStableOffset = lastStableOffset;
                    LogStartOffset = logStartOffset;
                    AbortedTransactions = abortedTransactions;
                    PreferredReadReplica = preferredReadReplica;
                    RecordBatches = recordBatches;
                }

                public int PartitionId { get; }
                public short ErrorCode { get; }
                public long HighWaterMark { get; }
                public long LastStableOffset { get; }
                public long LogStartOffset { get; }
                public AbortedTransaction[] AbortedTransactions { get; }
                public int PreferredReadReplica { get; }

                public IReadOnlyCollection<RecordBatch> RecordBatches { get; }

                public class AbortedTransaction
                {
                    public AbortedTransaction(long producerId, long firstOffset)
                    {
                        ProducerId = producerId;
                        FirstOffset = firstOffset;
                    }

                    public long ProducerId { get; }
                    public long FirstOffset { get; }
                }
            }
        }
    }
}
