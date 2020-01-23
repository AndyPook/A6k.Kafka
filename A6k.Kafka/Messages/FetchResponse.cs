namespace A6k.Kafka.Messages
{
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
}
