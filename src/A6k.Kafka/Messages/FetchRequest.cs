namespace A6k.Kafka.Messages
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
}
