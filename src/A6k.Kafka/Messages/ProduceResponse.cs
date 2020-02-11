using System.Collections.Generic;

namespace A6k.Kafka.Messages
{
    public class ProduceResponse
    {
        public ProduceResponse(TopicResponse[] topicResponses, int throttleTime)
        {
            Topics = topicResponses;
            ThrottleTime = throttleTime;
        }

        public IReadOnlyList<TopicResponse> Topics { get; }
        public int ThrottleTime { get; }

        public class TopicResponse
        {
            public TopicResponse(string topic, PartitionResponse[] partitions)
            {
                Topic = topic;
                Partitions = partitions;
            }

            public string Topic { get; }
            public IReadOnlyList<PartitionResponse> Partitions { get; }

            public class PartitionResponse
            {
                public PartitionResponse(int partition, short errorCode, short baseOffset, long logAppendTime, long logStartOffset) //, RecordError[] recordErrors)
                {
                    Partition = partition;
                    ErrorCode = (ResponseError)errorCode;
                    BaseOffset = baseOffset;
                    LogAppendTime = logAppendTime;
                    LogStartOffset = logStartOffset;
                    //RecordErrors = recordErrors;
                }

                public int Partition { get; }
                public ResponseError ErrorCode { get; }
                public short BaseOffset { get; }
                public long LogAppendTime { get; }
                public long LogStartOffset { get; }

                //public RecordError[] RecordErrors { get; }

                //public class RecordError
                //{
                //    public RecordError(int batchIndex, string batchIndexErrorMessage)
                //    {
                //        BatchIndex = batchIndex;
                //        BatchIndexErrorMessage = batchIndexErrorMessage;
                //    }

                //    public int BatchIndex { get; }
                //    public string BatchIndexErrorMessage { get; }
                //}
            }
        }
    }
}
