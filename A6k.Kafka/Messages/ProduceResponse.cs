namespace A6k.Kafka.Messages
{
    public class ProduceResponse
    {
        public Response[] Responses { get; set; }
        public int ThrottleTime { get; set; }

        public class Response
        {
            public string Topic { get; set; }
            public PartitionResponse[] Partitions { get; set; }

            public class PartitionResponse
            {
                public PartitionResponse(int partition, short errorCode, short baseOffset, long logAppendTime, long logStartOffset) //, RecordError[] recordErrors)
                {
                    Partition = partition;
                    ErrorCode = errorCode;
                    BaseOffset = baseOffset;
                    LogAppendTime = logAppendTime;
                    LogStartOffset = logStartOffset;
                    //RecordErrors = recordErrors;
                }

                public int Partition { get; }
                public short ErrorCode { get; }
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
