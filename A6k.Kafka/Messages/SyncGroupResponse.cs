namespace A6k.Kafka.Messages
{
    public class SyncGroupResponse
    {
        public SyncGroupResponse(int throttleTime, short errorCode, byte[] assignment)
        {
            ThrottleTime = throttleTime;
            ErrorCode = (ResponseError)errorCode;
            Assignment = assignment;
        }

        public int ThrottleTime { get; }
        public ResponseError ErrorCode { get; }
        public byte[] Assignment { get; }
    }
}
