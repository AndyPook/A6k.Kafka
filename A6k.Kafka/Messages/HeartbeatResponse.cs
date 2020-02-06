namespace A6k.Kafka.Messages
{
    public class HeartbeatResponse
    {
        public HeartbeatResponse(int throttleTime, short errorCode)
        {
            ThrottleTime = throttleTime;
            ErrorCode = (ResponseError)errorCode;
        }

        public int ThrottleTime { get; }
        public ResponseError ErrorCode { get; }
    }
}
