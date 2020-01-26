namespace A6k.Kafka.Messages
{
    public class HeartbeatResponse
    {
        public HeartbeatResponse(short errorCode)
        {
            ErrorCode = (ResponseError)errorCode;
        }

        public ResponseError ErrorCode { get; }
    }
}
