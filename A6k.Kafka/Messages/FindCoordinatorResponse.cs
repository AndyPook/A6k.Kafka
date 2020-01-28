namespace A6k.Kafka.Messages
{
    public class FindCoordinatorResponse
    {
        public FindCoordinatorResponse(int throttleTime, short errorCode, string errorMessage, int nodeId, string host, int port)
        {
            ThrottleTime = throttleTime;
            ErrorCode = (ResponseError)errorCode;
            ErrorMessage = errorMessage;
            NodeId = nodeId;
            Host = host;
            Port = port;
        }

        public int ThrottleTime { get; }
        public ResponseError ErrorCode { get; }
        public string ErrorMessage { get; }
        public int NodeId { get; }
        public string Host { get; }
        public int Port { get; }
    }
}
