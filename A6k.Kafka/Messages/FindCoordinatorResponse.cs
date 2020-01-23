namespace A6k.Kafka.Messages
{
    public class FindCoordinatorResponse
    {
        public int ThrottleTime { get; set; }
        public short ErrorCode { get; set; }
        public string ErrorMessage { get; set; }
        public int NodeId { get; set; }
        public string Host { get; set; }
        public int Port { get; set; }
    }
}
