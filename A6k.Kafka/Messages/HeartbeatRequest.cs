namespace A6k.Kafka.Messages
{
    public class HeartbeatRequest
    {
        public string GroupId { get; set; }
        public int GenerationId { get; set; }
        public string MemberId { get; set; }
    }
}
