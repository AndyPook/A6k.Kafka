namespace A6k.Kafka.Messages
{
    public class JoinGroupRequest
    {
        public string GroupId { get; set; }
        public int SessionTimeout { get; set; }
        public int RebalanceTimeout { get; set; }
        public string MemberId { get; set; }
        public string GroupInstanceId { get; set; }
        public string ProtocolType { get; set; }
        public Protocol[] Protocols { get; set; }

        public class Protocol
        {
            public string Name { get; set; }
            public byte[] Metadata { get; set; }
        }
    }
}
