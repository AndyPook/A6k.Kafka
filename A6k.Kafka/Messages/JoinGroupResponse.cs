namespace A6k.Kafka.Messages
{
    public class JoinGroupResponse
    {
        public int ThrottleTime { get; set; }
        public short ErrorCode { get; set; }
        public int GenerationId { get; set; }
        public string ProtocolName { get; set; }
        public string Leader { get; set; }
        public string MemberId { get; set; }
        public Member[] Members { get; set; }

        public class Member
        {
            public string MemberId { get; set; }
            public string GroupInstanceId { get; set; }
            public byte[] Metadata { get; set; }
        }
    }
}
