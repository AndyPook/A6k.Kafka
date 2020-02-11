using System;

namespace A6k.Kafka.Messages
{
    public class SyncGroupRequest
    {
        public string GroupId { get; set; }
        public int GenerationId { get; set; }
        public string MemberId { get; set; }
        public string GroupInstanceId { get; set; }
        public Assignment[] Assignments { get; set; }

        public class Assignment
        {
            public string MemberId { get; set; }
            public byte[] AssignmentData { get; set; }
        }
    }
}
