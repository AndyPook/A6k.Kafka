using System;
using System.Collections.Generic;

namespace A6k.Kafka.Messages
{
    public class JoinGroupResponse
    {
        public JoinGroupResponse(int throttleTime, short errorCode, int generationId, string protocolName, string leader, string memberId, Member[] members)
        {
            ThrottleTime = throttleTime;
            ErrorCode = (ResponseError)errorCode;
            GenerationId = generationId;
            ProtocolName = protocolName;
            Leader = leader;
            MemberId = memberId;
            Members = members;
        }

        public int ThrottleTime { get; }
        public ResponseError ErrorCode { get; }
        public int GenerationId { get; }
        public string ProtocolName { get; }
        public string Leader { get; }
        public string MemberId { get; }
        public IReadOnlyList<Member> Members { get; }

        public class Member
        {
            public Member(string memberId, string groupInstanceId, byte[] metadata)
            {
                MemberId = memberId;
                GroupInstanceId = groupInstanceId;
                Metadata = metadata;
            }

            public string MemberId { get; }
            public string GroupInstanceId { get; }
            public byte[] Metadata { get; }
        }
    }
}
