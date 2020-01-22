using System.Buffers;
using Bedrock.Framework.Protocols;

namespace A6k.Kafka
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

    public class JoinGroupRequestWriter : IMessageWriter<JoinGroupRequest>
    {
        public void WriteMessage(JoinGroupRequest message, IBufferWriter<byte> output)
        {
            output.WriteString(message.GroupId);
            output.WriteInt(message.SessionTimeout);
            output.WriteInt(message.RebalanceTimeout);
            output.WriteString(message.MemberId);
            output.WriteNullableString(message.GroupInstanceId);
            output.WriteString(message.ProtocolType);
            output.WriteArray(message.Protocols, WriteProtocol);
        }

        private void WriteProtocol(JoinGroupRequest.Protocol message, IBufferWriter<byte> output)
        {
            output.WriteString(message.Name);
            output.Write(message.Metadata);
        }
    }

    public class JoinGroupResponseReader : KafkaResponseReader<JoinGroupResponse>
    {
        protected override bool TryParseMessage(ref SequenceReader<byte> reader, out JoinGroupResponse message)
        {
            message = default;

            if (!reader.TryReadInt(out var throttleTime))
                return false;
            if (!reader.TryReadShort(out var errorCode))
                return false;
            if (!reader.TryReadInt(out var generationId))
                return false;
            if (!reader.TryReadString(out var protocolName))
                return false;
            if (!reader.TryReadString(out var leader))
                return false;
            if (!reader.TryReadString(out var memberId))
                return false;
            if (!reader.TryReadArray<JoinGroupResponse.Member>(TryParseMember, out var members))
                return false;

            message = new JoinGroupResponse
            {
                ThrottleTime = throttleTime,
                ErrorCode = errorCode,
                GenerationId = generationId,
                ProtocolName = protocolName,
                Leader = leader,
                MemberId = memberId,
                Members = members
            };
            return true;
        }

        private bool TryParseMember(ref SequenceReader<byte> reader, out JoinGroupResponse.Member member)
        {
            member = default;
            if (!reader.TryReadString(out var memberId))
                return false;
            if (!reader.TryReadString(out var groupInstanceId))
                return false;
            if (!reader.TryReadBytes(out var metadata))
                return false;

            member = new JoinGroupResponse.Member
            {
                MemberId = memberId,
                GroupInstanceId = groupInstanceId,
                Metadata = metadata
            };
            return true;
        }
    }
}
