using System.Buffers;

namespace A6k.Kafka.Messages
{
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
