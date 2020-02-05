using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using A6k.Kafka.Messages;
using Bedrock.Framework.Protocols;

namespace A6k.Kafka
{
    public class GroupMember
    {
        public GroupMember(string memberId, string groupInstanceId, byte[] metadata)
        {
            MemberId = memberId;
            GroupInstanceId = groupInstanceId;
            Metadata = metadata;
        }

        public string MemberId { get; }
        public string GroupInstanceId { get; }
        public byte[] Metadata { get; }
    }

    public class ClientGroupCoordinator
    {
        public enum State
        {
            None,
            Find,
            Found,
            Join,
        }

        private readonly MetadataManager metadataManager;
        private readonly string groupId;
        private readonly ConsumerGroupMetadata groupMetadata;
        private readonly byte[] groupMetadataBytes;

        private bool heartbeatRunning = false;
        private int coordinatorBrokerId = 0;
        private int generationId = 0;
        private string memberId;
        private string leaderId;
        private State state;

        public ClientGroupCoordinator(MetadataManager metadataManager, string groupId, params string[] topics)
        {
            this.metadataManager = metadataManager;
            this.groupId = groupId;

            groupMetadata = new ConsumerGroupMetadata(0, topics);
            groupMetadataBytes = groupMetadata.ToArray();
        }

        public State CurrentState => state;
        public int CoordinatorId => coordinatorBrokerId;
        public string MemberId => memberId;
        public string LeaderId => leaderId;

        public bool IsLeader => string.Equals(leaderId, memberId);
        public IReadOnlyList<GroupMember> Members { get; private set; } = Array.Empty<GroupMember>();

        public async Task FindCoordinator()
        {
            var broker = metadataManager.GetRandomBroker();
            var response = await broker.Connection.FindCoordinator(groupId);
            //switch (response.ErrorCode)
            //{
            //    case ResponseError.COORDINATOR_NOT_AVAILABLE:
            //        throw null;
            //}
            if (response.ErrorCode != ResponseError.NO_ERROR)
                throw new InvalidOperationException($"FindCoordinator oops: {response.ErrorCode.ToString()}");

            var b = metadataManager.GetBroker(response.NodeId);
            if (!b.Equals(response.Host, response.Port))
                throw new InvalidOperationException($"Coordinator detail do not match metadata broker: {response.NodeId}/{response.Host}:{response.Port} != {b}");

            coordinatorBrokerId = response.NodeId;
            state = State.Found;
        }

        public async Task JoinGroup()
        {
            var b = metadataManager.GetBroker(coordinatorBrokerId);
            bool joined = false;
            while (!joined)
            {
                var response = await b.Connection.JoinGroup(new JoinGroupRequest
                {
                    GroupId = groupId,
                    ProtocolType = "consumer",
                    MemberId = memberId,
                    SessionTimeout = 10_000,
                    RebalanceTimeout = 300_000,
                    Protocols = new JoinGroupRequest.Protocol[]
                    {
                        new JoinGroupRequest.Protocol{ Name = "range", Metadata = groupMetadataBytes },
                        new JoinGroupRequest.Protocol{ Name = "roundrobin", Metadata = groupMetadataBytes }
                    }
                });
                Console.WriteLine("JoinGroup: " + response.ErrorCode);
                switch (response.ErrorCode)
                {
                    case ResponseError.NO_ERROR:
                        memberId = response.MemberId;
                        leaderId = response.Leader;
                        generationId = response.GenerationId;
                        joined = true;
                        break;
                    case ResponseError.MEMBER_ID_REQUIRED:
                        memberId = response.MemberId;
                        break;
                    case ResponseError.UNKNOWN_MEMBER_ID:
                        throw new InvalidOperationException();
                    default:
                        await Task.Delay(1_000);
                        break;
                }

                if (joined && IsLeader)
                    Members = response.Members.Select(m => new GroupMember(m.MemberId, m.GroupInstanceId, m.Metadata)).ToArray();
                else
                    Members = Array.Empty<GroupMember>();
            }
        }

        public async Task SyncGroup()
        {
            var b = metadataManager.GetBroker(coordinatorBrokerId);
            var response = await b.Connection.SyncGroup(new SyncGroupRequest
            {
                GroupId = groupId,
                MemberId = memberId,
                GenerationId = generationId
            });
            if (response.ErrorCode != ResponseError.NO_ERROR)
                throw new InvalidOperationException("SyncGroup error: " + response.ErrorCode);

            if (IsLeader && response.Assignment?.Length == 0)
            {
                Console.WriteLine("leader but not assignments");
            }

            CurrentMemberState = new MemberState(new ReadOnlySequence<byte>(response.Assignment));
            CurrentMemberState = new MemberState(response.Assignment);
        }

        public MemberState CurrentMemberState { get; private set; }

        private async Task SendHeartbeats()
        {
            heartbeatRunning = true;
            var broker = metadataManager.GetBroker(coordinatorBrokerId);
            while (heartbeatRunning)
            {
                var response = await broker.Connection.Heartbeat(new HeartbeatRequest
                {
                    GroupId = groupId,
                    GenerationId = generationId,
                    MemberId = memberId
                });

                switch (response.ErrorCode)
                {
                    case ResponseError.GROUP_COORDINATOR_NOT_AVAILABLE:
                    case ResponseError.GROUP_ID_NOT_FOUND:
                        throw new InvalidOperationException($"Heartbeat: " + response.ErrorCode.ToString());

                    case ResponseError.REASSIGNMENT_IN_PROGRESS:
                        // TODO: handle reassignment
                        break;
                }

                //sensors.heartbeatSensor.record(response.requestLatencyMs());
                //Errors error = heartbeatResponse.error();
                switch (response.ErrorCode)
                {
                    case ResponseError.NO_ERROR:
                        //log.debug("Received successful Heartbeat response");
                        break;
                    case ResponseError.COORDINATOR_NOT_AVAILABLE:
                    case ResponseError.NOT_COORDINATOR:
                        //log.info("Attempt to heartbeat failed since coordinator {} is either not started or not valid.",
                        //coordinator());
                        //markCoordinatorUnknown();
                        //future.raise(error);
                        break;
                    case ResponseError.REBALANCE_IN_PROGRESS:
                        //log.info("Attempt to heartbeat failed since group is rebalancing");
                        //requestRejoin();
                        //future.raise(error);
                        break;
                    case ResponseError.ILLEGAL_GENERATION:
                        //log.info("Attempt to heartbeat failed since generation {} is not current", generation.generationId);
                        //resetGenerationOnResponseError(ApiKeys.HEARTBEAT, error);
                        //future.raise(error);
                        break;
                    case ResponseError.FENCED_INSTANCE_ID:
                        //log.error("Received fatal exception: group.instance.id gets fenced");
                        //future.raise(error);
                        break;
                    case ResponseError.UNKNOWN_MEMBER_ID:
                        //log.info("Attempt to heartbeat failed for since member id {} is not valid.", generation.memberId);
                        //resetGenerationOnResponseError(ApiKeys.HEARTBEAT, error);
                        //future.raise(error);
                        break;
                    case ResponseError.GROUP_AUTHORIZATION_FAILED:
                        //future.raise(GroupAuthorizationException.forGroupId(rebalanceConfig.groupId));
                        break;
                    default:
                        throw new KafkaException("Unexpected error in heartbeat response: " + response.ErrorCode.ToString());
                }

                await Task.Delay(3_000); // heartbeat.interval.ms
            }
        }

        public class ConsumerGroupMetadata
        {

            public ConsumerGroupMetadata(in ReadOnlySequence<byte> input)
            {
                var reader = new SequenceReader<byte>(input);
                if (
                    !reader.TryReadShort(out var version) ||
                    !reader.TryReadArrayOfString(out var topics) ||
                    !reader.TryReadBytes(out var userdata)
                )
                    throw new InvalidOperationException("BadFormat: ConsumerGroupMetadata");

                Version = version;
                Topics = topics;
                UserData = userdata.ToArray();
            }

            public ConsumerGroupMetadata(short version, string[] topics, byte[] userData = null)
            {
                Version = version;
                Topics = topics;
                UserData = userData;
            }

            public short Version { get; set; }

            public string[] Topics { get; set; }
            public byte[] UserData { get; set; }

            public void CopyTo(IBufferWriter<byte> output)
            {
                output.WriteShort(Version);
                output.WriteArray(Topics);
                output.WriteBytes(UserData);
            }

            public byte[] ToArray()
            {
                var buffer = new MemoryBufferWriter();
                CopyTo(buffer);
                return buffer.ToArray();
            }
        }

        public class MemberState
        {
            public MemberState(short version, IReadOnlyList<TopicPartition> assignments, byte[] userData)
            {
                Version = version;
                Assignments = assignments;
                UserData = userData;
            }

            public MemberState(in ReadOnlyMemory<byte> input) : this(new SequenceReader<byte>(new ReadOnlySequence<byte>(input))) { }
            public MemberState(in ReadOnlySequence<byte> input) : this(new SequenceReader<byte>(input)) { }
            public MemberState(SequenceReader<byte> reader)
            {
                if (
                    !reader.TryReadShort(out var version) ||
                    !reader.TryReadArray<TopicPartition>(TryParseAssignment, out var topics) ||
                    !reader.TryReadBytes(out var userdata)
                )
                    throw new InvalidOperationException("BadFormat: ConsumerGroupMetadata");

                Version = version;
                Assignments = topics;
                UserData = userdata.ToArray();

                bool TryParseAssignment(ref SequenceReader<byte> reader, out TopicPartition member)
                {
                    member = default;
                    if (
                        !reader.TryReadString(out var topic) ||
                        !reader.TryReadArrayOfInt(out var partitions)
                    )
                        return false;

                    member = new TopicPartition(topic, partitions);
                    return true;
                }
            }

            public short Version { get; }
            public IReadOnlyList<TopicPartition> Assignments { get; }
            public byte[] UserData { get; }

            public void CopyTo(IBufferWriter<byte> buffer)
            {
                buffer.WriteShort(Version);
                buffer.WriteArray(Assignments, WriteAssignment);
                buffer.WriteBytes(UserData);

                void WriteAssignment(TopicPartition message, IBufferWriter<byte> output)
                {
                    output.WriteString(message.Topic);
                    output.WriteArray(message.Partitions);
                }
            }

            public class TopicPartition
            {
                public TopicPartition(string topic, int[] partitions)
                {
                    Topic = topic;
                    Partitions = partitions;
                }

                public string Topic { get; }
                public int[] Partitions { get; }
            }
        }
    }
}
