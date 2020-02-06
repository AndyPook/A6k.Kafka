using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using A6k.Kafka.Messages;
using A6k.Kafka.Metadata;

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
        public enum CoordinatorState
        {
            None,
            Finding,
            Found,
            Joining,
            Joined,
            Syncing,
            Synced,
            Heartbeating
        }

        private readonly MetadataManager metadataManager;
        private readonly ConsumerGroupMetadata groupMetadata;
        private readonly byte[] groupMetadataBytes;

        public ClientGroupCoordinator(MetadataManager metadataManager, string groupId, params string[] topics)
        {
            this.metadataManager = metadataManager;
            this.GroupId = groupId;

            groupMetadata = new ConsumerGroupMetadata(0, topics);
            groupMetadataBytes = groupMetadata.ToArray();
        }

        public string GroupId { get; }
        private CoordinatorState state;
        public CoordinatorState State
        {
            get => state;
            private set
            {
                try
                {
                    StateChanged?.Invoke(this, state, value);
                }
                catch (Exception ex)
                {
                    Console.WriteLine("problem in state chage handler: " + ex.Message);
                }
                state = value;
            }
        }
        public int CoordinatorId { get; private set; }
        public string MemberId { get; private set; }
        public string LeaderId { get; private set; }
        public int GenerationId { get; private set; }

        public bool IsLeader => string.Equals(LeaderId, MemberId);
        public MemberState CurrentMemberState { get; private set; } = MemberState.Empty;
        public IReadOnlyList<GroupMember> Members { get; private set; } = Array.Empty<GroupMember>();

        public event Action<ClientGroupCoordinator> CoordinatorFound;
        public event Action<ClientGroupCoordinator> GroupJoined;
        public event Action<ClientGroupCoordinator> GroupSynced;
        public event Action<ClientGroupCoordinator, CoordinatorState, CoordinatorState> StateChanged;

        public Broker GetCoordinator() => metadataManager.GetBroker(CoordinatorId);

        public async Task Start()
        {
            State = CoordinatorState.None;
            await FindCoordinator();
            await RequestJoin();
        }

        public void Stop() => CancelHeartbeat();

        private async Task RequestJoin()
        {
            if (heartbeatCancellation != null && !heartbeatCancellation.IsCancellationRequested)
                CancelHeartbeat();

            await JoinGroup();
            await SyncGroup();
            _ = StartHeartbeat();
        }

        private async Task FindCoordinator()
        {
            State = CoordinatorState.Finding;
            var broker = metadataManager.GetRandomBroker();
            var response = await broker.Connection.FindCoordinator(GroupId);
            switch (response.ErrorCode)
            {
                case ResponseError.NO_ERROR:
                    break;
                case ResponseError.COORDINATOR_NOT_AVAILABLE:
                    throw new KafkaException($"FindCoordinator not available: {response.ErrorCode.ToString()}");
                default:
                    throw new KafkaException($"FindCoordinator oops: {response.ErrorCode.ToString()}");
            }

            var b = metadataManager.GetBroker(response.NodeId);
            if (!b.Equals(response.Host, response.Port))
                throw new KafkaException($"Coordinator detail do not match metadata broker: {response.NodeId}/{response.Host}:{response.Port} != {b}");

            CoordinatorId = response.NodeId;
            State = CoordinatorState.Found;

            CoordinatorFound?.Invoke(this);
        }

        private async Task JoinGroup()
        {
            State = CoordinatorState.Joining;
            var b = metadataManager.GetBroker(CoordinatorId);
            bool joined = false;
            while (!joined)
            {
                var response = await b.Connection.JoinGroup(new JoinGroupRequest
                {
                    GroupId = GroupId,
                    ProtocolType = "consumer",
                    MemberId = MemberId,
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
                        MemberId = response.MemberId;
                        LeaderId = response.Leader;
                        GenerationId = response.GenerationId;
                        if (joined && IsLeader)
                            Members = response.Members.Select(m => new GroupMember(m.MemberId, m.GroupInstanceId, m.Metadata)).ToArray();
                        else
                            Members = Array.Empty<GroupMember>();
                        joined = true;
                        State = CoordinatorState.Joined;
                        GroupJoined?.Invoke(this);
                        break;
                    case ResponseError.MEMBER_ID_REQUIRED:
                        MemberId = response.MemberId;
                        break;
                    case ResponseError.UNKNOWN_MEMBER_ID:
                        throw new KafkaException("JoinGroup: unknown MembrId");
                    default:
                        await Task.Delay(500);
                        break;
                }
            }
        }

        private async Task SyncGroup()
        {
            State = CoordinatorState.Syncing;
            Console.WriteLine("SyncGroup");
            var b = metadataManager.GetBroker(CoordinatorId);
            var request = new SyncGroupRequest
            {
                GroupId = GroupId,
                MemberId = MemberId,
                GenerationId = GenerationId
            };
            if (IsLeader)
            {
                // figure out member assignments
            }

            var response = await b.Connection.SyncGroup(request);
            switch (response.ErrorCode)
            {
                case ResponseError.NO_ERROR:
                    break;
                case ResponseError.UNKNOWN_MEMBER_ID:
                case ResponseError.ILLEGAL_GENERATION:
                //reset
                default:
                    throw new KafkaException("SyncGroup error: " + response.ErrorCode);
            }

            if (IsLeader && response.Assignment?.Length == 0)
            {
                Console.WriteLine("leader but no assignments");
                CurrentMemberState = MemberState.Empty;
            }
            else
                CurrentMemberState = new MemberState(response.Assignment);

            State = CoordinatorState.Synced;
            GroupSynced?.Invoke(this);
        }

        private CancellationTokenSource heartbeatCancellation;

        private void CancelHeartbeat()
        {
            heartbeatCancellation.Cancel();
            heartbeatCancellation = null;
        }

        private async Task StartHeartbeat()
        {
            if (heartbeatCancellation != null && !heartbeatCancellation.IsCancellationRequested)
                return;

            await Task.Yield();
            heartbeatCancellation = new CancellationTokenSource();
            try
            {
                await StartHeartbeat(heartbeatCancellation.Token);
            }
            catch (Exception ex)
            {
                Console.WriteLine("heartbeat: " + ex.Message);
            }
        }
        private async Task StartHeartbeat(CancellationToken cancellationToken)
        {
            State = CoordinatorState.Heartbeating;
            var coordinator = GetCoordinator();
            while (!cancellationToken.IsCancellationRequested)
            {
                //Console.WriteLine("Heartbeat");
                var response = await coordinator.Connection.Heartbeat(new HeartbeatRequest
                {
                    GroupId = GroupId,
                    GenerationId = GenerationId,
                    MemberId = MemberId
                });

                //var response = await broker.Connection.ApiVersion();

                //switch (response.ErrorCode)
                //{
                //    case ResponseError.GROUP_COORDINATOR_NOT_AVAILABLE:
                //    case ResponseError.GROUP_ID_NOT_FOUND:
                //        throw new KafkaException($"Heartbeat: " + response.ErrorCode.ToString());

                //    case ResponseError.REASSIGNMENT_IN_PROGRESS:
                //        // TODO: handle reassignment
                //        break;
                //}

                //sensors.heartbeatSensor.record(response.requestLatencyMs());
                //Errors error = heartbeatResponse.error();
                switch (response.ErrorCode)
                {
                    case ResponseError.NO_ERROR:
                        //log.debug("Received successful Heartbeat response");
                        //Console.WriteLine("Received successful Heartbeat response");
                        break;
                    case ResponseError.COORDINATOR_NOT_AVAILABLE:
                    case ResponseError.NOT_COORDINATOR:
                        //log.info("Attempt to heartbeat failed since coordinator {} is either not started or not valid.",
                        //coordinator());
                        //markCoordinatorUnknown();
                        //future.raise(error);
                        await Start();
                        break;
                    case ResponseError.REBALANCE_IN_PROGRESS:
                        //log.info("Attempt to heartbeat failed since group is rebalancing");
                        //requestRejoin();
                        //future.raise(error);
                        await RequestJoin();
                        break;

                    case ResponseError.ILLEGAL_GENERATION:
                        //log.info("Attempt to heartbeat failed since generation {} is not current", generation.generationId);
                        //resetGenerationOnResponseError(ApiKeys.HEARTBEAT, error);
                        //future.raise(error);
                        await RequestJoin();
                        break;
                    case ResponseError.UNKNOWN_MEMBER_ID:
                        //log.info("Attempt to heartbeat failed for since member id {} is not valid.", generation.memberId);
                        //resetGenerationOnResponseError(ApiKeys.HEARTBEAT, error);
                        //future.raise(error);
                        await RequestJoin();
                        break;
                    case ResponseError.GROUP_AUTHORIZATION_FAILED:
                    //future.raise(GroupAuthorizationException.forGroupId(rebalanceConfig.groupId));
                    //Console.WriteLine("expected HeartBeat error: " + response.ErrorCode);
                    //break;
                    case ResponseError.FENCED_INSTANCE_ID:
                    //log.error("Received fatal exception: group.instance.id gets fenced");
                    //future.raise(error);
                    //break;
                    default:
                        Console.WriteLine("heartbeat: " + response.ErrorCode.ToString());
                        throw new KafkaException("Unexpected error in heartbeat response: " + response.ErrorCode.ToString());
                }

                // heartbeat.interval.ms
                // ignore cancellation
                await Task.Delay(3_000, cancellationToken).ContinueWith(_ => { });
            }
            Console.WriteLine("Heartbeat: stopped");
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
                    throw new KafkaException("BadFormat: ConsumerGroupMetadata");

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
            public static readonly MemberState Empty = new MemberState(0, Array.Empty<TopicPartition>(), Array.Empty<byte>());

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
                    throw new KafkaException("BadFormat: ConsumerGroupMetadata");

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
