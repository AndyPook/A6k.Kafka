using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using A6k.Kafka.Messages;
using A6k.Kafka.Metadata;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace A6k.Kafka
{
    public abstract class ClientGroupCoordinator
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

        public enum RebalanceProtocol : byte
        {
            Eager = 0,
            Cooperative = 1
        }

        protected readonly ClusterManager cluster;
        protected readonly Subscription groupMetadata;
        protected readonly byte[] groupMetadataBytes;

        protected readonly ILogger logger = NullLogger<ClientGroupCoordinator>.Instance;

        // ???
        protected RebalanceProtocol protocol;

        public ClientGroupCoordinator(ClusterManager cluster, string groupId, params string[] topics)
        {
            this.cluster = cluster;
            GroupId = groupId;

            groupMetadata = new Subscription(0, topics);
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
                    Console.WriteLine("problem in state change handler: " + ex.Message);
                }
                state = value;
            }
        }
        public int CoordinatorId { get; private set; }
        public string MemberId { get; private set; }
        public string LeaderId { get; private set; }
        public int GenerationId { get; private set; }
        public string GroupProtocol { get; private set; }

        public bool IsLeader => string.Equals(LeaderId, MemberId);
        public Assignment CurrentMemberState { get; private set; } = Assignment.Empty;
        public IReadOnlyList<GroupMember> Members { get; private set; } = Array.Empty<GroupMember>();

        public event Action<ClientGroupCoordinator> CoordinatorFound;
        public event Action<ClientGroupCoordinator> GroupJoined;
        public event Action<ClientGroupCoordinator> GroupSynced;
        public event Action<ClientGroupCoordinator, CoordinatorState, CoordinatorState> StateChanged;

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
            var broker = cluster.GetRandomBroker();
            var response = await broker.FindCoordinator(GroupId);
            switch (response.ErrorCode)
            {
                case ResponseError.NO_ERROR:
                    break;
                case ResponseError.COORDINATOR_NOT_AVAILABLE:
                    throw new KafkaException($"FindCoordinator not available: {response.ErrorCode.ToString()}");
                default:
                    throw new KafkaException($"FindCoordinator oops: {response.ErrorCode.ToString()}");
            }

            var b = cluster.GetBroker(response.NodeId);
            if (!b.Broker.Equals(response.Host, response.Port))
                throw new KafkaException($"Coordinator detail do not match metadata broker: {response.NodeId}/{response.Host}:{response.Port} != {b}");

            CoordinatorId = response.NodeId;
            State = CoordinatorState.Found;

            CoordinatorFound?.Invoke(this);
        }

        private async Task JoinGroup()
        {
            State = CoordinatorState.Joining;
            var b = cluster.GetBroker(CoordinatorId);
            bool joined = false;
            while (!joined)
            {
                var response = await b.JoinGroup(new JoinGroupRequest
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

                switch (response.ErrorCode)
                {
                    case ResponseError.NO_ERROR:
                        MemberId = response.MemberId;
                        LeaderId = response.Leader;
                        GenerationId = response.GenerationId;
                        GroupProtocol = response.ProtocolName;
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
            var b = cluster.GetBroker(CoordinatorId);
            var request = new SyncGroupRequest
            {
                GroupId = GroupId,
                MemberId = MemberId,
                GenerationId = GenerationId
            };
            if (IsLeader)
            {
                var assignments = PerformAssignment(LeaderId, "roundrobin", Members);
                request.Assignments = assignments.Select(kvp => new SyncGroupRequest.Assignment
                {
                    MemberId = kvp.Key,
                    AssignmentData = kvp.Value
                }).ToArray();
            }

            var response = await b.SyncGroup(request);
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
                CurrentMemberState = Assignment.Empty;
            }
            else
                CurrentMemberState = new Assignment(response.Assignment);

            State = CoordinatorState.Synced;
            GroupSynced?.Invoke(this);
        }

        protected abstract Dictionary<string, byte[]> PerformAssignment(
            string leaderId,
            string assignmentStrategy,
            IReadOnlyList<GroupMember> allSubscriptions);

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
            var coordinator = cluster.GetBroker(CoordinatorId);
            while (!cancellationToken.IsCancellationRequested)
            {
                //Console.WriteLine("Heartbeat");
                var response = await coordinator.Heartbeat(new HeartbeatRequest
                {
                    GroupId = GroupId,
                    GenerationId = GenerationId,
                    MemberId = MemberId
                });

                //var response = await broker.ApiVersion();

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
    }

    public class ConsumerGroupCoordiinator : ClientGroupCoordinator
    {
        public ConsumerGroupCoordiinator(ClusterManager cluster, string groupId, params string[] topics) : base(cluster, groupId, topics)
        {
        }

        override protected Dictionary<string, byte[]> PerformAssignment(
            string leaderId,
            string assignmentStrategy,
            IReadOnlyList<GroupMember> allSubscriptions
        )
        {
            var assignor = LookupAssignor(assignmentStrategy);
            if (assignor == null)
                throw new InvalidOperationException("Coordinator selected invalid assignment protocol: " + assignmentStrategy);

            var allSubscribedTopics = new HashSet<string>();
            var subscriptions = new Dictionary<string, Subscription>();

            // collect all the owned partitions
            var ownedPartitions = new Dictionary<string, List<Subscription.TopicPartitions>>();

            foreach (var memberSubscription in allSubscriptions)
            {
                var subscription = new Subscription(memberSubscription.Metadata, memberSubscription.GroupInstanceId);
                subscriptions[memberSubscription.MemberId] = subscription;
                foreach (var t in subscription.Topics)
                    allSubscribedTopics.Add(t);
                ownedPartitions[memberSubscription.MemberId] = subscription.OwnedPartitions.ToList();
            }

            // the leader will begin watching for changes to any of the topics the group is interested in,
            // which ensures that all metadata changes will eventually be seen
            updateGroupSubscription(allSubscribedTopics);

            // isLeader = true;

            logger.LogDebug("Performing assignment using strategy {Name} with subscriptions {Subscriptions}", assignor.Name, subscriptions);

            Dictionary<string, Assignment> assignments = assignor.Assign(cluster, new GroupSubscription(subscriptions)).groupAssignment();

            if (protocol == RebalanceProtocol.Cooperative)
            {
                validateCooperativeAssignment(ownedPartitions, assignments);
            }

            // user-customized assignor may have created some topics that are not in the subscription list
            // and assign their partitions to the members; in this case we would like to update the leader's
            // own metadata with the newly added topics so that it will not trigger a subsequent rebalance
            // when these topics gets updated from metadata refresh.
            //
            // TODO: this is a hack and not something we want to support long-term unless we push regex into the protocol
            //       we may need to modify the ConsumerPartitionAssignor API to better support this case.
            var assignedTopics = new HashSet<string>();
            foreach (Assignment assigned in assignments.Values)
            {
                foreach (var tp in assigned.AssignedPartitions)
                    assignedTopics.Add(tp.Topic);
            }

            if (!assignedTopics.IsProperSubsetOf(allSubscribedTopics))
            {
                var notAssignedTopics = new HashSet<string>(allSubscribedTopics);
                notAssignedTopics.RemoveWhere(t => assignedTopics.Contains(t));
                logger.LogWarning("The following subscribed topics are not assigned to any members: {NotAssignedTopics}", notAssignedTopics);
            }

            if (!allSubscribedTopics.IsProperSubsetOf(assignedTopics))
            {
                var newlyAddedTopics = new HashSet<string>(assignedTopics);
                newlyAddedTopics.RemoveWhere(t => allSubscribedTopics.Contains(t));
                logger.LogInformation("The following not-subscribed topics are assigned, and their metadata will be " +
                        "fetched from the brokers: {NewlyAddedTopics}", newlyAddedTopics);

                foreach (var t in assignedTopics)
                    allSubscribedTopics.Add(t);
                updateGroupSubscription(allSubscribedTopics);
            }

            assignmentSnapshot = metadataSnapshot;

            logger.LogInformation("Finished assignment for group at generation {Generation}: {Assignments}", GenerationId, assignments);

            var groupAssignment = new Dictionary<string, byte[]>();
            foreach (var assignmentEntry in assignments)
            {
                var buffer = assignmentEntry.Value.ToArray();
                groupAssignment[assignmentEntry.Key] = buffer;
            }

            return groupAssignment;
        }

        private IConsumerPartitionAssignor LookupAssignor(string assignmentStrategy)
        {
            switch (assignmentStrategy.ToLowerInvariant())
            {
                case "roundrobin": return new RoundRobinAssignor();
                default: throw new InvalidOperationException("Unknown partition assignment strategy: " + assignmentStrategy);
            }
        }
    }

    public class GroupMember
    {
        public GroupMember(string memberId, string groupInstanceId, byte[] metadata)
        {
            MemberId = memberId;
            GroupInstanceId = groupInstanceId;
            Metadata = metadata;
            Subscription = new Subscription(metadata, groupInstanceId);
        }

        public string MemberId { get; }
        public string GroupInstanceId { get; }
        public byte[] Metadata { get; }

        public Subscription Subscription { get; }
    }

    public class Assignment
    {
        public static readonly Assignment Empty = new Assignment(0, Array.Empty<TopicPartitions>(), Array.Empty<byte>());

        public Assignment(short version, IReadOnlyList<TopicPartitions> assignments, byte[] userData)
        {
            Version = version;
            AssignedPartitions = assignments;
            UserData = userData;
        }

        public Assignment(in ReadOnlyMemory<byte> input) : this(new SequenceReader<byte>(new ReadOnlySequence<byte>(input))) { }
        public Assignment(in ReadOnlySequence<byte> input) : this(new SequenceReader<byte>(input)) { }
        public Assignment(SequenceReader<byte> reader)
        {
            if (
                !reader.TryReadShort(out var version) ||
                !reader.TryReadArray<TopicPartitions>(TryParseAssignment, out var topics) ||
                !reader.TryReadBytes(out var userdata)
            )
                throw new KafkaException("BadFormat: ConsumerGroupMetadata");

            Version = version;
            AssignedPartitions = topics;
            UserData = userdata.ToArray();

            bool TryParseAssignment(ref SequenceReader<byte> reader, out TopicPartitions member)
            {
                member = default;
                if (
                    !reader.TryReadString(out var topic) ||
                    !reader.TryReadArrayOfInt(out var partitions)
                )
                    return false;

                member = new TopicPartitions(topic, partitions);
                return true;
            }
        }

        public short Version { get; }
        public IReadOnlyList<TopicPartitions> AssignedPartitions { get; }
        public byte[] UserData { get; }

        public void WriteTo(IBufferWriter<byte> buffer)
        {
            buffer.WriteShort(Version);
            buffer.WriteArray(AssignedPartitions, WriteAssignment);
            buffer.WriteBytes(UserData);

            void WriteAssignment(TopicPartitions message, IBufferWriter<byte> output)
            {
                output.WriteString(message.Topic);
                output.WriteArray(message.Partitions);
            }
        }

        public byte[] ToArray()
        {
            using var buffer = new MemoryBufferWriter();
            WriteTo(buffer);
            return buffer.ToArray();
        }


        public class TopicPartitions
        {
            public TopicPartitions(string topic, IReadOnlyList<int> partitions)
            {
                Topic = topic;
                Partitions = partitions;
            }

            public string Topic { get; }
            public IReadOnlyList<int> Partitions { get; }
        }
    }

    public class Subscription
    {
        public Subscription(in ReadOnlyMemory<byte> input, string groupInstanceId)
            : this(new ReadOnlySequence<byte>(input), groupInstanceId) { }
        public Subscription(in ReadOnlySequence<byte> input, string groupInstanceId)
        {
            var reader = new SequenceReader<byte>(input);
            if (
                !reader.TryReadShort(out var version) ||
                !reader.TryReadArrayOfString(out var topics) ||
                !reader.TryReadBytes(out var userdata) ||
                (version >= 1 && !reader.TryReadArray<TopicPartitions>(TryParsePartition, out var owned))
            )
                throw new KafkaException("BadFormat: ConsumerGroupMetadata");

            Version = version;
            Topics = topics;
            UserData = userdata.ToArray();
            GroupInstanceId = groupInstanceId;

            bool TryParsePartition(ref SequenceReader<byte> reader, out TopicPartitions message)
            {
                message = default;
                if (!reader.TryReadString(out var topicName))
                    return false;
                if (!reader.TryReadArrayOfInt(out var partitions))
                    return false;

                message = new TopicPartitions(topicName, partitions);
                return true;
            }
        }

        public Subscription(short version, string[] topics, byte[] userData = null, string groupInstanceId = null)
        {
            Version = version;
            Topics = topics;
            UserData = userData;
            GroupInstanceId = groupInstanceId;
        }

        public short Version { get; }

        public string[] Topics { get; }
        public byte[] UserData { get; }
        public IReadOnlyCollection<TopicPartitions> OwnedPartitions { get; }

        public string GroupInstanceId { get; }

        public void WriteTo(IBufferWriter<byte> output)
        {
            output.WriteShort(Version);
            output.WriteArray(Topics);
            output.WriteBytes(UserData);
        }

        public byte[] ToArray()
        {
            using var buffer = new MemoryBufferWriter();
            WriteTo(buffer);
            return buffer.ToArray();
        }

        public class TopicPartitions
        {
            public TopicPartitions(string topic, IReadOnlyList<int> partitions)
            {
                Topic = topic;
                Partitions = partitions;
            }

            public string Topic { get; }
            public IReadOnlyList<int> Partitions { get; }
        }
    }
}
