using System;
using System.Buffers;
using System.Threading.Tasks;
using A6k.Kafka.Messages;
using Bedrock.Framework.Protocols;

namespace A6k.Kafka
{
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
        private readonly byte[] groupMetadata;

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

            groupMetadata = WriteConsumerGroupMetadata(0, topics);
        }

        public State CurrentState => state;
        public int CoordinatorId => coordinatorBrokerId;
        public string MemberId => memberId;
        public string LeaderId => leaderId;

        public bool IsLeader => string.Equals(leaderId, memberId);

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
            var response = await b.Connection.JoinGroup(new JoinGroupRequest
            {
                GroupId = groupId,
                ProtocolType = "client",
                SessionTimeout = 10_000,
                RebalanceTimeout = 300_000,
                Protocols = new JoinGroupRequest.Protocol[]
                {
                    new JoinGroupRequest.Protocol{ Name = "range", Metadata = groupMetadata }
                }
            });

            memberId = response.MemberId;
        }

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

                await Task.Delay(3_000); // heartbeat.interval.ms
            }
        }

        public class ConsumerGroupMetadata
        {
            public short Version { get; set; }

            public string[] Topics { get; set; }
            public byte[] UserData { get; set; }
        }
        
        private ConsumerGroupMetadata ParseConsumerGroupMetadata(in ReadOnlySequence<byte> input)
        {
            var reader = new SequenceReader<byte>(input);
            if (
                !reader.TryReadShort(out var version) ||
                !reader.TryReadArrayOfString(out var topics) ||
                !reader.TryReadBytes(out var userdata)
            )
                throw new InvalidOperationException("BadFormat: ConsumerGroupMetadata");

            return new ConsumerGroupMetadata
            {
                Version = version,
                Topics = topics,
                UserData = userdata
            };
        }

        private byte[] WriteConsumerGroupMetadata(short version, params string[] topics)
        {
            var buffer = new MemoryBufferWriter<byte>();
            
            buffer.WriteShort(version);
            buffer.WriteArray(topics, (t, o) => o.WriteString(t));
            buffer.WriteInt(0); // no userdata

            return buffer.AsReadOnlySequence.ToArray();
        }
    }
}
