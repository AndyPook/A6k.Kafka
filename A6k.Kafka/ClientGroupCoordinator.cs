using System;
using System.Threading.Tasks;
using A6k.Kafka.Messages;

namespace A6k.Kafka
{
    public class ClientGroupCoordinator
    {
        private enum State
        {
            None,
            Find,
            Found,
            Join,
        }

        private readonly MetadataManager metadataManager;
        private readonly string groupId;

        private bool heartbeatRunning = false;
        private int coordinatorBrokerId = 0;
        private int generationId = 0;
        private string memberId;
        private State state;

        public ClientGroupCoordinator(MetadataManager metadataManager, string groupId)
        {
            this.metadataManager = metadataManager;
            this.groupId = groupId;
        }

        private async Task FindCoordinator()
        {
            var broker = metadataManager.GetRandomBroker();
            var response = await broker.Connection.FindCoordinator(groupId);
            switch (response.ErrorCode)
            {
                case ResponseError.COORDINATOR_NOT_AVAILABLE:
                    throw null;
            }

            var b = metadataManager.GetBroker(response.NodeId);
            if (!b.Equals(response.Host, response.Port))
                throw new InvalidOperationException($"Coordinator detail do not match metadata broker: {response.NodeId}/{response.Host}:{response.Port} != {b}");

            coordinatorBrokerId = response.NodeId;
            state = State.Found;
        }

        private async Task JoinGroup()
        {
            var b = metadataManager.GetBroker(coordinatorBrokerId);
            var response = await b.Connection.JoinGroup(new JoinGroupRequest
            {
                GroupId = groupId,
                ProtocolType = "client",
                SessionTimeout = 10_000,
                RebalanceTimeout = 300_000
            });
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
    }
}
