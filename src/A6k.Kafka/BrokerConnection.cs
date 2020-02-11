
using A6k.Kafka.Metadata;
using Bedrock.Framework;

namespace A6k.Kafka
{
    public class BrokerConnection : KafkaConnection
    {
        internal BrokerConnection(Broker broker, Client client, string clientId) : base(client, broker.EndPoint, clientId)
        {
            Broker = broker;
        }

        public Broker Broker { get; }

        public int NodeId => Broker.NodeId;
        public string Host => Broker.Host;
        public int Port => Broker.Port;
        public string Rack => Broker.Rack;
    }
}
