using System;
using System.Diagnostics.CodeAnalysis;
using System.Net;

namespace A6k.Kafka.Metadata
{
    public class Broker : IEquatable<Broker>
    {
        public Broker(int nodeId, string host, int port, string rack)
        {
            // TODO: guard statements

            NodeId = nodeId;
            Host = host;
            Port = port;
            Rack = rack;

            if (!IPAddress.TryParse(Host, out var addr))
            {
                string s = Host.Replace("localhost", "127.0.0.1");
                if (!IPAddress.TryParse(s, out addr))
                    throw new ArgumentException("invalid broker address");
            }

            EndPoint = new IPEndPoint(addr, Port);
        }

        public int NodeId { get; }
        public string Host { get; }
        public int Port { get; }
        public string Rack { get; }

        public IPEndPoint EndPoint { get; }

        public bool Equals(string host, int port, string rack = null)
        {
            if (rack == null)
                return string.Equals(Host, host) && Port == port;

            return string.Equals(Host, host) && Port == port && string.Equals(Rack, rack);
        }

        public override bool Equals(object obj) => obj is Broker b && Equals(b);

        public bool Equals([AllowNull] Broker other)
        {
            if (ReferenceEquals(null, other))
                return false;
            if (ReferenceEquals(this, other))
                return true;

            return
                NodeId == other.NodeId &&
                string.Equals(Host, other.Host) &&
                Port == other.Port &&
                string.Equals(Rack, other.Rack);
        }

        public override int GetHashCode() => HashCode.Combine(NodeId, Host, Port, Rack);

        public override string ToString() => $"node: {NodeId}: {Host}:{Port} ({Rack})";
    }
}
