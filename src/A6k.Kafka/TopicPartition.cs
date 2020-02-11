using System;

namespace A6k.Kafka
{
    /// <summary>
    /// A topic name and partition number
    /// </summary>
    public sealed class TopicPartition
    {
        private int hash = 0;

        public TopicPartition(string topic, int partition)
        {
            Topic = topic;
            Partition = partition;
        }

        public string Topic { get; }
        public int Partition { get; }

        public override int GetHashCode()
        {
            if (hash != 0)
                return hash;

            hash = HashCode.Combine(Partition, Topic);
            return hash;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
                return false; 
            if (ReferenceEquals(this, obj)) 
                return true;
            if (!(obj is TopicPartition other))
                return false;

            return Partition == other.Partition && string.Equals(Topic, other.Topic);
        }

        public override string ToString() => $"{Topic} - {Partition}";
    }
}
