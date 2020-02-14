using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace A6k.Kafka
{
    public interface IConsumerPartitionAssignor
    {
        string Name { get; }
        Dictionary<string, List<TopicPartition>> Assign(Dictionary<string, int> partitionsPerTopic, Dictionary<string, Subscription> subscriptions);
    }

    //public class RangeAssignor : IGroupAssignor
    //{
    //    public string Name { get; } = "range";
    //}

    public class RoundRobinAssignor : AbstractPartitionAssignor, IConsumerPartitionAssignor
    {
        public string Name { get; } = "roundrobin";

        public Dictionary<string, List<TopicPartition>> Assign(Dictionary<string, int> partitionsPerTopic, Dictionary<string, Subscription> subscriptions)
        {
            var assignment = new Dictionary<string, List<TopicPartition>>();
            var memberInfoList = new List<GroupMember>();
            foreach (var memberSubscription in subscriptions)
            {
                assignment.Add(memberSubscription.Key, new List<TopicPartition>());
                memberInfoList.Add(new GroupMember(memberSubscription.Key, memberSubscription.Value.GroupInstanceId, ));
            }

            memberInfoList.Sort();
            var assigner = new CircularIterator<GroupMember>(memberInfoList);

            foreach (TopicPartition partition in allPartitionsSorted(partitionsPerTopic, subscriptions))
            {
                while (!subscriptions[assigner.Peek().MemberId].Topics.Contains(partition.Topic))
                    assigner.Next();
                assignment[assigner.Next().MemberId].Add(partition);
            }
            return assignment;
        }

        private List<TopicPartition> allPartitionsSorted(Dictionary<string, int> partitionsPerTopic, Dictionary<string, Subscription> subscriptions)
        {
            var topics = new SortedSet<string>();
            foreach (var subscription in subscriptions.Values.SelectMany(x => x.Topics))
                topics.Add(subscription);

            var allPartitions = new List<TopicPartition>();
            foreach (var topic in topics)
            {
                int numPartitionsForTopic = partitionsPerTopic[topic];
                allPartitions.AddRange(AbstractPartitionAssignor.Partitions(topic, numPartitionsForTopic));
            }
            return allPartitions;
        }
    }

    public abstract class AbstractPartitionAssignor:IConsumerPartitionAssignor
    {
        public string Name { get; }

        public async Task<GroupAssignment> Assign(ClusterManager metadata, GroupSubscription groupSubscription)
        {
            Dictionary<string, Subscription> subscriptions = groupSubscription.groupSubscription();
            var allSubscribedTopics = new HashSet<string>();
            foreach (var topic in subscriptions.SelectMany(e => e.Value.Topics))
                allSubscribedTopics.Add(topic);

            var partitionsPerTopic = new Dictionary<string, int>();
            foreach (string topic in allSubscribedTopics)
            {
                int numPartitions = (await metadata.GetTopic(topic)).Partitions.Count;
                if (numPartitions > 0)
                    partitionsPerTopic.Add(topic, numPartitions);
                else
                    log.debug("Skipping assignment for topic {} since no metadata is available", topic);
            }

            Dictionary<string, List<TopicPartition>> rawAssignments = Assign(partitionsPerTopic, subscriptions);

            // this class maintains no user data, so just wrap the results
            var assignments = new Dictionary<string, Assignment>();
            foreach (var assignmentEntry in rawAssignments)
                assignments.Add(assignmentEntry.Key, new Assignment(0, assignmentEntry.Value, null));
            return new GroupAssignment(assignments);
        }

        protected static List<TopicPartition> Partitions(string topic, int numPartitions)
        {
            var partitions = new List<TopicPartition>(numPartitions);
            for (int i = 0; i < numPartitions; i++)
                partitions.Add(new TopicPartition(topic, i));
            return partitions;
        }

        public Dictionary<string, List<TopicPartition>> Assign(Dictionary<string, int> partitionsPerTopic, Dictionary<string, Subscription> subscriptions)
        {
            throw new NotImplementedException();
        }
    }

    public class CircularIterator_<T> : IEnumerator<T>
    {
        int i = 0;
        private List<T> list;
        public CircularIterator_(List<T> list)
        {
            if (list == null || list.Count == 0)
                throw new ArgumentException("CircularIterator can only be used on non-empty lists");

            this.list = list;
        }

        public T Current { get; private set; }
        object IEnumerator.Current => Current;

        public T Peek() => list[i];

        public bool MoveNext()
        {
            Current = list[i];
            i = (i + 1) % list.Count;
            return true;
        }

        public void Reset() => throw new NotImplementedException();

        public void Dispose() { }
    }

    public class CircularIterator<T>
    {
        int i = 0;
        private List<T> list;

        public CircularIterator(List<T> list)
        {
            if (list == null || list.Count == 0)
                throw new ArgumentException("CircularIterator can only be used on non-empty lists");

            this.list = list;
        }

        public bool HasNext() => true;

        public T Next()
        {
            T next = list[i];
            i = (i + 1) % list.Count;
            return next;
        }

        public T Peek() => list[i];
    }
}
