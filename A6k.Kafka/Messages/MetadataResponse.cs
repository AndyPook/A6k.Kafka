﻿using System.Collections.Generic;

namespace A6k.Kafka.Messages
{
    public class MetadataResponse
    {
        public MetadataResponse(Broker[] brokers, string clusterId, int controllerId, TopicMetadata[] topics)
        {
            Brokers = brokers;
            ClusterId = clusterId;
            ControllerId = controllerId;
            Topics = topics;
        }

        public IReadOnlyCollection<Broker> Brokers { get; }
        public string ClusterId { get; }
        public int ControllerId { get; }
        public IReadOnlyCollection<TopicMetadata> Topics { get; }

        public class Broker
        {
            public Broker(int nodeId, string host, int port, string rack)
            {
                NodeId = nodeId;
                Host = host;
                Port = port;
                Rack = rack;
            }

            public int NodeId { get; }
            public string Host { get; }
            public int Port { get; }
            public string Rack { get; }
        }

        public class TopicMetadata
        {
            public TopicMetadata(short errorCode)
            {
                ErrorCode = errorCode;
            }
            public TopicMetadata(string topicName, bool isInternal, PartitionMetadata[] partitions)
            {
                TopicName = topicName;
                IsInternal = isInternal;
                Partitions = partitions;
            }

            public short ErrorCode { get; }
            public string TopicName { get; }
            public bool IsInternal { get; }
            public IReadOnlyCollection<PartitionMetadata> Partitions { get; }
        }

        public class PartitionMetadata
        {
            public PartitionMetadata(short errorCode)
            {
                ErrorCode = errorCode;
            }
            public PartitionMetadata(int partitionId, int leader, int[] replicas, int[] isr)
            {
                PartitionId = partitionId;
                Leader = leader;
                Replicas = replicas;
                Isr = isr;
            }

            public short ErrorCode { get; }
            public int PartitionId { get; }
            public int Leader { get; }
            public IReadOnlyCollection<int> Replicas { get; }
            public IReadOnlyCollection<int> Isr { get; }
        }
    }
}
