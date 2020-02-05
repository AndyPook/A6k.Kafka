using System.Collections.Generic;
using System.Threading.Tasks;

using A6k.Kafka.Messages;

namespace A6k.Kafka
{
    public partial class KafkaConnection
    {
        public ValueTask<ProduceResponse> Produce<TKey, TValue>(Message<TKey, TValue> message, ISerializer<TKey> keyWriter, ISerializer<TValue> valueWriter)
            => SendRequest(ApiKey.Produce, 7, message, new ProduceRequestWriter<TKey, TValue>(keyWriter, valueWriter), new ProduceResponseReader());

        public ValueTask<ProduceResponse> Produce(ProducerRecord message)
            => SendRequest(ApiKey.Produce, 7, message, new ProducerRecordRequestWriter(), new ProduceResponseReader());

        public ValueTask<FetchResponse> Fetch(FetchRequest request)
            => SendRequest(ApiKey.Fetch, 11, request, new FetchRequestWriter(), new FetchResponseReader());

        public ValueTask<MetadataResponse> Metadata(ICollection<string> topics = null)
            => SendRequest(ApiKey.Metadata, 2, topics, new MetadataRequestWriter(), new MetadataResponseReader());

        public ValueTask<MetadataResponse> Metadata(params string[] topics)
            => SendRequest(ApiKey.Metadata, 2, topics, new MetadataRequestWriter(), new MetadataResponseReader());

        public ValueTask<OffsetCommitResponse> OffsetCommit(OffsetCommitRequest request)
            => SendRequest(ApiKey.OffsetCommit, 7, request, new OffsetCommitRequestWriter(), new OffsetCommitResponseReader());

        public ValueTask<OffsetFetchResponse> OffsetFetch(OffsetFetchRequest request)
            => SendRequest(ApiKey.OffsetFetch, 1, request, new OffsetFetchRequestWriter(), new OffsetFetchResponseReader());

        public ValueTask<FindCoordinatorResponse> FindCoordinator(string key, CoordinatorType keyType = CoordinatorType.RD_KAFKA_COORD_GROUP)
            => SendRequest(ApiKey.FindCoordinator, 2, (key, keyType), new FindCoordinatorRequestWriter(), new FindCoordinatorResponseReader());

        public ValueTask<JoinGroupResponse> JoinGroup(JoinGroupRequest request)
            => SendRequest(ApiKey.JoinGroup, 5, request, new JoinGroupRequestWriter(), new JoinGroupResponseReader());

        public ValueTask<HeartbeatResponse> Heartbeat(HeartbeatRequest request)
            => SendRequest(ApiKey.Heartbeat, 3, request, new HeartbeatRequestWriter(), new HeartbeatResponseReader());

        public ValueTask<SyncGroupResponse> SyncGroup(SyncGroupRequest request)
            => SendRequest(ApiKey.SyncGroup, 3, request, new SyncGroupRequestWriter(), new SyncGroupResponseReader());

        public ValueTask<ApiVersionResponse> ApiVersion()
            => SendRequest<object, ApiVersionResponse>(ApiKey.ApiVersion, 0, null, null, new ApiVersionResponseReader());
    }
}
