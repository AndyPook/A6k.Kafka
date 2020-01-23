using System.Collections.Generic;
using System.Threading.Tasks;
using Bedrock.Framework.Protocols;

using A6k.Kafka.Messages;

namespace A6k.Kafka
{
    public partial class KafkaProtocol
    {
        public async Task<ApiVersionResponse> ApiVersion()
        {
            return await SendRequest<object, ApiVersionResponse>(ApiKey.ApiVersion, 0, null, null, new ApiVersionResponseReader());
        }

        public async Task<MetadataResponse> Metadata(ICollection<string> topics = null)
        {
            return await SendRequest(ApiKey.Metadata, 2, topics, new MetadataRequestWriter(), new MetadataResponseReader());
        }
        public async Task<MetadataResponse> Metadata(params string[] topics)
        {
            return await SendRequest(ApiKey.Metadata, 2, topics, new MetadataRequestWriter(), new MetadataResponseReader());
        }

        public async Task<ProduceResponse> Produce<TKey, TValue>(string topic, Message<TKey, TValue> message, IMessageWriter<TKey> keyWriter, IMessageWriter<TValue> valueWriter)
        {
            return await SendRequest(ApiKey.Produce, 7, message, new ProduceRequestWriter<TKey, TValue>(topic, keyWriter, valueWriter), new ProduceResponseReader());
        }

        public async Task<FindCoordinatorResponse> FindCoordinator(string key, CoordinatorType keyType = CoordinatorType.RD_KAFKA_COORD_GROUP)
        {
            return await SendRequest(ApiKey.FindCoordinator, 2, (key, keyType), new FindCoordinatorRequestWriter(), new FindCoordinatorResponseReader());
        }

        public async Task<JoinGroupResponse> JoinGroup(JoinGroupRequest request)
        {
            return await SendRequest(ApiKey.JoinGroup, 5, request, new JoinGroupRequestWriter(), new JoinGroupResponseReader());
        }

        public async Task<FetchResponse> Fetch(FetchRequest request)
        {
            return await SendRequest(ApiKey.Fetch, 11, request, new FetchRequestWriter(), new FetchResponseReader());
        }
    }
}
