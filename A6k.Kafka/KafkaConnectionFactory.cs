using System;
using System.Net;
using System.Threading.Tasks;
using A6k.Kafka.Metadata;
using Bedrock.Framework;
using Microsoft.Extensions.Logging;

namespace A6k.Kafka
{
    public class KafkaConnectionFactory
    {
        private readonly IServiceProvider serviceProvider;
        private readonly ILogger<KafkaConnectionFactory> logger;

        public KafkaConnectionFactory(IServiceProvider serviceProvider, ILogger<KafkaConnectionFactory> logger)
        {
            this.serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
            this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        public Client CreateClient()
        {
            var client = new ClientBuilder(serviceProvider)
                .UseSockets()
                .UseConnectionLogging()
                .Build();
            return client;
        }

        public KafkaConnection CreateConnection(IPEndPoint endPoint, string clientId)
        {
            var client = CreateClient();

            logger.LogInformation("Connecting to: {broker}", endPoint);
            var kafka = new KafkaConnection(client, endPoint, clientId);
            return kafka;
        }

        public KafkaConnection CreateConnection(string addr, string clientId)
        {
            if (!IPEndPoint.TryParse(addr, out var ep))
            {
                string s = addr.Replace("localhost", "127.0.0.1");
                if (!IPEndPoint.TryParse(s, out ep))
                    throw new InvalidOperationException("invalid broker address");
            }

            return CreateConnection(ep, clientId);
        }
        public KafkaConnection CreateConnection(string host, int port, string clientId)
        {
            if (!IPAddress.TryParse(host, out var addr))
            {
                string s = host.Replace("localhost", "127.0.0.1");
                if (!IPAddress.TryParse(s, out addr))
                    throw new InvalidOperationException("invalid broker address");
            }

            var ep = new IPEndPoint(addr, port);
            return CreateConnection(ep, clientId);
        }
    }
}
