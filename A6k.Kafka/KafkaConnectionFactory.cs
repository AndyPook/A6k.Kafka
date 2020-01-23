using System;
using System.Net;
using System.Threading.Tasks;
using Bedrock.Framework;
using Microsoft.Extensions.Logging;

namespace A6k.Kafka
{
    public class KafkaConnectionFactory
    {
        private readonly IServiceProvider serviceProvider;

        public ILogger<KafkaConnectionFactory> Logger { get; }

        public KafkaConnectionFactory(IServiceProvider serviceProvider, ILogger<KafkaConnectionFactory> logger)
        {
            this.serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
            Logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        public async Task<KafkaConnection> CreateConnection(EndPoint endPoint, string clientId)
        {
            var client = new ClientBuilder(serviceProvider)
                .UseSockets()
                .UseConnectionLogging()
                .Build();

            Logger.LogInformation("Connecting to: {broker}", endPoint);
            var connection = await client.ConnectAsync(endPoint);
            var kafka = new KafkaConnection(connection, clientId);
            return kafka;
        }

        public Task<KafkaConnection> CreateConnection(string addr, string clientId)
        {
            IPEndPoint ep = ParseAddress(addr);
            return CreateConnection(ep, clientId);
        }
        public Task<KafkaConnection> CreateConnection(string host, int port, string clientId)
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

        private static IPEndPoint ParseAddress(string server)
        {
            if (!IPEndPoint.TryParse(server, out var ep))
            {
                string s = server.Replace("localhost", "127.0.0.1");
                if (!IPEndPoint.TryParse(s, out ep))
                    throw new InvalidOperationException("invalid broker address");
            }

            return ep;
        }
    }
}
