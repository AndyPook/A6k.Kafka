using System;
using System.Buffers;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;
using Microsoft.AspNetCore.Connections;

using Bedrock.Framework.Protocols;
using Bedrock.Framework;
using System.Net;
using System.Collections.Generic;
using A6k.Kafka.Metadata;
using System.Linq;

namespace A6k.Kafka
{
    public interface IKafkaConnection
    {
        ValueTask<TResponse> SendRequest<TRequest, TResponse>(short apikey, short version, TRequest request, IMessageWriter<TRequest> messageWriter, IMessageReader<TResponse> messageReader);
    }

    public partial class KafkaConnection : IKafkaConnection, IAsyncDisposable
    {
        private readonly Client client;
        private readonly IPEndPoint endpoint;
        private ConnectionContext connection;
        private CancellationTokenSource cancellation = new CancellationTokenSource();

        private int correlationId = 0;
        private ChannelWriter<Op> outboundWriter;
        private ChannelWriter<Op> inflightWriter;

        //public KafkaConnection(ConnectionContext connection, string clientId)
        //{
        //    this.connection = connection ?? throw new ArgumentNullException(nameof(connection));
        //    ClientId = clientId;

        //    StartOutbound(cancellation.Token);
        //    StartInbound(cancellation.Token);
        //}

        public KafkaConnection(Client client, IPEndPoint endpoint, string clientId)
        {
            this.client = client ?? throw new ArgumentNullException(nameof(client));
            this.endpoint = endpoint;
            ClientId = clientId;
        }

        public string ClientId { get; }
        public IReadOnlyList<ApiVersion> ApiVersions { get; private set; }


        public async ValueTask<TResponse> SendRequest<TRequest, TResponse>(short apikey, short version, TRequest request, IMessageWriter<TRequest> messageWriter, IMessageReader<TResponse> messageReader)
        {
            await EnsureConnection();

            var op = new Op<TRequest, TResponse>
            {
                ApiKey = apikey,
                Version = version,
                Request = request,
                MessageWriter = messageWriter,
                MessageReader = messageReader
            };
            outboundWriter.TryWrite(op);
            return await op.GetResponse();
        }

        private async ValueTask EnsureConnection()
        {
            if (connection is null)
            {
                connection = await client.ConnectAsync(endpoint);

                StartOutbound(cancellation.Token);
                StartInbound(cancellation.Token);

                var versions = await ApiVersion();
                ApiVersions = versions.ApiVersions.Select(x => new ApiVersion(x.ApiKey, x.MinVersion, x.MaxVersion)).ToArray();
                Console.WriteLine("connected: " + endpoint);
            }
        }


        private void StartOutbound(CancellationToken cancellationToken = default)
        {
            // adding a bound here just to protect myself
            // I wouldn't expect this to grow too large
            // should add some metrics for monitoring
            var channel = Channel.CreateBounded<Op>(new BoundedChannelOptions(100) { SingleReader = true });
            outboundWriter = channel.Writer;
            var reader = channel.Reader;

            _ = ProcessOutbound(reader, cancellationToken).ConfigureAwait(false);
        }

        private async ValueTask ProcessOutbound(ChannelReader<Op> outboundReader, CancellationToken cancellationToken)
        {
            await Task.Yield();

            try
            {
                await foreach (var op in outboundReader.ReadAllAsync(cancellationToken))
                    await SendRequest(op);
                //{
                //    op.CorrelationId = ++correlationId;
                //    await inflightWriter.WriteAsync(op, cancellationToken);
                //    WriteRequest(op);
                //    await connection.Transport.Output.FlushAsync().ConfigureAwait(false);
                //}
            }
            catch (OperationCanceledException) { /* ignore cancellation */ }

            async ValueTask SendRequest(Op op)
            {
                op.CorrelationId = ++correlationId;
                await inflightWriter.WriteAsync(op, cancellationToken);
                using var buffer = new MemoryBufferWriter();

                // write v1 Header
                buffer.WriteShort(op.ApiKey);
                buffer.WriteShort(op.Version);
                buffer.WriteInt(op.CorrelationId);
                buffer.WriteString(ClientId);

                op.WriteMessage(buffer);

                connection.Transport.Output.WriteInt(buffer.Length);
                buffer.CopyTo(connection.Transport.Output);
                await connection.Transport.Output.FlushAsync().ConfigureAwait(false);
            }

            //void WriteRequest(Op op)
            //{
            //    var buffer = MemoryBufferWriter.CreateWriter();

            //    // write v1 Header
            //    buffer.WriteShort(op.ApiKey);
            //    buffer.WriteShort(op.Version);
            //    buffer.WriteInt(op.CorrelationId);
            //    buffer.WriteString(ClientId);

            //    op.WriteMessage(buffer);

            //    connection.Transport.Output.WriteInt((int)buffer.BytesCommitted);
            //    buffer.CopyTo(connection.Transport.Output);
            //}
        }

        private void StartInbound(CancellationToken cancellationToken = default)
        {
            // adding a bound here just to protect myself
            // I wouldn't expect this to grow too large
            // should add some metrics for monitoring
            var channel = Channel.CreateBounded<Op>(new BoundedChannelOptions(100) { SingleWriter = true, SingleReader = true });
            inflightWriter = channel.Writer;
            var reader = channel.Reader;

            _ = ProcessInbound(reader, cancellationToken).ConfigureAwait(false);
        }

        private async Task ProcessInbound(ChannelReader<Op> inflightReader, CancellationToken cancellationToken)
        {
            await Task.Yield();
            var headerReader = new KafkaResponseHeaderReader();
            var protocolReader = connection.CreateReader();

            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var result = await protocolReader.ReadAsync(headerReader, cancellationToken);
                    if (result.IsCompleted)
                        break;

                    var header = result.Message;
                    protocolReader.Advance();

                    if (!inflightReader.TryRead(out var op) || op.CorrelationId != header.CorrelationId)
                        throw new InvalidOperationException("no outstanding op for correlationId: " + header.CorrelationId);

                    await op.ParseResponse(protocolReader);
                    protocolReader.Advance();
                    op.Dispose();
                }
                catch (Exception)
                {
                    if (!connection.ConnectionClosed.IsCancellationRequested)
                        throw;
                    break;
                }
            }
        }

        public async ValueTask DisposeAsync()
        {
            cancellation.Cancel();
            await connection.DisposeAsync();
        }

        private abstract class Op : IDisposable
        {
            public int CorrelationId { get; set; }
            public short ApiKey { get; set; }
            public short Version { get; set; }

            public abstract void WriteMessage(IBufferWriter<byte> output);
            public abstract ValueTask ParseResponse(ProtocolReader reader);

            public abstract void Dispose();
        }

        private class Op<TRequest, TResponse> : Op, IValueTaskSource<TResponse>
        {
            private ManualResetValueTaskSourceCore<TResponse> vts;

            public TRequest Request { get; set; }

            public IMessageWriter<TRequest> MessageWriter { get; set; }
            public IMessageReader<TResponse> MessageReader { get; set; }

            public override async ValueTask ParseResponse(ProtocolReader reader)
            {
                var result = await reader.ReadAsync(MessageReader);
                vts.SetResult(result.Message);
            }

            public ValueTask<TResponse> GetResponse() => new ValueTask<TResponse>(this, vts.Version);

            public override void WriteMessage(IBufferWriter<byte> output) => MessageWriter?.WriteMessage(Request, output);

            TResponse IValueTaskSource<TResponse>.GetResult(short token) => vts.GetResult(token);
            ValueTaskSourceStatus IValueTaskSource<TResponse>.GetStatus(short token) => vts.GetStatus(token);
            void IValueTaskSource<TResponse>.OnCompleted(Action<object> continuation, object state, short token, ValueTaskSourceOnCompletedFlags flags)
                => vts.OnCompleted(continuation, state, token, flags);

            public override void Dispose() => (Request as IDisposable)?.Dispose();
        }
    }
}
