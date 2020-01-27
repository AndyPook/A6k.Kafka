using System;
using System.Buffers;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;
using Microsoft.AspNetCore.Connections;

using Bedrock.Framework.Protocols;

namespace A6k.Kafka
{
    public partial class KafkaConnection : IAsyncDisposable
    {
        private readonly ConnectionContext connection;
        private readonly string clientId;

        private int correlationId = 0;
        private ChannelWriter<Op> outboundWriter;
        private ChannelWriter<Op> inflightWriter;

        public KafkaConnection(ConnectionContext connection, string clientId)
        {
            this.connection = connection;
            this.clientId = clientId;

            StartOutbound();
            StartInbound();
        }

        private ValueTask<TResponse> SendRequest<TRequest, TResponse>(short apikey, short version, TRequest request, IMessageWriter<TRequest> messageWriter, IMessageReader<TResponse> messageReader)
        {
            var op = new Op<TRequest, TResponse>
            {
                CorrelationId = Interlocked.Increment(ref correlationId),
                ApiKey = apikey,
                Version = version,
                Request = request,
                MessageWriter = messageWriter,
                MessageReader = messageReader
            };
            outboundWriter.TryWrite(op);
            return op.GetResponse();
        }

        private void StartOutbound(CancellationToken cancellationToken = default)
        {
            // adding a bound here just to protect myself
            // I wouldn't expect this to grow too large
            // should add some metrics for monitoring
            var channel = Channel.CreateBounded<Op>(new BoundedChannelOptions(100) { SingleReader = true });
            outboundWriter = channel.Writer;
            var reader = channel.Reader;

            _ = ProcessOutbound(reader, cancellationToken);
        }

        private async Task ProcessOutbound(ChannelReader<Op> outboundReader, CancellationToken cancellationToken)
        {
            await Task.Yield();

            try
            {
                while (await outboundReader.WaitToReadAsync(cancellationToken))
                {
                    while (outboundReader.TryRead(out var op))
                    {
                        await SendRequest(op);
                    }
                }
            }
            catch (OperationCanceledException) { }
            finally
            {
            }

            async ValueTask SendRequest(Op op)
            {
                inflightWriter.TryWrite(op);
                var buffer = new MemoryBufferWriter<byte>();

                // write v1 Header
                buffer.WriteShort(op.ApiKey);
                buffer.WriteShort(op.Version);
                buffer.WriteInt(op.CorrelationId);
                buffer.WriteString(clientId);

                op.WriteMessage(buffer);

                connection.Transport.Output.WriteInt((int)buffer.Length);
                buffer.CopyTo(connection.Transport.Output);
                await connection.Transport.Output.FlushAsync().ConfigureAwait(false);
            }
        }

        private void StartInbound(CancellationToken cancellationToken = default)
        {
            // adding a bound here just to protect myself
            // I wouldn't expect this to grow too large
            // should add some metrics for monitoring
            var channel = Channel.CreateBounded<Op>(new BoundedChannelOptions(100) { SingleReader = true });
            inflightWriter = channel.Writer;
            var reader = channel.Reader;

            _ = ProcessInbound(reader, cancellationToken);
        }
        private async ValueTask ProcessInbound(ChannelReader<Op> inflightReader, CancellationToken cancellationToken)
        {
            await Task.Yield();
            var headerReader = new KafkaResponseHeaderReader();
            var reader = connection.CreateReader();

            while (true)
            {
                try
                {
                    var result = await reader.ReadAsync(headerReader);
                    var header = result.Message;

                    if (result.IsCompleted)
                        break;
                    reader.Advance();

                    if (!inflightReader.TryRead(out var op) || op.CorrelationId != header.CorrelationId)
                        throw new InvalidOperationException("no outstanding op for correlationId: " + header.CorrelationId);

                    await op.ParseResponse(reader);
                }
                finally
                {
                    reader.Advance();
                }
            }
        }

        public ValueTask DisposeAsync() => connection.DisposeAsync();

        private abstract class Op
        {
            public int CorrelationId { get; set; }
            public short ApiKey { get; set; }
            public short Version { get; set; }

            public abstract void WriteMessage(IBufferWriter<byte> output);
            public abstract ValueTask ParseResponse(ProtocolReader reader);
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
                reader.Advance();
                vts.SetResult(result.Message);
            }

            public ValueTask<TResponse> GetResponse() => new ValueTask<TResponse>(this, vts.Version);

            public override void WriteMessage(IBufferWriter<byte> output) => MessageWriter?.WriteMessage(Request, output);

            TResponse IValueTaskSource<TResponse>.GetResult(short token) => vts.GetResult(token);
            ValueTaskSourceStatus IValueTaskSource<TResponse>.GetStatus(short token) => vts.GetStatus(token);
            void IValueTaskSource<TResponse>.OnCompleted(Action<object> continuation, object state, short token, ValueTaskSourceOnCompletedFlags flags)
                => vts.OnCompleted(continuation, state, token, flags);
        }
    }
}
