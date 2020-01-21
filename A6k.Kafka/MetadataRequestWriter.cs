using System.Buffers;
using System.Collections.Generic;
using Bedrock.Framework.Protocols;

namespace A6k.Kafka
{
    public class MetadataRequestWriter : IMessageWriter<ICollection<string>>
    {
        public void WriteMessage(ICollection<string> message, IBufferWriter<byte> output)
        {
            if (message == null)
            {
                output.WriteInt(-1);
                return;
            }
            else if (message.Count == 0)
            {
                output.WriteInt(0);
                return;
            }

            output.WriteInt(message.Count);
            foreach (var topic in message)
                output.WriteString(topic);
        }
    }
}
