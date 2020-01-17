using System.Buffers;
using System.Collections.Generic;
using Bedrock.Framework.Protocols;

namespace TestConsole
{
    public class MetadataRequestWriter : IMessageWriter<ICollection<string>>
    {
        public void WriteMessage(ICollection<string> message, IBufferWriter<byte> output)
        {
            if (message == null)
            {
                output.Write(-1);
                return;
            }
            else if (message.Count == 0)
            {
                output.Write(0);
                return;
            }

            output.Write(message.Count);
            foreach (var topic in message)
                output.Write(topic);
        }
    }
}
