using System;

namespace A6k.Kafka
{
    public interface IDeserializer<T>
    {
        T Deserialize(in ReadOnlySpan<byte> input);
    }
}
