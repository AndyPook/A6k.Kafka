using System;

namespace A6k.Kafka
{
    public class KafkaException : Exception
    {
        public KafkaException(string message) : base(message) { }

        public KafkaException(string message, Exception innerException) : base(message, innerException) { }
    }
}
