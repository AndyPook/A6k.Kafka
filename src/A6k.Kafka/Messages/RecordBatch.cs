using System.Collections.Generic;

namespace A6k.Kafka.Messages
{
    public class RecordBatch
    {
        public long BaseOffset { get; set; }
        public int BatchLength { get; set; }
        public int PartitionLeaderEpoc { get; set; }
        public int Magic { get; set; }
        public int Crc { get; set; }
        public short Attributes { get; set; }
        public int LastOffsetDelta { get; set; }
        public long FirstTimeStamp { get; set; }
        public long MaxTimeStamp { get; set; }
        public long ProducerId { get; set; }
        public short ProducerEpoc { get; set; }
        public int BaseSequence { get; set; }
        public IReadOnlyList<Record> Records { get; set; }

        public class Record
        {
            public byte Attributes { get; set; }
            public long TimeStampDelta { get; set; }
            public long OffsetDelta { get; set; }
            public long Offset { get; set; }
            public byte[] Key { get; set; }
            public byte[] Value { get; set; }
            public IReadOnlyList<Header> Headers { get; set; }

            public class Header
            {
                public Header(string key, byte[] value)
                {
                    Key = key;
                    Value = value;
                }

                public string Key { get; }
                public byte[] Value { get; }
            }
        }
    }
}
