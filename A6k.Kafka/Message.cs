using System;
using System.Collections.Generic;
using System.Threading;

namespace A6k.Kafka
{
    public class Message<TKey, TValue>
    {
        private KeyValueListNode headers;

        public DateTime Timestamp { get; set; }

        public TKey Key { get; set; }

        public TValue Value { get; set; }

        public int HeadersLength
        {
            get
            {
                if (headers == null)
                    return 0;

                int length = 0;
                for (var h = headers; h != null; h = h.Next)
                    length++;
                return length;
            }
        }

        public IEnumerable<KeyValuePair<string, byte[]>> Headers
        {
            get
            {
                if (headers == null)
                    yield break;

                for (var h = headers; h != null; h = h.Next)
                    yield return h.KeyValue;
            }
        }

        public object GetHeader(string key)
        {
            foreach (var keyValue in Headers)
                if (key == keyValue.Key)
                    return keyValue.Value;

            return null;
        }

        public Message<TKey, TValue> AddHeader(string key, byte[] value)
        {
            var currentHeader = headers;
            var newHeaders = new KeyValueListNode { KeyValue = new KeyValuePair<string, byte[]>(key, value) };

            do
            {
                newHeaders.Next = currentHeader;
                currentHeader = Interlocked.CompareExchange(ref headers, newHeaders, currentHeader);
            } while (!ReferenceEquals(newHeaders.Next, currentHeader));

            return this;
        }

        public void ForEachHeader(Action<KeyValuePair<string, byte[]>> action)
        {
            if (headers == null)
                return;
            for (var h = headers; h != null; h = h.Next)
                action(h.KeyValue);
        }

        private partial class KeyValueListNode
        {
            public KeyValuePair<string, byte[]> KeyValue;
            public KeyValueListNode Next;
        }
    }
}
