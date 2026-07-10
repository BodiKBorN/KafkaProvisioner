using Confluent.Kafka;
using Newtonsoft.Json;
using System.Buffers;
using System.Text;

namespace Kafka.Serializers.Json
{
    public abstract class BaseJsonDataSerializer<T> : ISerializer<T>, IDeserializer<T>
    {
        protected static JsonSerializer _serializer;

        public byte[] Serialize(T data, SerializationContext context)
        {
            if (data == null)
            {
                return null;
            }

            var ms = new MemoryStream();
            var sw = new StreamWriter(ms, new UTF8Encoding(false));
            var writer = new JsonTextWriter(sw)
            {
                ArrayPool = JsonArrayPool.Instance
            };

            _serializer.Serialize(writer, data);
            writer.Flush();

            return ms.ToArray();
        }

        public T? Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        {
            if (isNull)
            {
                return default;
            }

            using var ms = new MemoryStream(data.ToArray());
            using var sr = new StreamReader(ms, Encoding.UTF8);
            using var reader = new JsonTextReader(sr)
            {
                ArrayPool = JsonArrayPool.Instance
            };
            return _serializer.Deserialize<T>(reader);
        }
    }

    internal class JsonArrayPool : IArrayPool<char>
    {
        public static readonly JsonArrayPool Instance = new();

        public char[] Rent(int minimumLength) =>
            ArrayPool<char>.Shared.Rent(minimumLength);

        public void Return(char[] array) =>
            ArrayPool<char>.Shared.Return(array);
    }
}
