using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;

namespace Kafka.Serializers.Json;

public class JsonDataSerializer<T> : BaseJsonDataSerializer<T>
{
    public JsonDataSerializer()
    {
        _serializer = JsonSerializer.CreateDefault(
            new JsonSerializerSettings
            {
                ContractResolver = new DefaultContractResolver()
            });
    }
}