using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;

namespace Kafka.Serializers.Json;

public class JsonDataIgnoreCamelCaseNamingSerializer<T> : BaseJsonDataSerializer<T>
{
    public JsonDataIgnoreCamelCaseNamingSerializer()
    {
        _serializer = JsonSerializer.CreateDefault(
            new JsonSerializerSettings
            {
                ContractResolver = new DefaultContractResolver
                {
                    NamingStrategy = new CamelCaseNamingStrategy()
                    {
                        OverrideSpecifiedNames = true
                    }
                }
            });
    }
}