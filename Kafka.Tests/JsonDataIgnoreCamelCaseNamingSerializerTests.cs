using System.Text;
using AutoFixture.NUnit3;
using Kafka.Serializers.Json;
using Confluent.Kafka;
using FluentAssertions;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using NUnit.Framework;

namespace Tech.Kafka.Tests;

public class JsonDataIgnoreCamelCaseNamingSerializerTests
{
    [Test, AutoData]
    public void Serialize_WithData_ReturnsSerializedByteArray(
        JObject data,
        JsonDataIgnoreCamelCaseNamingSerializer<JObject> sut,
        SerializationContext serializationContext)
    {
        // Act
        var result = sut.Serialize(data, serializationContext);

        // Assert
        result.Should().NotBeNull()
            .And.BeAssignableTo<byte[]>();

        var json = Encoding.UTF8.GetString(result);
        json.Should().Be(JsonConvert.SerializeObject(data));
    }

    [Test, AutoData]
    public void Serialize_WithNullData_ReturnsNull(
        JsonDataIgnoreCamelCaseNamingSerializer<JObject> sut,
        SerializationContext serializationContext)
    {
        // Arrange
        JObject data = null;

        // Act
        var result = sut.Serialize(data, serializationContext);

        // Assert
        result.Should().BeNull();
    }

    [Test, AutoData]
    public void Deserialize_WithValidData_ReturnsDeserializedObject(
        JObject data,
        JsonDataIgnoreCamelCaseNamingSerializer<JObject> sut,
        SerializationContext serializationContext)
    {
        // Arrange
        var json = JsonConvert.SerializeObject(data);
        var byteArray = Encoding.UTF8.GetBytes(json);

        // Act
        var result = sut.Deserialize(byteArray, false, serializationContext);

        // Assert
        result.Should().BeEquivalentTo(data);
    }
}