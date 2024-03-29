﻿using System.Reflection;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Tavenem.DataStorage.Cosmos;

/// <summary>
/// A converter for <see cref="CosmosPagedList{T}"/>.
/// </summary>
public class CosmosPagedListConverter : JsonConverterFactory
{
    /// <summary>
    /// When overridden in a derived class, determines whether the converter instance can
    /// convert the specified object type.
    /// </summary>
    /// <param name="typeToConvert">
    /// The type of the object to check whether it can be converted by this converter instance.
    /// </param>
    /// <returns>
    /// <see langword="true" /> if the instance can convert the specified object type;
    /// otherwise, <see langword="false" />.
    /// </returns>
    public override bool CanConvert(Type typeToConvert)
        => typeToConvert.IsGenericType
        && typeToConvert.GetGenericTypeDefinition() == typeof(CosmosPagedList<>);

    /// <summary>Creates a converter for a specified type.</summary>
    /// <param name="typeToConvert">The type handled by the converter.</param>
    /// <param name="options">The serialization options to use.</param>
    /// <returns>
    /// A <see cref="JsonConverter"/>.
    /// </returns>
    public override JsonConverter CreateConverter(Type typeToConvert, JsonSerializerOptions options)
    {
        var valueType = typeToConvert.GetGenericArguments()[0];

        return (JsonConverter)Activator.CreateInstance(
            typeof(CosmosPagedListConverterInner<>).MakeGenericType(new Type[] { valueType }),
            BindingFlags.Instance | BindingFlags.Public,
            binder: null,
            args: null,
            culture: null)!;
    }

    private class CosmosPagedListConverterInner<T> : JsonConverter<CosmosPagedList<T>>
    {
        /// <summary>Reads and converts the JSON to type <typeparamref name="T" />.</summary>
        /// <param name="reader">The reader.</param>
        /// <param name="typeToConvert">The type to convert.</param>
        /// <param name="options">An object that specifies serialization options to use.</param>
        /// <returns>The converted value.</returns>
        public override CosmosPagedList<T>? Read(
            ref Utf8JsonReader reader,
            Type typeToConvert,
            JsonSerializerOptions options)
        {
            if (reader.TokenType != JsonTokenType.StartObject)
            {
                throw new JsonException();
            }

            var collection = new List<T>();
            var pageNumber = 1L;
            var pageSize = 0L;
            string? continuationToken = null;

            while (reader.Read())
            {
                if (reader.TokenType == JsonTokenType.EndObject)
                {
                    return new CosmosPagedList<T>(collection, pageNumber, pageSize, continuationToken);
                }

                if (reader.TokenType != JsonTokenType.PropertyName)
                {
                    throw new JsonException();
                }

                switch (reader.GetString())
                {
                    case nameof(PagedList<T>.PageNumber):
                        if (!reader.Read()
                            || reader.TokenType != JsonTokenType.Number
                            || !reader.TryGetInt64(out pageNumber))
                        {
                            throw new JsonException();
                        }
                        break;
                    case nameof(PagedList<T>.PageSize):
                        if (!reader.Read()
                            || reader.TokenType != JsonTokenType.Number
                            || !reader.TryGetInt64(out pageSize))
                        {
                            throw new JsonException();
                        }
                        break;
                    case nameof(CosmosPagedList<T>.ContinuationToken):
                        if (!reader.Read())
                        {
                            throw new JsonException();
                        }
                        continuationToken = reader.GetString();
                        break;
                    case "List":
                        collection = JsonSerializer.Deserialize<List<T>>(ref reader, options);
                        break;
                }
            }

            throw new JsonException();
        }

        /// <summary>Writes a specified value as JSON.</summary>
        /// <param name="writer">The writer to write to.</param>
        /// <param name="value">The value to convert to JSON.</param>
        /// <param name="options">An object that specifies serialization options to use.</param>
        public override void Write(
            Utf8JsonWriter writer,
            CosmosPagedList<T> value,
            JsonSerializerOptions options)
        {
            writer.WriteStartObject();
            writer.WriteNumber(nameof(PagedList<T>.PageNumber), value.PageNumber);
            writer.WriteNumber(nameof(PagedList<T>.PageSize), value.PageSize);
            writer.WriteString(nameof(CosmosPagedList<T>.ContinuationToken), value.ContinuationToken);
            writer.WritePropertyName("List");
            JsonSerializer.Serialize(writer, value.ToList(), options);
            writer.WriteEndObject();
        }
    }
}
