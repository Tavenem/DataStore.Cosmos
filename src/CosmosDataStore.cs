using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Options;

namespace Tavenem.DataStorage.Cosmos;

/// <summary>
/// A data store for <see cref="IIdItem"/> instances backed by Azure Cosmos DB.
/// </summary>
/// <remarks>
/// <para>
/// The default interface methods which query for a specific item assumes that the <see
/// cref="IIdItem.Id"/> property is the partition key for the container. If a different
/// partition key is used, be sure to use the overload which takes one as a parameter.
/// </para>
/// <para>
/// The default interface methods which retrieve paginated results will only function properly
/// when getting the first page. For all subsequent pages, use the overload which takes a
/// continuation token. This token is exposed as a property of the <see
/// cref="CosmosPagedList{T}"/> which is returned by those overloads, as well as the standard
/// interface methods (it is a subclass of the base <see cref="PagedList{T}"/> and the result
/// can be cast to the more specific <see cref="CosmosPagedList{T}"/> type).
/// </para>
/// </remarks>
/// <param name="cosmosClient">The <see cref="CosmosClient"/> used for all transactions.</param>
/// <param name="databaseName">The name of the database used by this <see cref="IDataStore"/>.</param>
/// <param name="containerName">The name of the container used by this <see cref="IDataStore"/>.</param>
/// <param name="cacheOptions">The options of the in-memory cache.</param>
public class CosmosDataStore(
    CosmosClient cosmosClient,
    string databaseName,
    string containerName,
    IOptions<MemoryCacheOptions>? cacheOptions = null) : CosmosDataStore<IIdItem>(cosmosClient, databaseName, containerName, cacheOptions), IIdItemDataStore
{
    /// <inheritdoc />
    public override string GetKey<T>(T item) => item.Id;

    /// <summary>
    /// Gets the name of the property used to discriminate types, if any.
    /// </summary>
    /// <typeparam name="T">The type of item.</typeparam>
    /// <returns>
    /// The name of the property used to discriminate types, if any.
    /// </returns>
    /// <remarks>
    /// Always returns <see cref="IIdItem.IdItemTypePropertyName"/> for <see cref="CosmosDataStore"/>.
    /// </remarks>
    public override string? GetTypeDiscriminatorName<T>() => IIdItem.IdItemTypePropertyName;

    /// <summary>
    /// Gets the name of the property used to discriminate types, if any.
    /// </summary>
    /// <typeparam name="T">The type of item.</typeparam>
    /// <param name="item">The item whose discriminator property is being obtained.</param>
    /// <returns>
    /// The name of the property used to discriminate types, if any.
    /// </returns>
    /// <remarks>
    /// Always returns <see cref="IIdItem.IdItemTypePropertyName"/> for <see cref="CosmosDataStore"/>.
    /// </remarks>
    public override string? GetTypeDiscriminatorName<T>(T item) => GetTypeDiscriminatorName<T>();

    /// <summary>
    /// Gets the value of the item's type discriminator, if any.
    /// </summary>
    /// <typeparam name="T">The type of item.</typeparam>
    /// <returns>
    /// The value of <typeparamref name="T"/>'s type discriminator, if any.
    /// </returns>
    /// <remarks>
    /// Always returns <see cref="IIdItem.GetIdItemTypeName"/> for <see cref="CosmosDataStore"/>.
    /// </remarks>
    public override string? GetTypeDiscriminatorValue<T>() => T.GetIdItemTypeName();

    /// <summary>
    /// Gets the value of the item's type discriminator, if any.
    /// </summary>
    /// <typeparam name="T">The type of item.</typeparam>
    /// <param name="item">The item whose type discriminator is being obtained.</param>
    /// <returns>
    /// The value of <paramref name="item"/>'s type discriminator, if any.
    /// </returns>
    /// <remarks>
    /// Always returns <see cref="IIdItem.GetIdItemTypeName"/> for <see cref="CosmosDataStore"/>.
    /// </remarks>
    public override string? GetTypeDiscriminatorValue<T>(T item) => GetTypeDiscriminatorValue<T>();
}
