using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Options;
using System.Text.Json.Serialization.Metadata;
using Tavenem.DataStorage.Interfaces;

namespace Tavenem.DataStorage.Cosmos;

/// <summary>
/// A data store for <typeparamref name="TItem"/> instances backed by Azure Cosmos DB.
/// </summary>
/// <typeparam name="TItem">A shared interface for all stored items.</typeparam>
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
public abstract class CosmosDataStore<TItem>(
    CosmosClient cosmosClient,
    string databaseName,
    string containerName,
    IOptions<MemoryCacheOptions>? cacheOptions = null) : IDataStore<string, TItem>
    where TItem : notnull
{
    private readonly MemoryCache _cache = new(cacheOptions ?? new MemoryCacheOptions());

    /// <summary>
    /// The <see cref="Microsoft.Azure.Cosmos.Container"/> used for all transactions.
    /// </summary>
    public Container Container { get; set; } = cosmosClient.GetContainer(databaseName, containerName);

    /// <inheritdoc />
    public TimeSpan DefaultCacheTimeout { get; set; } = TimeSpan.FromMinutes(2);

    /// <summary>
    /// <para>
    /// Indicates whether this <see cref="IDataStore"/> implementation allows items to be
    /// cached.
    /// </para>
    /// <para>
    /// This is <see langword="true"/> for <see cref="CosmosDataStore"/>.
    /// </para>
    /// </summary>
    public bool SupportsCaching => true;

    /// <inheritdoc />
    public string? CreateNewIdFor<T>() where T : TItem => Guid.NewGuid().ToString();

    /// <inheritdoc />
    public string? CreateNewIdFor(Type type) => Guid.NewGuid().ToString();

    /// <inheritdoc />
    public async ValueTask<T?> GetItemAsync<T>(string? id, TimeSpan? cacheTimeout = null, CancellationToken cancellationToken = default) where T : TItem
    {
        if (string.IsNullOrEmpty(id))
        {
            return default;
        }
        return await _cache.GetOrCreateAsync(
            id,
            async entry =>
            {
                try
                {
                    var result = await Container.ReadItemAsync<T>(id, new PartitionKey(id)).ConfigureAwait(false);
                    if ((int)result.StatusCode is >= 200 and < 300)
                    {
                        return result.Resource;
                    }
                }
                catch (CosmosException ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
                {
                    return default;
                }
                return default;
            },
            new MemoryCacheEntryOptions { AbsoluteExpirationRelativeToNow = cacheTimeout ?? DefaultCacheTimeout })
            .ConfigureAwait(false);
    }

    /// <inheritdoc />
    public ValueTask<T?> GetItemAsync<T>(
        string? id,
        JsonTypeInfo<T>? typeInfo,
        TimeSpan? cacheTimeout = null,
        CancellationToken cancellationToken = default) where T : TItem
        => GetItemAsync<T>(id, cacheTimeout, cancellationToken);

    /// <inheritdoc />
    public abstract string GetKey<T>(T item) where T : TItem;

    /// <inheritdoc />
    public abstract string? GetTypeDiscriminatorName<T>() where T : TItem;

    /// <inheritdoc />
    public abstract string? GetTypeDiscriminatorName<T>(T item) where T : TItem;

    /// <inheritdoc />
    public abstract string? GetTypeDiscriminatorValue<T>() where T : TItem;

    /// <inheritdoc />
    public abstract string? GetTypeDiscriminatorValue<T>(T item) where T : TItem;

    /// <inheritdoc />
    public IDataStoreQueryable<T> Query<T>(JsonTypeInfo<T>? typeInfo = null) where T : notnull, TItem
        => new CosmosDataStoreQueryable<T>(
        this,
        Container,
        Container
            .GetItemLinqQueryable<T>()
            .Where(x => GetTypeDiscriminatorValue(x)!.StartsWith(GetTypeDiscriminatorValue<T>()!)));

    /// <inheritdoc />
    public async ValueTask<bool> RemoveItemAsync<T>(T? item, CancellationToken cancellationToken = default) where T : TItem
    {
        if (item is null)
        {
            return true;
        }
        var key = GetKey(item);
        if (string.IsNullOrEmpty(key))
        {
            return true;
        }
        return await RemoveItemAsync<T>(key, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask<bool> RemoveItemAsync<T>(string? id, CancellationToken cancellationToken = default) where T : TItem
    {
        if (string.IsNullOrEmpty(id))
        {
            return true;
        }
        var result = await Container
            .DeleteItemAsync<T>(id, new PartitionKey(id), cancellationToken: cancellationToken)
            .ConfigureAwait(false);
        return (int)result.StatusCode is >= 200 and < 300;
    }

    /// <inheritdoc />
    public async ValueTask<T?> StoreItemAsync<T>(T? item, TimeSpan? cacheTimeout = null, CancellationToken cancellationToken = default) where T : TItem
    {
        if (item is null)
        {
            return item;
        }
        var key = GetKey(item);
        if (string.IsNullOrEmpty(key))
        {
            return default;
        }
        var result = await Container
            .UpsertItemAsync(item, new PartitionKey(key), cancellationToken: cancellationToken)
            .ConfigureAwait(false);
        if ((int)result.StatusCode is >= 200 and < 300)
        {
            _cache.Set(key, item, cacheTimeout ?? DefaultCacheTimeout);
        }
        return item;
    }

    /// <inheritdoc />
    public ValueTask<T?> StoreItemAsync<T>(T? item, JsonTypeInfo<T>? typeInfo, TimeSpan? cacheTimeout = null, CancellationToken cancellationToken = default) where T : TItem
        => StoreItemAsync(item, cacheTimeout, cancellationToken);
}
