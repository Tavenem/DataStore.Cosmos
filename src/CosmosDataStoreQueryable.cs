using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Linq;
using System.Linq.Expressions;
using Tavenem.DataStorage.Cosmos;
using Tavenem.DataStorage.Interfaces;

namespace Tavenem.DataStorage;

/// <summary>
/// Provides LINQ operations on a <see cref="CosmosDataStore"/>'s data.
/// </summary>
/// <typeparam name="TSource">
/// The type of the elements of the source.
/// </typeparam>
public class CosmosDataStoreQueryable<TSource>(IDataStore provider, Container container, IQueryable<TSource> source)
    : IDataStoreOrderableQueryable<TSource>,
    IDataStoreSelectManyQueryable<TSource>,
    IDataStoreSelectQueryable<TSource>,
    IDataStoreSkipQueryable<TSource>,
    IDataStoreTakeQueryable<TSource>,
    IDataStoreWhereQueryable<TSource>
    where TSource : notnull
{
    /// <summary>
    /// The <see cref="Microsoft.Azure.Cosmos.Container"/> used for all transactions.
    /// </summary>
    protected readonly Container container = container;

    private string? _continuationToken;
    private int _lastPage;

    /// <inheritdoc />
    public IDataStore Provider { get; } = provider;

    /// <summary>
    /// Determines whether a sequence contains any elements.
    /// </summary>
    /// <param name="cancellationToken">A <see cref="CancellationToken"/>.</param>
    /// <returns>
    /// <see langword="true"/> if the source sequence contains any elements; otherwise, <see
    /// langword="false"/>.
    /// </returns>
    public async ValueTask<bool> AnyAsync(CancellationToken cancellationToken = default)
    {
        var iterator = container.GetItemQueryIterator<TSource>(
            source.ToQueryDefinition(),
            null,
            new QueryRequestOptions { MaxItemCount = 1 });
        if (!iterator.HasMoreResults)
        {
            return false;
        }

        var results = await iterator.ReadNextAsync(cancellationToken).ConfigureAwait(false);
        return results.Count > 0;
    }

    /// <summary>
    /// Determines whether any element of a sequence satisfies a condition.
    /// </summary>
    /// <param name="predicate">A function to test each element for a condition.</param>
    /// <param name="cancellationToken">A <see cref="CancellationToken"/>.</param>
    /// <returns>
    /// <see langword="true"/>> if the source sequence is not empty and at least one of its elements
    /// passes the test in the specified predicate; otherwise, <see langword="false"/>.
    /// </returns>
    public async ValueTask<bool> AnyAsync(Expression<Func<TSource, bool>> predicate, CancellationToken cancellationToken = default)
    {
        var iterator = container.GetItemQueryIterator<TSource>(
            source.Where(predicate).ToQueryDefinition(),
            null,
            new QueryRequestOptions { MaxItemCount = 1 });
        if (iterator.HasMoreResults)
        {
            var results = await iterator.ReadNextAsync(cancellationToken).ConfigureAwait(false);
            return results.Count > 0;
        }
        else
        {
            return false;
        }
    }

    /// <summary>
    /// Returns the number of elements in this source.
    /// </summary>
    /// <param name="cancellationToken">A <see cref="CancellationToken"/>.</param>
    /// <returns>The number of elements in the this source.</returns>
    /// <exception cref="OverflowException">
    /// The number of elements in this source is larger than <see cref="int.MaxValue"/>.
    /// </exception>
    public async ValueTask<int> CountAsync(CancellationToken cancellationToken = default)
        => await source.CountAsync(cancellationToken).ConfigureAwait(false);

    /// <inheritdoc />
    async IAsyncEnumerator<TSource> IAsyncEnumerable<TSource>.GetAsyncEnumerator(CancellationToken cancellationToken)
    {
        var iterator = source.ToFeedIterator();
        while (iterator.HasMoreResults)
        {
            foreach (var item in await iterator.ReadNextAsync(cancellationToken).ConfigureAwait(false))
            {
                yield return item;
            }
        }
    }

    /// <inheritdoc />
    public async ValueTask<IPagedList<TSource>> GetPageAsync(
        int pageNumber,
        int pageSize,
        CancellationToken cancellationToken = default)
    {
        CosmosPagedList<TSource> page;
        if (!string.IsNullOrEmpty(_continuationToken)
            && pageNumber == _lastPage + 1)
        {
            page = await container
                .GetItemQueryIterator<TSource>(
                    source.ToQueryDefinition(),
                    _continuationToken,
                    new QueryRequestOptions { MaxItemCount = pageSize + 1 })
                .AsCosmosPagedListAsync(pageNumber, pageSize)
                .ConfigureAwait(false);
        }
        else
        {
            page = await container
                .GetItemQueryIterator<TSource>(
                    source.Skip((pageNumber - 1) * pageSize).ToQueryDefinition(),
                    null,
                    new QueryRequestOptions { MaxItemCount = pageSize + 1 })
                .AsCosmosPagedListAsync(pageNumber, pageSize)
                .ConfigureAwait(false);
        }
        _continuationToken = page.ContinuationToken;
        _lastPage = pageNumber;
        return page;
    }

    /// <summary>
    /// Returns the maximum value in a generic sequence.
    /// </summary>
    /// <param name="comparer">Ignored. Not supported by EntityFramework.</param>
    /// <param name="cancellationToken">A <see cref="CancellationToken"/>.</param>
    /// <returns>The maximum value in the sequence.</returns>
#pragma warning disable IDE0060 // Remove unused parameter. Provided to match extension on IAsyncEnumerable<T> so that this implementation takes precedence.
    public async ValueTask<TSource?> MaxAsync(IComparer<TSource>? comparer = null, CancellationToken cancellationToken = default)
#pragma warning restore IDE0060 // Remove unused parameter
        => await source.MaxAsync(cancellationToken).ConfigureAwait(false);

    /// <summary>
    /// Returns the minimum value in a generic sequence.
    /// </summary>
    /// <param name="comparer">Ignored. Not supported by EntityFramework.</param>
    /// <param name="cancellationToken">A <see cref="CancellationToken"/>.</param>
    /// <returns>The minimum value in the sequence.</returns>
#pragma warning disable IDE0060 // Remove unused parameter. Provided to match extension on IAsyncEnumerable<T> so that this implementation takes precedence.
    public async ValueTask<TSource?> MinAsync(IComparer<TSource>? comparer = null, CancellationToken cancellationToken = default)
#pragma warning restore IDE0060 // Remove unused parameter
        => await source.MinAsync(cancellationToken).ConfigureAwait(false);

    /// <inheritdoc />
    public IOrderedDataStoreQueryable<TSource> Order(IComparer<TSource>? comparer = null)
        => comparer is null
        ? new OrderedCosmosDataStoreQueryable<TSource>(Provider, container, source.Order())
        : new OrderedCosmosDataStoreQueryable<TSource>(Provider, container, source.Order(comparer));

    /// <inheritdoc />
    public IOrderedDataStoreQueryable<TSource> OrderBy<TKey>(Expression<Func<TSource, TKey>> keySelector, IComparer<TKey>? comparer = null)
        => new OrderedCosmosDataStoreQueryable<TSource>(Provider, container, source.OrderBy(keySelector, comparer));

    /// <inheritdoc />
    public IOrderedDataStoreQueryable<TSource> OrderByDescending<TKey>(Expression<Func<TSource, TKey>> keySelector, IComparer<TKey>? comparer = null)
        => new OrderedCosmosDataStoreQueryable<TSource>(Provider, container, source.OrderByDescending(keySelector, comparer));

    /// <inheritdoc />
    public IOrderedDataStoreQueryable<TSource> OrderDescending(IComparer<TSource>? comparer = null)
        => comparer is null
        ? new OrderedCosmosDataStoreQueryable<TSource>(Provider, container, source.OrderDescending())
        : new OrderedCosmosDataStoreQueryable<TSource>(Provider, container, source.OrderDescending(comparer));

    /// <inheritdoc />
    public IDataStoreSelectQueryable<TResult> Select<TResult>(Expression<Func<TSource, TResult>> selector) where TResult : notnull
        => new CosmosDataStoreQueryable<TResult>(Provider, container, source.Select(selector));

    /// <inheritdoc />
    public IDataStoreSelectQueryable<TResult> Select<TResult>(Expression<Func<TSource, int, TResult>> selector) where TResult : notnull
        => new CosmosDataStoreQueryable<TResult>(Provider, container, source.Select(selector));

    /// <inheritdoc />
    public IDataStoreSelectManyQueryable<TResult> SelectMany<TCollection, TResult>(
        Expression<Func<TSource, IEnumerable<TCollection>>> collectionSelector,
        Expression<Func<TSource, TCollection, TResult>> resultSelector) where TResult : notnull
        => new CosmosDataStoreQueryable<TResult>(Provider, container, source.SelectMany(collectionSelector, resultSelector));

    /// <inheritdoc />
    public IDataStoreSelectManyQueryable<TResult> SelectMany<TCollection, TResult>(
        Expression<Func<TSource, int, IEnumerable<TCollection>>> collectionSelector,
        Expression<Func<TSource, TCollection, TResult>> resultSelector) where TResult : notnull
        => new CosmosDataStoreQueryable<TResult>(Provider, container, source.SelectMany(collectionSelector, resultSelector));

    /// <inheritdoc />
    public IDataStoreSkipQueryable<TSource> Skip(int count)
        => new CosmosDataStoreQueryable<TSource>(Provider, container, source.Skip(count));

    /// <inheritdoc />
    public IDataStoreTakeQueryable<TSource> Take(int count)
        => new CosmosDataStoreQueryable<TSource>(Provider, container, source.Take(count));

    /// <inheritdoc />
    public IDataStoreTakeQueryable<TSource> Take(Range range)
        => new CosmosDataStoreQueryable<TSource>(Provider, container, source.Take(range));

    /// <inheritdoc />
    public IDataStoreWhereQueryable<TSource> Where(Expression<Func<TSource, bool>> predicate)
        => new CosmosDataStoreQueryable<TSource>(Provider, container, source.Where(predicate));

    /// <inheritdoc />
    public IDataStoreWhereQueryable<TSource> Where(Expression<Func<TSource, int, bool>> predicate)
        => new CosmosDataStoreQueryable<TSource>(Provider, container, source.Where(predicate));
}
