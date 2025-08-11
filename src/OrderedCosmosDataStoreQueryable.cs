using Microsoft.Azure.Cosmos;
using System.Linq.Expressions;
using Tavenem.DataStorage.Interfaces;

namespace Tavenem.DataStorage.Cosmos;

/// <summary>
/// Provides LINQ operations on a <see cref="CosmosDataStore"/>, after an ordering operation.
/// </summary>
/// <typeparam name="TSource">
/// The type of the elements of the source.
/// </typeparam>
public class OrderedCosmosDataStoreQueryable<TSource>(IDataStore provider, Container container, IOrderedQueryable<TSource> source)
    : CosmosDataStoreQueryable<TSource>(provider, container, source), IOrderedDataStoreQueryable<TSource>
    where TSource : notnull
{
    /// <inheritdoc />
    public IOrderedDataStoreQueryable<TSource> ThenBy<TKey>(Expression<Func<TSource, TKey>> keySelector, IComparer<TKey>? comparer = null)
        => new OrderedCosmosDataStoreQueryable<TSource>(Provider, container, source.ThenBy(keySelector, comparer));

    /// <inheritdoc />
    public IOrderedDataStoreQueryable<TSource> ThenByDescending<TKey>(Expression<Func<TSource, TKey>> keySelector, IComparer<TKey>? comparer = null)
        => new OrderedCosmosDataStoreQueryable<TSource>(Provider, container, source.ThenByDescending(keySelector, comparer));
}
