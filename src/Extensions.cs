﻿using Microsoft.Azure.Cosmos;

namespace Tavenem.DataStorage.Cosmos;

/// <summary>
/// Extension methods.
/// </summary>
public static class DataStorageCosmosExtensions
{
    /// <summary>
    /// Returns a <see cref="CosmosPagedList{T}"/> wrapper for the current <see
    /// cref="FeedIterator{T}"/>.
    /// </summary>
    /// <typeparam name="T">The type of items in the collection.</typeparam>
    /// <param name="iterator">The current <see cref="FeedIterator{T}"/>.</param>
    /// <param name="pageNumber">The current page number. The first page is 1.</param>
    /// <param name="pageSize">The page size.</param>
    /// <returns>A <see cref="CosmosPagedList{T}"/> containing the items in the current
    /// collection.</returns>
    public static Task<CosmosPagedList<T>> AsCosmosPagedListAsync<T>(
        this FeedIterator<T>? iterator,
        long pageNumber,
        long pageSize)
        => CosmosPagedList<T>.FromFeedIteratorAsync(iterator, pageNumber, pageSize);
}
