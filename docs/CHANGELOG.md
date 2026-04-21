# Changelog

## 0.34.0-0.34.1-preview
### Changed
- Update to .NET 10
- The original `CosmosDataStore` has been divided into three separate implementations:
  - `CosmosDataStore<TItem>` which is an abstract class that allows specifying the item type for stored items
    - `CosmosDataStore<TItem>` implements the updated `IDataStore<string, TItem>` interface (see [the `Tavenem.DataStore` project](https://github.com/Tavenem/DataStore) for details)
  - `CosmosDataStore` which replicates the original by extending `CosmosDataStore<IIdItem>`
    - `CosmosDataStore` implements the updated `IIdItemDataStore` interface (see [the `Tavenem.DataStore` project](https://github.com/Tavenem/DataStore) for details)
- `CosmosDataStoreQueryable<TItem, TSource>` implements the following updated interfaces (see [the `Tavenem.DataStore` project](https://github.com/Tavenem/DataStore) for details):
  - `IDataStoreOrderableQueryable<TSource>`
  - `IDataStoreSelectManyQueryable<TSource>`
  - `IDataStoreSelectQueryable<TSource>`
  - `IDataStoreSkipQueryable<TSource>`
  - `IDataStoreTakeQueryable<TSource>`
  - `IDataStoreWhereQueryable<TSource>`

## 0.33.1-preview
### Changed
- Clarify 1-based indexing of page numbers in `IPagedList`.

## 0.32.0-preview - 0.33.0-preview
### Changed
- Update dependencies

## 0.31.0-preview
### Changed
- Update to .NET 7 preview

## 0.30.0-preview
### Changed
- Update to .NET 6 preview
- Update to C# 10 preview

## 0.29.1-preview
### Added
- Initial preview release