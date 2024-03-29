﻿using SimpleDB.IndexedSearch;

namespace SimpleDB.Core;

internal class DBEngine : IDBEngine
{
    private readonly ICollectionFactory _collectionFactory;
    private readonly MapperHolder _mapperHolder;
    private readonly CollectionHolder _collectionHolder;
    private readonly IndexHolder _indexHolder;
    private readonly IIndexUpdater _indexUpdater;

    public DBEngine(ICollectionFactory collectionFactory, MapperHolder mapperHolder, IndexHolder indexHolder, IIndexUpdater indexUpdater)
    {
        _collectionFactory = collectionFactory;
        _mapperHolder = mapperHolder;
        _indexHolder = indexHolder;
        _indexUpdater = indexUpdater;
        _collectionHolder = new CollectionHolder();
    }

    public ICollection<TEntity> GetCollection<TEntity>()
    {
        var collection = _collectionHolder.GetOrNull<TEntity>();
        if (collection is null)
        {
            var mapper = _mapperHolder.Get<TEntity>();
            collection = _collectionFactory.Make<TEntity>(mapper, _indexHolder, _indexUpdater);
            _collectionHolder.Add(collection);
        }

        return collection;
    }
}
