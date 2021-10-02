using SimpleDB.IndexedSearch;

namespace SimpleDB.Core
{
    internal class DBEngine : IDBEngine
    {
        private readonly MapperHolder _mapperHolder;
        private readonly CollectionHolder _collectionHolder;
        private readonly IndexHolder _indexHolder;
        private readonly IndexUpdater _indexUpdater;

        public DBEngine(MapperHolder mapperHolder, IndexHolder indexHolder, IndexUpdater indexUpdater)
        {
            _mapperHolder = mapperHolder;
            _indexHolder = indexHolder;
            _indexUpdater = indexUpdater;
            _collectionHolder = new CollectionHolder();
        }

        public ICollection<TEntity> GetCollection<TEntity>()
        {
            var collection = _collectionHolder.GetOrNull<TEntity>();
            if (collection == null)
            {
                var mapper = _mapperHolder.Get<TEntity>();
                collection = new Collection<TEntity>(mapper, _indexHolder, _indexUpdater);
                _collectionHolder.Add(collection);
            }

            return collection;
        }
    }
}
