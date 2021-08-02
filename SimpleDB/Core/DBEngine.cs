using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using SimpleDB.Infrastructure;

namespace SimpleDB.Core
{
    internal class DBEngine : IDBEngine
    {
        private string _workingDirectory;
        private MapperHolder _mapperHolder;
        private CollectionHolder _collectionHolder;

        public DBEngine(string workingDirectory, MapperHolder mapperHolder)
        {
            _workingDirectory = workingDirectory;
            _mapperHolder = mapperHolder;
            _collectionHolder = new CollectionHolder();
        }

        public void Dispose()
        {
            _collectionHolder.GetAllCollections().Cast<IDisposable>().Each(x => x.Dispose());
        }

        public ICollection<TEntity> GetCollection<TEntity>()
        {
            var collection = _collectionHolder.GetOrNull<TEntity>();
            if (collection == null)
            {
                var mapper = _mapperHolder.Get<TEntity>();
                collection = new Collection<TEntity>(_workingDirectory, mapper);
                _collectionHolder.Add(collection);
            }

            return collection;
        }
    }
}
