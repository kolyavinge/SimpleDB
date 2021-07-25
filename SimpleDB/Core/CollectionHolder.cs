using System;
using System.Collections.Generic;
using System.Text;

namespace SimpleDB.Core
{
    internal class CollectionHolder
    {
        private readonly Dictionary<Type, object> _collection;

        public CollectionHolder()
        {
            _collection = new Dictionary<Type, object>();
        }

        public Collection<TEntity> GetOrNull<TEntity>()
        {
            return _collection.ContainsKey(typeof(TEntity)) ? (Collection<TEntity>)_collection[typeof(TEntity)] : null;
        }

        public void Add<TEntity>(Collection<TEntity> collection)
        {
            _collection.Add(typeof(TEntity), collection);
        }
    }
}
