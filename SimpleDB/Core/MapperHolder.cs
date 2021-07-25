using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SimpleDB.Core
{
    internal class MapperHolder
    {
        private readonly Dictionary<Type, object> _mappers;

        public MapperHolder(IEnumerable<object> mappers)
        {
            _mappers = mappers.ToDictionary(k => k.GetType().GenericTypeArguments[0], v => v);
        }

        public Mapper<TEntity> Get<TEntity>()
        {
            return (Mapper<TEntity>)_mappers[typeof(TEntity)];
        }
    }
}
