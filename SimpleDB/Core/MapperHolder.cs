using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SimpleDB.Core
{
    internal class MapperHolder
    {
        private readonly Dictionary<Type, IMapper> _mappers;

        public MapperHolder(IEnumerable<IMapper> mappers)
        {
            _mappers = mappers.ToDictionary(k => k.EntityType, v => v);
        }

        public Mapper<TEntity> Get<TEntity>()
        {
            return (Mapper<TEntity>)_mappers[typeof(TEntity)];
        }
    }
}
