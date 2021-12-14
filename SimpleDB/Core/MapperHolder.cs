using System;
using System.Collections.Generic;
using System.Linq;

namespace SimpleDB.Core
{
    internal class MapperHolder
    {
        private readonly Dictionary<Type, IMapper> _mappersByType;
        private readonly Dictionary<string, IMapper> _mappersByEntityName;

        public MapperHolder(IEnumerable<IMapper> mappers)
        {
            _mappersByType = mappers.ToDictionary(k => k.EntityType, v => v);
            _mappersByEntityName = mappers.ToDictionary(k => k.EntityName, v => v);
        }

        public Mapper<TEntity> Get<TEntity>()
        {
            return (Mapper<TEntity>)_mappersByType[typeof(TEntity)];
        }

        public IMapper Get(string entityName)
        {
            return _mappersByEntityName[entityName];
        }
    }
}
