using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using SimpleDB.Core;
using SimpleDB.Utils.EnumerableExtension;

namespace SimpleDB.IndexedSearch
{
    internal class IndexUpdater
    {
        private readonly Dictionary<Type, List<AbstractIndex>> _indexes;
        private readonly string _workingDirectory;
        private readonly MapperHolder _mapperHolder;

        public IndexUpdater(string workingDirectory, IEnumerable<AbstractIndex> indexes, MapperHolder mapperHolder)
        {
            _indexes = indexes.GroupBy(x => x.Meta.EntityType).ToDictionary(k => k.Key, v => v.ToList());
            _workingDirectory = workingDirectory;
            _mapperHolder = mapperHolder;
        }

        public void AddToIndexes<TEntity>(IEnumerable<TEntity> entities)
        {
            if (!_indexes.ContainsKey(typeof(TEntity))) return;
            var entityIndexes = _indexes[typeof(TEntity)];
            var mapper = _mapperHolder.Get<TEntity>();
            var fieldNumbers = entityIndexes.Select(x => x.Meta.IndexedFieldNumber).ToHashSet();
            fieldNumbers.AddRange(entityIndexes.SelectMany(x => x.Meta.IncludedFieldNumbers));
            var fieldValueCollections = entities.Select(entity => new
            {
                PrimaryKeyValue = mapper.GetPrimaryKeyValue(entity),
                FieldValues = mapper.GetFieldValueCollection(entity, fieldNumbers).ToDictionary(k => k.Number, v => v.Value)
            }).ToList();
            foreach (var index in entityIndexes)
            {
                foreach (var fieldValueCollection in fieldValueCollections)
                {
                    var indexedFieldValue = fieldValueCollection.FieldValues[index.Meta.IndexedFieldNumber];
                    var includedFieldValues = index.Meta.IncludedFieldNumbers.Select(fn => fieldValueCollection.FieldValues[fn]).ToArray();
                    var indexItem = new IndexItem { PrimaryKeyValue = fieldValueCollection.PrimaryKeyValue, IncludedFields = includedFieldValues };
                    index.Add(indexedFieldValue, indexItem);
                }
                var indexFileName = Path.Combine(_workingDirectory, IndexFileName.FromEntityName(mapper.EntityName, index.Meta.Name));
                var indexFile = new IndexFile(indexFileName, mapper.PrimaryKeyMapping.PropertyType, mapper.FieldMetaCollection);
                indexFile.WriteIndex(index);
            }
        }
    }
}
