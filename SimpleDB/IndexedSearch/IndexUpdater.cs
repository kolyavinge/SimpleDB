using System.Collections.Generic;
using System.Linq;
using SimpleDB.Core;
using SimpleDB.Utils.EnumerableExtension;

namespace SimpleDB.IndexedSearch
{
    internal class IndexUpdater
    {
        private readonly Dictionary<string, List<IIndex>> _indexes;
        private readonly MapperHolder _mapperHolder;
        private readonly IIndexFileFactory _indexFileFactory;

        public IndexUpdater(IEnumerable<IIndex> indexes, MapperHolder mapperHolder, IIndexFileFactory indexFileFactory)
        {
            _indexes = indexes.GroupBy(x => x.Meta.EntityName).ToDictionary(k => k.Key, v => v.ToList());
            _mapperHolder = mapperHolder;
            _indexFileFactory = indexFileFactory;
        }

        public void AddToIndexes<TEntity>(IEnumerable<TEntity> entities)
        {
            var mapper = _mapperHolder.Get<TEntity>();
            if (!_indexes.ContainsKey(mapper.EntityName)) return;
            var entityIndexes = _indexes[mapper.EntityName];
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
                SaveIndexFile(index);
            }
        }

        public void UpdateIndexes<TEntity>(IEnumerable<TEntity> entities)
        {
            var mapper = _mapperHolder.Get<TEntity>();
            if (!_indexes.ContainsKey(mapper.EntityName)) return;
            var entityIndexes = _indexes[mapper.EntityName];
            var fieldNumbers = entityIndexes.Select(x => x.Meta.IndexedFieldNumber).ToHashSet();
            fieldNumbers.AddRange(entityIndexes.SelectMany(x => x.Meta.IncludedFieldNumbers));
            var fieldValueDictionary = entities.ToDictionary(
                entity => mapper.GetPrimaryKeyValue(entity),
                entity => mapper.GetFieldValueCollection(entity, fieldNumbers).ToDictionary(k => k.Number, v => v.Value));
            UpdateIndexes(entityIndexes, fieldValueDictionary);
        }

        public void UpdateIndexes(string entityName, IEnumerable<object> primaryKeyValues, IEnumerable<FieldValue> updatedFields)
        {
            if (!_indexes.ContainsKey(entityName)) return;
            var entityIndexes = _indexes[entityName];
            var fieldValueDictionary = primaryKeyValues.ToDictionary(
                primaryKeyValue => primaryKeyValue,
                _ => updatedFields.ToDictionary(k => k.Number, v => v.Value));
            UpdateIndexes(entityIndexes, fieldValueDictionary);
        }

        private void UpdateIndexes(IEnumerable<IIndex> entityIndexes, Dictionary<object, Dictionary<byte, object>> fieldValueDictionary)
        {
            foreach (var index in entityIndexes)
            {
                var updatedIndexItems = new List<UpdatedIndexItem>();
                foreach (var indexValue in index.GetAllIndexValues())
                {
                    foreach (var item in indexValue.Items)
                    {
                        if (!fieldValueDictionary.ContainsKey(item.PrimaryKeyValue)) continue;
                        var fieldValueCollection = fieldValueDictionary[item.PrimaryKeyValue];
                        for (int includedFieldNumberIndex = 0; includedFieldNumberIndex < index.Meta.IncludedFieldNumbers.Length; includedFieldNumberIndex++)
                        {
                            var includedFieldNumber = index.Meta.IncludedFieldNumbers[includedFieldNumberIndex];
                            if (fieldValueCollection.ContainsKey(includedFieldNumber))
                            {
                                item.IncludedFields[includedFieldNumberIndex] = fieldValueCollection[includedFieldNumber];
                            }
                        }
                        if (fieldValueCollection.ContainsKey(index.Meta.IndexedFieldNumber))
                        {
                            var updatedIndexedFieldValue = fieldValueCollection[index.Meta.IndexedFieldNumber];
                            if (updatedIndexedFieldValue != indexValue.IndexedFieldValue)
                            {
                                updatedIndexItems.Add(new UpdatedIndexItem
                                {
                                    UpdatedIndexedFieldValue = updatedIndexedFieldValue,
                                    IndexValue = indexValue,
                                    IndexItem = item
                                });
                            }
                        }
                    }
                }
                foreach (var item in updatedIndexItems)
                {
                    item.IndexValue.Items.RemoveAll(x => x.PrimaryKeyValue == item.IndexItem.PrimaryKeyValue);
                    if (!item.IndexValue.Items.Any())
                    {
                        index.Delete(item.IndexValue.IndexedFieldValue);
                    }
                }
                foreach (var item in updatedIndexItems.GroupBy(x => x.UpdatedIndexedFieldValue))
                {
                    index.Add(item.Key, item.Select(x => x.IndexItem));
                }
                SaveIndexFile(index);
            }
        }

        public void DeleteFromIndexes(string entityName, IEnumerable<object> primaryKeyValues)
        {
            if (!_indexes.ContainsKey(entityName)) return;
            var entityIndexes = _indexes[entityName];
            var primaryKeyValuesSet = primaryKeyValues.ToHashSet();
            foreach (var index in entityIndexes)
            {
                var updatedIndexItems = new List<UpdatedIndexItem>();
                foreach (var indexValue in index.GetAllIndexValues())
                {
                    foreach (var item in indexValue.Items)
                    {
                        if (!primaryKeyValuesSet.Contains(item.PrimaryKeyValue)) continue;
                        updatedIndexItems.Add(new UpdatedIndexItem { IndexValue = indexValue, IndexItem = item });
                    }
                }
                foreach (var item in updatedIndexItems)
                {
                    item.IndexValue.Items.RemoveAll(x => x.PrimaryKeyValue == item.IndexItem.PrimaryKeyValue);
                    if (!item.IndexValue.Items.Any())
                    {
                        index.Delete(item.IndexValue.IndexedFieldValue);
                    }
                }
                SaveIndexFile(index);
            }
        }

        private void SaveIndexFile(IIndex index)
        {
            var mapper = _mapperHolder.Get(index.Meta.EntityName);
            var indexFile = _indexFileFactory.Make(index.Meta.EntityName, index.Meta.Name, mapper.PrimaryKeyType, mapper.FieldMetaCollection);
            indexFile.WriteIndex(index);
        }

        class UpdatedIndexItem
        {
            public object UpdatedIndexedFieldValue;
            public IndexValue IndexValue;
            public IndexItem IndexItem;
        }
    }
}
