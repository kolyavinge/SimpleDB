using System;
using System.Collections.Generic;
using System.Linq;
using SimpleDB.Core;
using SimpleDB.Utils.EnumerableExtension;

namespace SimpleDB.IndexedSearch
{
    internal class IndexUpdater
    {
        private readonly Dictionary<Type, List<IIndex>> _indexes;
        private readonly MapperHolder _mapperHolder;

        public IndexUpdater(IEnumerable<IIndex> indexes, MapperHolder mapperHolder)
        {
            _indexes = indexes.GroupBy(x => x.Meta.EntityType).ToDictionary(k => k.Key, v => v.ToList());
            _mapperHolder = mapperHolder;
        }

        public IndexUpdater()
        {
            _indexes = new Dictionary<Type, List<IIndex>>();
            _mapperHolder = new MapperHolder(Enumerable.Empty<object>());
        }

        public void AddToIndexes<TEntity>(TEntity entity)
        {
            AddToIndexes<TEntity>(new[] { entity });
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
                var indexFileName = IndexFileName.GetFullFileName(mapper.EntityName, index.Meta.Name);
                var indexFile = new IndexFile(indexFileName, mapper.PrimaryKeyMapping.PropertyType, mapper.FieldMetaCollection);
                indexFile.WriteIndex(index);
            }
        }

        public void UpdateIndexes<TEntity>(TEntity entity)
        {
            UpdateIndexes<TEntity>(new[] { entity });
        }

        public void UpdateIndexes<TEntity>(IEnumerable<TEntity> entities)
        {
            if (!_indexes.ContainsKey(typeof(TEntity))) return;
            var entityIndexes = _indexes[typeof(TEntity)];
            var mapper = _mapperHolder.Get<TEntity>();
            var fieldNumbers = entityIndexes.Select(x => x.Meta.IndexedFieldNumber).ToHashSet();
            fieldNumbers.AddRange(entityIndexes.SelectMany(x => x.Meta.IncludedFieldNumbers));
            var fieldValueDictionary = entities.Select(entity => new
            {
                PrimaryKeyValue = mapper.GetPrimaryKeyValue(entity),
                FieldValues = mapper.GetFieldValueCollection(entity, fieldNumbers).ToDictionary(k => k.Number, v => v.Value)
            }).ToDictionary(k => k.PrimaryKeyValue, v => v.FieldValues);
            foreach (var index in entityIndexes)
            {
                var updatedIndexItems = new List<UpdatedIndexItem>();
                foreach (var indexValue in index.GetAllIndexValues())
                {
                    foreach (var item in indexValue.Items)
                    {
                        if (!fieldValueDictionary.ContainsKey(item.PrimaryKeyValue)) continue;
                        var fieldValueCollection = fieldValueDictionary[item.PrimaryKeyValue];
                        item.IncludedFields = index.Meta.IncludedFieldNumbers.Select(fn => fieldValueCollection[fn]).ToArray();
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
                var indexFileName = IndexFileName.GetFullFileName(mapper.EntityName, index.Meta.Name);
                var indexFile = new IndexFile(indexFileName, mapper.PrimaryKeyMapping.PropertyType, mapper.FieldMetaCollection);
                indexFile.WriteIndex(index);
            }
        }

        public void UpdateIndexes<TEntity>(IEnumerable<object> primaryKeyValues, IEnumerable<FieldValue> updatedFields)
        {
            if (!_indexes.ContainsKey(typeof(TEntity))) return;
            var entityIndexes = _indexes[typeof(TEntity)];
            var mapper = _mapperHolder.Get<TEntity>();
            var fieldValueDictionary = primaryKeyValues.Select(primaryKeyValue => new
            {
                PrimaryKeyValue = primaryKeyValue,
                FieldValues = updatedFields.ToDictionary(k => k.Number, v => v.Value)
            }).ToDictionary(k => k.PrimaryKeyValue, v => v.FieldValues);
            foreach (var index in entityIndexes)
            {
                var updatedIndexItems = new List<UpdatedIndexItem>();
                foreach (var indexValue in index.GetAllIndexValues())
                {
                    foreach (var item in indexValue.Items)
                    {
                        if (!fieldValueDictionary.ContainsKey(item.PrimaryKeyValue)) continue;
                        var fieldValueCollection = fieldValueDictionary[item.PrimaryKeyValue];
                        item.IncludedFields = index.Meta.IncludedFieldNumbers.Select(fn => fieldValueCollection[fn]).ToArray();
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
                var indexFileName = IndexFileName.GetFullFileName(mapper.EntityName, index.Meta.Name);
                var indexFile = new IndexFile(indexFileName, mapper.PrimaryKeyMapping.PropertyType, mapper.FieldMetaCollection);
                indexFile.WriteIndex(index);
            }
        }

        public void DeleteFromIndexes<TEntity>(object primaryKeyValue)
        {
            DeleteFromIndexes<TEntity>(new[] { primaryKeyValue });
        }

        public void DeleteFromIndexes<TEntity>(IEnumerable<object> primaryKeyValues)
        {
            if (!_indexes.ContainsKey(typeof(TEntity))) return;
            var entityIndexes = _indexes[typeof(TEntity)];
            var mapper = _mapperHolder.Get<TEntity>();
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
                var indexFileName = IndexFileName.GetFullFileName(mapper.EntityName, index.Meta.Name);
                var indexFile = new IndexFile(indexFileName, mapper.PrimaryKeyMapping.PropertyType, mapper.FieldMetaCollection);
                indexFile.WriteIndex(index);
            }
        }

        struct UpdatedIndexItem
        {
            public object UpdatedIndexedFieldValue;
            public IndexValue IndexValue;
            public IndexItem IndexItem;
        }
    }
}
