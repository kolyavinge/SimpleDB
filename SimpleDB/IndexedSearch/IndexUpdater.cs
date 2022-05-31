using System.Collections.Generic;
using System.Linq;
using SimpleDB.Core;
using SimpleDB.Utils.EnumerableExtension;

namespace SimpleDB.IndexedSearch;

internal interface IIndexUpdater
{
    void AddToIndexes<TEntity>(Mapper<TEntity> mapper, IEnumerable<TEntity> entities);
    void UpdateIndexes<TEntity>(Mapper<TEntity> mapper, IEnumerable<TEntity> entities);
    void UpdateIndexes(EntityMeta entityMeta, IEnumerable<object> primaryKeyValues, IEnumerable<FieldValue> updatedFields);
    void DeleteFromIndexes(EntityMeta entityMeta, IEnumerable<object> primaryKeyValues);
}

internal class IndexUpdater : IIndexUpdater
{
    private readonly Dictionary<string, List<IIndex>> _indexes;
    private readonly IIndexFileFactory? _indexFileFactory;

    public IndexUpdater(IEnumerable<IIndex> indexes, IIndexFileFactory? indexFileFactory)
    {
        _indexes = indexes.GroupBy(x => x.Meta.EntityName).ToDictionary(k => k.Key, v => v.ToList());
        _indexFileFactory = indexFileFactory;
    }

    public void AddToIndexes<TEntity>(Mapper<TEntity> mapper, IEnumerable<TEntity> entities)
    {
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
                var indexedFieldValue = index.Meta.IndexedFieldNumber == PrimaryKey.FieldNumber
                    ? fieldValueCollection.PrimaryKeyValue
                    : fieldValueCollection.FieldValues[index.Meta.IndexedFieldNumber];
                var includedFieldValues = index.Meta.IncludedFieldNumbers.Select(fn => fieldValueCollection.FieldValues[fn]).ToArray();
                var indexItem = new IndexItem(fieldValueCollection.PrimaryKeyValue, includedFieldValues);
                index.Add(indexedFieldValue, indexItem);
            }
            SaveIndexFile(mapper.EntityMeta, index);
        }
    }

    public void UpdateIndexes<TEntity>(Mapper<TEntity> mapper, IEnumerable<TEntity> entities)
    {
        if (!_indexes.ContainsKey(mapper.EntityName)) return;
        var entityIndexes = _indexes[mapper.EntityName];
        var fieldNumbers = entityIndexes.Select(x => x.Meta.IndexedFieldNumber).ToHashSet();
        fieldNumbers.AddRange(entityIndexes.SelectMany(x => x.Meta.IncludedFieldNumbers));
        var fieldValueDictionary = entities.ToDictionary(
            entity => mapper.GetPrimaryKeyValue(entity),
            entity => mapper.GetFieldValueCollection(entity, fieldNumbers).ToDictionary(k => k.Number, v => v.Value));
        UpdateIndexes(mapper.EntityMeta, entityIndexes, fieldValueDictionary);
    }

    public void UpdateIndexes(EntityMeta entityMeta, IEnumerable<object> primaryKeyValues, IEnumerable<FieldValue> updatedFields)
    {
        if (!_indexes.ContainsKey(entityMeta.EntityName)) return;
        var entityIndexes = _indexes[entityMeta.EntityName];
        var fieldValueDictionary = primaryKeyValues.ToDictionary(
            primaryKeyValue => primaryKeyValue,
            _ => updatedFields.ToDictionary(k => k.Number, v => v.Value));
        UpdateIndexes(entityMeta, entityIndexes, fieldValueDictionary);
    }

    private void UpdateIndexes(EntityMeta entityMeta, IEnumerable<IIndex> entityIndexes, Dictionary<object, Dictionary<byte, object?>> fieldValueDictionary)
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
                            updatedIndexItems.Add(new UpdatedIndexItem(updatedIndexedFieldValue, indexValue, item));
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
            foreach (var item in updatedIndexItems.GroupBy(x => x.UpdatedIndexedFieldValue!))
            {
                index.Add(item.Key, item.Select(x => x.IndexItem));
            }
            SaveIndexFile(entityMeta, index);
        }
    }

    public void DeleteFromIndexes(EntityMeta entityMeta, IEnumerable<object> primaryKeyValues)
    {
        if (!_indexes.ContainsKey(entityMeta.EntityName)) return;
        var entityIndexes = _indexes[entityMeta.EntityName];
        var primaryKeyValuesSet = primaryKeyValues.ToHashSet();
        foreach (var index in entityIndexes)
        {
            var updatedIndexItems = new List<UpdatedIndexItem>();
            foreach (var indexValue in index.GetAllIndexValues())
            {
                foreach (var item in indexValue.Items)
                {
                    if (!primaryKeyValuesSet.Contains(item.PrimaryKeyValue)) continue;
                    updatedIndexItems.Add(new UpdatedIndexItem(null, indexValue, item));
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
            SaveIndexFile(entityMeta, index);
        }
    }

    private void SaveIndexFile(EntityMeta entityMeta, IIndex index)
    {
        var indexFile = _indexFileFactory!.Make(entityMeta.EntityName, index.Meta.Name, entityMeta.PrimaryKeyFieldMeta.Type, entityMeta.FieldMetaCollection);
        indexFile.WriteIndex(index);
    }

    class UpdatedIndexItem
    {
        public object? UpdatedIndexedFieldValue { get; }
        public IndexValue IndexValue { get; }
        public IndexItem IndexItem { get; }

        public UpdatedIndexItem(object? updatedIndexedFieldValue, IndexValue indexValue, IndexItem indexItem)
        {
            UpdatedIndexedFieldValue = updatedIndexedFieldValue;
            IndexValue = indexValue;
            IndexItem = indexItem;
        }
    }
}
