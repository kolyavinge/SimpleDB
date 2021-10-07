using System;
using System.Collections.Generic;
using System.Linq;
using SimpleDB.Core;
using SimpleDB.Queries;

namespace SimpleDB.IndexedSearch
{
    internal class IndexResult
    {
        public IndexMeta IndexMeta { get; set; }

        public IndexValue IndexValue { get; set; }

        public IEnumerable<FieldValueCollection> ToFieldValueCollections(IDictionary<object, PrimaryKey> primaryKeys)
        {
            var converter = new IndexValueConverter(IndexMeta);
            foreach (var indexItem in IndexValue.Items)
            {
                var fieldValueCollection = new FieldValueCollection { PrimaryKey = primaryKeys[indexItem.PrimaryKeyValue] };
                fieldValueCollection.AddRange(converter.GetFieldValues(IndexValue, indexItem));
                yield return fieldValueCollection;
            }
        }
    }

    internal class IndexHolder
    {
        private readonly Dictionary<Type, List<IIndex>> _indexes;

        public IndexHolder(IEnumerable<IIndex> indexes)
        {
            _indexes = indexes.GroupBy(x => x.Meta.EntityType).ToDictionary(k => k.Key, v => v.ToList());
        }

        public IndexHolder()
        {
            _indexes = new Dictionary<Type, List<IIndex>>();
        }

        public bool AnyIndexFor(Type entityType, ISet<byte> fieldNumbers)
        {
            if (!_indexes.ContainsKey(entityType)) return false;
            return _indexes[entityType].Any(x => fieldNumbers.Contains(x.Meta.IndexedFieldNumber));
        }

        public IEnumerable<IndexResult> GetIndexResult(Type operationType, bool isNotApplied, Type entityType, byte fieldNumber, object fieldValue)
        {
            if (!_indexes.ContainsKey(entityType)) return null;
            var index = _indexes[entityType].FirstOrDefault(i => i.Meta.IndexedFieldNumber == fieldNumber);
            if (index != null)
            {
                if (operationType == typeof(WhereClause.EqualsOperation) && !isNotApplied)
                {
                    var indexValue = index.GetEquals(fieldValue);
                    return new[] { new IndexResult { IndexMeta = index.Meta, IndexValue = indexValue } };
                }
                else if (operationType == typeof(WhereClause.EqualsOperation) && isNotApplied)
                {
                    var indexValues = index.GetNotEquals(fieldValue);
                    return indexValues.Select(indexValue => new IndexResult { IndexMeta = index.Meta, IndexValue = indexValue });
                }
                else if (operationType == typeof(WhereClause.LessOperation) && !isNotApplied)
                {
                    var indexValues = index.GetLess(fieldValue);
                    return indexValues.Select(indexValue => new IndexResult { IndexMeta = index.Meta, IndexValue = indexValue });
                }
                else if (operationType == typeof(WhereClause.LessOperation) && isNotApplied)
                {
                    var indexValues = index.GetGreatOrEquals(fieldValue);
                    return indexValues.Select(indexValue => new IndexResult { IndexMeta = index.Meta, IndexValue = indexValue });
                }
                else if (operationType == typeof(WhereClause.GreatOperation) && !isNotApplied)
                {
                    var indexValues = index.GetGreat(fieldValue);
                    return indexValues.Select(indexValue => new IndexResult { IndexMeta = index.Meta, IndexValue = indexValue });
                }
                else if (operationType == typeof(WhereClause.GreatOperation) && isNotApplied)
                {
                    var indexValues = index.GetLessOrEquals(fieldValue);
                    return indexValues.Select(indexValue => new IndexResult { IndexMeta = index.Meta, IndexValue = indexValue });
                }
                else if (operationType == typeof(WhereClause.LessOrEqualsOperation) && !isNotApplied)
                {
                    var indexValues = index.GetLessOrEquals(fieldValue);
                    return indexValues.Select(indexValue => new IndexResult { IndexMeta = index.Meta, IndexValue = indexValue });
                }
                else if (operationType == typeof(WhereClause.LessOrEqualsOperation) && isNotApplied)
                {
                    var indexValues = index.GetGreat(fieldValue);
                    return indexValues.Select(indexValue => new IndexResult { IndexMeta = index.Meta, IndexValue = indexValue });
                }
                else if (operationType == typeof(WhereClause.GreatOrEqualsOperation) && !isNotApplied)
                {
                    var indexValues = index.GetGreatOrEquals(fieldValue);
                    return indexValues.Select(indexValue => new IndexResult { IndexMeta = index.Meta, IndexValue = indexValue });
                }
                else if (operationType == typeof(WhereClause.GreatOrEqualsOperation) && isNotApplied)
                {
                    var indexValues = index.GetLess(fieldValue);
                    return indexValues.Select(indexValue => new IndexResult { IndexMeta = index.Meta, IndexValue = indexValue });
                }
                else if (operationType == typeof(WhereClause.LikeOperation) && !isNotApplied)
                {
                    var indexValues = index.GetLike(fieldValue);
                    return indexValues.Select(indexValue => new IndexResult { IndexMeta = index.Meta, IndexValue = indexValue });
                }
                else if (operationType == typeof(WhereClause.LikeOperation) && isNotApplied)
                {
                    var indexValues = index.GetNotLike(fieldValue);
                    return indexValues.Select(indexValue => new IndexResult { IndexMeta = index.Meta, IndexValue = indexValue });
                }
                else if (operationType == typeof(WhereClause.InOperation) && !isNotApplied)
                {
                    var indexValues = index.GetIn((IEnumerable<object>)fieldValue);
                    return indexValues.Select(indexValue => new IndexResult { IndexMeta = index.Meta, IndexValue = indexValue });
                }
                else if (operationType == typeof(WhereClause.InOperation) && isNotApplied)
                {
                    var indexValues = index.GetNotIn((IEnumerable<object>)fieldValue);
                    return indexValues.Select(indexValue => new IndexResult { IndexMeta = index.Meta, IndexValue = indexValue });
                }
                else throw new InvalidOperationException();
            }

            return null;
        }

        public IEnumerable<FieldValueCollection> GetScanResult(Type entityType, IEnumerable<object> primaryKeyValues, IDictionary<object, PrimaryKey> primaryKeys, IEnumerable<byte> fieldNumbers)
        {
            if (!_indexes.ContainsKey(entityType)) return null;
            var fieldNumbersSet = fieldNumbers.ToHashSet();
            var fieldValueDictionary = primaryKeyValues.Select(pk => new FieldValueCollection { PrimaryKey = primaryKeys[pk] }).ToDictionary(k => k.PrimaryKey.Value, v => v);
            foreach (var index in _indexes[entityType].Where(x => x.Meta.IsContainAnyFields(fieldNumbersSet)))
            {
                var converter = new IndexValueConverter(index.Meta);
                foreach (var indexValue in index.GetAllIndexValues())
                {
                    foreach (var indexItem in indexValue.Items)
                    {
                        if (!fieldValueDictionary.ContainsKey(indexItem.PrimaryKeyValue)) continue;
                        var fieldValueCollection = fieldValueDictionary[indexItem.PrimaryKeyValue];
                        fieldValueCollection.AddRange(converter.GetFieldValues(indexValue, indexItem, fieldNumbersSet));
                    }
                }
            }

            return fieldValueDictionary.Values.Where(x => x.Any());
        }
    }
}
