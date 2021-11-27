using System;
using System.Collections.Generic;
using System.Linq;
using SimpleDB.Core;
using SimpleDB.Queries;
using SimpleDB.Utils.EnumerableExtension;

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

        public bool AnyIndexContainsFields(Type entityType, ISet<byte> fieldNumbers)
        {
            if (!_indexes.ContainsKey(entityType)) return false;
            return _indexes[entityType].Any(x => x.Meta.IsContainAnyFields(fieldNumbers));
        }

        public bool AnyIndexFor(Type entityType, byte indexedFieldNumber)
        {
            if (!_indexes.ContainsKey(entityType)) return false;
            return _indexes[entityType].Any(x => x.Meta.IndexedFieldNumber == indexedFieldNumber);
        }

        public IEnumerable<IndexResult> GetIndexResult(Type operationType, bool isNotApplied, Type entityType, byte fieldNumber, object fieldValue)
        {
            if (!_indexes.ContainsKey(entityType)) return null;
            IEnumerable<IndexValue> indexValues = null;
            var index = _indexes[entityType].FirstOrDefault(i => i.Meta.IndexedFieldNumber == fieldNumber);
            if (index != null)
            {
                if (operationType == typeof(WhereClause.EqualsOperation) && !isNotApplied)
                {
                    var indexValue = index.GetEquals(fieldValue);
                    if (indexValue != null) indexValues = new[] { indexValue };
                }
                else if (operationType == typeof(WhereClause.EqualsOperation) && isNotApplied)
                {
                    indexValues = index.GetNotEquals(fieldValue);
                }
                else if (operationType == typeof(WhereClause.LessOperation) && !isNotApplied)
                {
                    indexValues = index.GetLess(fieldValue);
                }
                else if (operationType == typeof(WhereClause.LessOperation) && isNotApplied)
                {
                    indexValues = index.GetGreatOrEquals(fieldValue);
                }
                else if (operationType == typeof(WhereClause.GreatOperation) && !isNotApplied)
                {
                    indexValues = index.GetGreat(fieldValue);
                }
                else if (operationType == typeof(WhereClause.GreatOperation) && isNotApplied)
                {
                    indexValues = index.GetLessOrEquals(fieldValue);
                }
                else if (operationType == typeof(WhereClause.LessOrEqualsOperation) && !isNotApplied)
                {
                    indexValues = index.GetLessOrEquals(fieldValue);
                }
                else if (operationType == typeof(WhereClause.LessOrEqualsOperation) && isNotApplied)
                {
                    indexValues = index.GetGreat(fieldValue);
                }
                else if (operationType == typeof(WhereClause.GreatOrEqualsOperation) && !isNotApplied)
                {
                    indexValues = index.GetGreatOrEquals(fieldValue);
                }
                else if (operationType == typeof(WhereClause.GreatOrEqualsOperation) && isNotApplied)
                {
                    indexValues = index.GetLess(fieldValue);
                }
                else if (operationType == typeof(WhereClause.LikeOperation) && !isNotApplied)
                {
                    indexValues = index.GetLike(fieldValue);
                }
                else if (operationType == typeof(WhereClause.LikeOperation) && isNotApplied)
                {
                    indexValues = index.GetNotLike(fieldValue);
                }
                else if (operationType == typeof(WhereClause.InOperation) && !isNotApplied)
                {
                    indexValues = index.GetIn((IEnumerable<object>)fieldValue);
                }
                else if (operationType == typeof(WhereClause.InOperation) && isNotApplied)
                {
                    indexValues = index.GetNotIn((IEnumerable<object>)fieldValue);
                }
                else throw new InvalidOperationException();
            }
            if (indexValues != null)
            {
                return indexValues.Select(indexValue => new IndexResult { IndexMeta = index.Meta, IndexValue = indexValue });
            }
            else
            {
                return null;
            }
        }

        public IEnumerable<IndexResult> GetIndexResult(Type entityType, byte indexedFieldNumber, SortDirection sortDirection)
        {
            if (!_indexes.ContainsKey(entityType)) return Enumerable.Empty<IndexResult>();
            var index = _indexes[entityType].FirstOrDefault(x => x.Meta.IndexedFieldNumber == indexedFieldNumber);
            if (index != null)
            {
                var indexValues = index.GetAllIndexValues(sortDirection);
                return indexValues.Select(indexValue => new IndexResult { IndexMeta = index.Meta, IndexValue = indexValue });
            }
            else
            {
                return Enumerable.Empty<IndexResult>();
            }
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
                        fieldValueCollection.AddRange(converter.GetFieldValues(indexValue, indexItem).Where(f => !fieldValueCollection.Contains(f.Number)));
                    }
                }
            }

            return fieldValueDictionary.Values.Where(x => x.Any());
        }
    }
}
