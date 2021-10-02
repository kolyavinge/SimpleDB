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
            foreach (var item in IndexValue.Items)
            {
                var fieldValueCollection = new FieldValueCollection { PrimaryKey = primaryKeys[item.PrimaryKeyValue] };
                fieldValueCollection.Add(IndexMeta.IndexedFieldNumber, new FieldValue(IndexMeta.IndexedFieldNumber, IndexValue.IndexedFieldValue));
                var includedFieldNumbers = IndexMeta.IncludedFieldNumbers ?? new byte[0];
                for (int includedFieldNumberIndex = 0; includedFieldNumberIndex < includedFieldNumbers.Length; includedFieldNumberIndex++)
                {
                    var number = includedFieldNumbers[includedFieldNumberIndex];
                    var value = item.IncludedFields[includedFieldNumberIndex];
                    fieldValueCollection.Add(number, new FieldValue(number, value));
                }
                yield return fieldValueCollection;
            }
        }
    }

    internal class IndexHolder
    {
        private readonly Dictionary<Type, List<AbstractIndex>> _indexes;

        public IndexHolder(IEnumerable<AbstractIndex> indexes)
        {
            _indexes = indexes.GroupBy(x => x.Meta.EntityType).ToDictionary(k => k.Key, v => v.ToList());
        }

        public IndexHolder()
        {
            _indexes = new Dictionary<Type, List<AbstractIndex>>();
        }

        public bool AnyIndexFor(Type entityType, ISet<byte> fieldNumbers)
        {
            if (!_indexes.ContainsKey(entityType)) return false;
            return _indexes[entityType].Any(x => fieldNumbers.Contains(x.Meta.IndexedFieldNumber));
        }

        public IEnumerable<IndexResult> GetIndexResults(Type operationType, bool isNotApplied, Type entityType, byte fieldNumber, object fieldValue)
        {
            if (!_indexes.ContainsKey(entityType)) return null;
            foreach (var index in _indexes[entityType].Where(x => x.Meta.IndexedFieldNumber == fieldNumber))
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
    }
}
