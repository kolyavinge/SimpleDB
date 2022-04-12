using System;
using System.Collections.Generic;
using System.Linq;
using SimpleDB.Core;
using SimpleDB.IndexedSearch;
using SimpleDB.Queries;

namespace SimpleDB.QueryExecutors
{
    internal class OrderByClauseAnalyzer
    {
        private readonly string _entityName;
        private readonly IDictionary<object, PrimaryKey> _primaryKeys;
        private readonly IndexHolder _indexHolder;

        public OrderByClauseAnalyzer(string entityName, IDictionary<object, PrimaryKey> primaryKeys, IndexHolder indexHolder)
        {
            _entityName = entityName;
            _primaryKeys = primaryKeys;
            _indexHolder = indexHolder;
        }

        public IEnumerable<FieldValueCollection> GetResult(OrderByClause orderByClause)
        {
            var fieldNumbers = orderByClause.GetAllFieldNumbers().ToList();
            if (fieldNumbers.Count == 1)
            {
                var indexValues = _indexHolder.GetIndexResult(_entityName, fieldNumbers.First(), orderByClause.OrderedItems.First().Direction);
                return indexValues.SelectMany(x => x.ToFieldValueCollections(_primaryKeys));
            }
            else
            {
                var indexItemDictionaries = GetIndexItemDictionaries(orderByClause).ToList();
                var resultComparer = new OrderByFieldValueCollectionComparer(indexItemDictionaries);
                var firstField = orderByClause.OrderedItems.OfType<OrderByClause.Field>().First();
                var firstFieldIndexResults = _indexHolder.GetIndexResult(_entityName, firstField.Number, firstField.Direction).ToList();
                var result = new FieldValueCollection[firstFieldIndexResults.Sum(x => x.IndexValue.Items.Count)];
                int resultPosition = 0;
                foreach (var firstFieldIndexResult in firstFieldIndexResults)
                {
                    if (firstFieldIndexResult.IndexValue.Items.Count == 1)
                    {
                        var item = firstFieldIndexResult.IndexValue.Items.First();
                        var fieldValueCollection = new FieldValueCollection { PrimaryKey = _primaryKeys[item.PrimaryKeyValue] };
                        fieldValueCollection.Add(new FieldValue(firstFieldIndexResult.IndexMeta.IndexedFieldNumber, firstFieldIndexResult.IndexValue.IndexedFieldValue));
                        result[resultPosition++] = fieldValueCollection;
                    }
                    else
                    {
                        var resultPositionStart = resultPosition;
                        foreach (var item in firstFieldIndexResult.IndexValue.Items)
                        {
                            var fieldValueCollection = new FieldValueCollection { PrimaryKey = _primaryKeys[item.PrimaryKeyValue] };
                            fieldValueCollection.Add(new FieldValue(firstFieldIndexResult.IndexMeta.IndexedFieldNumber, firstFieldIndexResult.IndexValue.IndexedFieldValue));
                            result[resultPosition++] = fieldValueCollection;
                        }
                        Array.Sort(result, resultPositionStart, resultPosition - resultPositionStart, resultComparer);
                    }
                }

                return result;
            }
        }

        private IEnumerable<IndexItemDictionary> GetIndexItemDictionaries(OrderByClause orderByClause)
        {
            foreach (var orderedField in orderByClause.OrderedItems.OfType<OrderByClause.Field>().Skip(1))
            {
                var indexResults = _indexHolder.GetIndexResult(_entityName, orderedField.Number, orderedField.Direction).ToArray();
                var indexItemDictionary = new IndexItemDictionary();
                for (int position = 0; position < indexResults.Length; position++)
                {
                    foreach (var indexItem in indexResults[position].IndexValue.Items)
                    {
                        indexItemDictionary.Add(indexItem.PrimaryKeyValue, position);
                    }
                }
                yield return indexItemDictionary;
            }
        }

        class IndexItemDictionary : Dictionary<object, int> { }

        class OrderByFieldValueCollectionComparer : IComparer<FieldValueCollection>
        {
            private readonly List<IndexItemDictionary> _indexItemDictionaries;

            public OrderByFieldValueCollectionComparer(List<IndexItemDictionary> indexItemDictionaries)
            {
                _indexItemDictionaries = indexItemDictionaries;
            }

            public int Compare(FieldValueCollection x, FieldValueCollection y)
            {
                foreach (var indexItemDictionary in _indexItemDictionaries)
                {
                    var xPosition = indexItemDictionary[x.PrimaryKey!.Value];
                    var yPosition = indexItemDictionary[y.PrimaryKey!.Value];
                    var compareResult = xPosition.CompareTo(yPosition);
                    if (compareResult == 0) continue;
                    return compareResult;
                }

                return 0;
            }
        }
    }
}
