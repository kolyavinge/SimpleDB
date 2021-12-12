using System;
using System.Collections.Generic;
using System.Linq;
using SimpleDB.Core;
using SimpleDB.IndexedSearch;
using SimpleDB.Queries;
using SimpleDB.Utils.EnumerableExtension;

namespace SimpleDB.QueryExecutors
{
    internal class SelectQueryExecutor
    {
        private readonly DataFile _dataFile;
        private readonly IDictionary<object, PrimaryKey> _primaryKeys;
        private readonly IndexHolder _indexHolder;

        public SelectQueryExecutor(DataFile dataFile, IDictionary<object, PrimaryKey> primaryKeys, IndexHolder indexHolder)
        {
            _dataFile = dataFile;
            _primaryKeys = primaryKeys;
            _indexHolder = indexHolder;
        }

        public SelectQueryResult ExecuteQuery(SelectQuery query)
        {
            try
            {
                _dataFile.BeginRead();
                return TryExecuteQuery(query);
            }
            finally
            {
                _dataFile.EndReadWrite();
            }
        }

        private SelectQueryResult TryExecuteQuery(SelectQuery query)
        {
            var fieldValueCollections = new List<FieldValueCollection>();
            var alreadyReadedFieldNumbers = new HashSet<byte>();
            bool isResultOrdered = false;
            // where
            if (query.WhereClause != null)
            {
                var whereFieldNumbers = query.WhereClause.GetAllFieldNumbers().ToHashSet();
                if (_indexHolder.AnyIndexContainsFields(query.EntityName, whereFieldNumbers))
                {
                    var analyzer = new WhereClauseAnalyzer(query.EntityName, _primaryKeys, new FieldValueReader(_dataFile), _indexHolder);
                    fieldValueCollections.AddRange(analyzer.GetResult(query.WhereClause));
                    alreadyReadedFieldNumbers.AddRange(fieldValueCollections.SelectMany(collection => collection.Select(field => field.Number)));
                }
                else
                {
                    alreadyReadedFieldNumbers.AddRange(whereFieldNumbers);
                    foreach (var primaryKey in _primaryKeys.Values.OrderBy(x => x.StartDataFileOffset))
                    {
                        var fieldValueCollection = new FieldValueCollection { PrimaryKey = primaryKey };
                        if (whereFieldNumbers.Any())
                        {
                            _dataFile.ReadFields(primaryKey.StartDataFileOffset, primaryKey.EndDataFileOffset, whereFieldNumbers, fieldValueCollection);
                        }
                        var whereResult = query.WhereClause.GetValue(fieldValueCollection);
                        if (whereResult)
                        {
                            fieldValueCollections.Add(fieldValueCollection);
                        }
                    }
                }
            }
            else if (query.OrderByClause != null
                && query.OrderByClause.OrderedItems.OfType<OrderByClause.Field>().Any()
                && query.OrderByClause.GetAllFieldNumbers().All(fieldNumber => _indexHolder.AnyIndexFor(query.EntityName, fieldNumber)))
            {
                var analyzer = new OrderByClauseAnalyzer(query.EntityName, _primaryKeys, _indexHolder);
                fieldValueCollections.AddRange(analyzer.GetResult(query.OrderByClause));
                alreadyReadedFieldNumbers.AddRange(query.OrderByClause.GetAllFieldNumbers());
                isResultOrdered = true;
            }
            else
            {
                foreach (var primaryKey in _primaryKeys.Values.OrderBy(x => x.StartDataFileOffset))
                {
                    var fieldValueCollection = new FieldValueCollection { PrimaryKey = primaryKey };
                    fieldValueCollections.Add(fieldValueCollection);
                }
            }
            // aggregate functions
            if (query.SelectClause.SelectItems.Any(x => x is SelectClause.CountAggregate))
            {
                var count = fieldValueCollections.Count - (query.Skip ?? 0);
                if (query.Limit.HasValue)
                {
                    count = Math.Min(count, query.Limit.Value);
                }
                return new SelectQueryResult { Scalar = count };
            }
            // добираем из индексов недостающие поля
            var allFieldNumbersInQuery = query.GetAllFieldNumbers().ToHashSet();
            allFieldNumbersInQuery.ExceptWith(alreadyReadedFieldNumbers);
            var remainingFieldValues = _indexHolder.GetScanResult(query.EntityName, fieldValueCollections.Select(x => x.PrimaryKey.Value), _primaryKeys, allFieldNumbersInQuery);
            FieldValueCollection.Merge(fieldValueCollections, remainingFieldValues);
            alreadyReadedFieldNumbers.AddRange(fieldValueCollections.SelectMany(collection => collection.Select(field => field.Number)));
            // order by
            if (!isResultOrdered && query.OrderByClause != null)
            {
                var orderbyFieldNumbers = query.OrderByClause.GetAllFieldNumbers().ToHashSet();
                orderbyFieldNumbers.ExceptWith(alreadyReadedFieldNumbers);
                if (orderbyFieldNumbers.Any())
                {
                    alreadyReadedFieldNumbers.AddRange(orderbyFieldNumbers);
                    foreach (var fieldValueCollection in fieldValueCollections)
                    {
                        var primaryKey = fieldValueCollection.PrimaryKey;
                        _dataFile.ReadFields(primaryKey.StartDataFileOffset, primaryKey.EndDataFileOffset, orderbyFieldNumbers, fieldValueCollection);
                    }
                }
                fieldValueCollections.Sort(query.OrderByClause);
            }
            // skip
            if (query.Skip.HasValue)
            {
                if (query.Skip.Value < fieldValueCollections.Count)
                {
                    fieldValueCollections.RemoveRange(0, query.Skip.Value);
                }
                else
                {
                    fieldValueCollections.Clear();
                }
            }
            // limit
            if (query.Limit.HasValue && query.Limit.Value < fieldValueCollections.Count)
            {
                fieldValueCollections.RemoveRange(query.Limit.Value, fieldValueCollections.Count - query.Limit.Value);
            }
            // select
            var selectFieldNumbers = query.SelectClause.GetAllFieldNumbers().ToHashSet();
            var nonSelectedFieldNumbers = selectFieldNumbers.ToHashSet();
            nonSelectedFieldNumbers.ExceptWith(alreadyReadedFieldNumbers);
            if (nonSelectedFieldNumbers.Any())
            {
                foreach (var fieldValueCollection in fieldValueCollections)
                {
                    var primaryKey = fieldValueCollection.PrimaryKey;
                    _dataFile.ReadFields(primaryKey.StartDataFileOffset, primaryKey.EndDataFileOffset, nonSelectedFieldNumbers, fieldValueCollection);
                }
            }

            return new SelectQueryResult { FieldValueCollections = fieldValueCollections };
        }

        public List<TEntity> MakeEntities<TEntity>(SelectQuery query, SelectQueryResult result, Mapper<TEntity> mapper)
        {
            var selectFieldNumbers = query.SelectClause.GetAllFieldNumbers().ToHashSet();
            var includePrimaryKey = query.SelectClause.SelectItems.Any(x => x is SelectClause.PrimaryKey);
            var queryResultItems = new List<TEntity>();
            foreach (var fieldValueCollection in result.FieldValueCollections)
            {
                var primaryKey = fieldValueCollection.PrimaryKey;
                var entity = mapper.MakeEntity(primaryKey.Value, fieldValueCollection, includePrimaryKey, selectFieldNumbers);
                queryResultItems.Add(entity);
            }

            return queryResultItems;
        }
    }

    internal class SelectQueryResult
    {
        public List<FieldValueCollection> FieldValueCollections { get; set; }

        public object Scalar { get; set; }
    }
}
