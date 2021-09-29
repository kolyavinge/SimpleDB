using System;
using System.Collections.Generic;
using System.Linq;
using SimpleDB.Core;
using SimpleDB.Queries;
using SimpleDB.Utils.EnumerableExtension;

namespace SimpleDB.QueryExecutors
{
    internal class SelectQueryExecutor<TEntity>
    {
        private readonly Mapper<TEntity> _mapper;
        private readonly DataFile _dataFile;
        private readonly IDictionary<object, PrimaryKey> _primaryKeys;
        private readonly IndexHolder _indexHolder;

        public SelectQueryExecutor(Mapper<TEntity> mapper, DataFile dataFile, IDictionary<object, PrimaryKey> primaryKeys, IndexHolder indexHolder = null)
        {
            _mapper = mapper;
            _dataFile = dataFile;
            _primaryKeys = primaryKeys;
            _indexHolder = indexHolder ?? new IndexHolder(Enumerable.Empty<AbstractIndex>());
        }

        public SelectQueryResult<TEntity> ExecuteQuery(SelectQuery query)
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

        private SelectQueryResult<TEntity> TryExecuteQuery(SelectQuery query)
        {
            var fieldValueCollections = new List<FieldValueCollection>();
            var allFieldNumbers = new HashSet<byte>();
            // where
            if (query.WhereClause != null)
            {
                var whereFieldNumbers = query.WhereClause.GetAllFieldNumbers().ToHashSet();
                if (_indexHolder.AnyIndexFor(typeof(TEntity), whereFieldNumbers))
                {
                    var analyzer = new WhereClauseAnalyzer(typeof(TEntity), _primaryKeys, new FieldValueReader(_dataFile), _indexHolder);
                    fieldValueCollections.AddRange(analyzer.GetResult(query.WhereClause));
                    allFieldNumbers.AddRange(fieldValueCollections.SelectMany(collection => collection.Select(field => field.Number)));
                }
                else
                {
                    allFieldNumbers.AddRange(whereFieldNumbers);
                    foreach (var primaryKey in _primaryKeys.Values.OrderBy(x => x.StartDataFileOffset))
                    {
                        var fieldValueCollection = new FieldValueCollection { PrimaryKey = primaryKey };
                        _dataFile.ReadFields(primaryKey.StartDataFileOffset, primaryKey.EndDataFileOffset, whereFieldNumbers, fieldValueCollection);
                        var whereResult = query.WhereClause.GetValue(fieldValueCollection);
                        if (whereResult)
                        {
                            fieldValueCollections.Add(fieldValueCollection);
                        }
                    }
                }
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
                return new SelectQueryResult<TEntity> { Scalar = count };
            }
            // order by
            if (query.OrderByClause != null)
            {
                var orderbyFieldNumbers = query.OrderByClause.GetAllFieldNumbers().ToHashSet();
                orderbyFieldNumbers.ExceptWith(allFieldNumbers);
                if (orderbyFieldNumbers.Any())
                {
                    allFieldNumbers.AddRange(orderbyFieldNumbers);
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
            nonSelectedFieldNumbers.ExceptWith(allFieldNumbers);
            if (nonSelectedFieldNumbers.Any())
            {
                foreach (var fieldValueCollection in fieldValueCollections)
                {
                    var primaryKey = fieldValueCollection.PrimaryKey;
                    _dataFile.ReadFields(primaryKey.StartDataFileOffset, primaryKey.EndDataFileOffset, nonSelectedFieldNumbers, fieldValueCollection);
                }
            }
            // result entities
            var includePrimaryKey = query.SelectClause.SelectItems.Any(x => x is SelectClause.PrimaryKey);
            var queryResultItems = new List<TEntity>();
            foreach (var fieldValueCollection in fieldValueCollections)
            {
                var primaryKey = fieldValueCollection.PrimaryKey;
                var entity = _mapper.MakeEntity(primaryKey.Value, fieldValueCollection, includePrimaryKey, selectFieldNumbers);
                queryResultItems.Add(entity);
            }

            return new SelectQueryResult<TEntity> { Items = queryResultItems };
        }
    }

    internal class SelectQueryResult<TEntity>
    {
        public List<TEntity> Items { get; set; }

        public object Scalar { get; set; }
    }
}
