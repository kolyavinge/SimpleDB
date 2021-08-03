using System.Collections.Generic;
using System.Linq;
using SimpleDB.Infrastructure;
using SimpleDB.Queries;

namespace SimpleDB.Core
{
    internal class QueryExecutor<TEntity>
    {
        private readonly Mapper<TEntity> _mapper;
        private readonly DataFile _dataFile;
        private readonly IEnumerable<PrimaryKey> _primaryKeys;

        public QueryExecutor(Mapper<TEntity> mapper, DataFile dataFile, IEnumerable<PrimaryKey> primaryKeys)
        {
            _mapper = mapper;
            _dataFile = dataFile;
            _primaryKeys = primaryKeys;
        }

        public List<TEntity> ExecuteQuery(Query query)
        {
            var fieldValueDictionaries = new List<FieldValueDictionary>();
            var allFieldNumbers = new HashSet<byte>();
            // where
            if (query.WhereClause != null)
            {
                var whereFieldNumbers = query.WhereClause.GetAllFieldNumbers().ToHashSet();
                allFieldNumbers.AddRange(whereFieldNumbers);
                foreach (var primaryKey in _primaryKeys.OrderBy(x => x.StartDataFileOffset))
                {
                    var fieldValueDictionary = new FieldValueDictionary { PrimaryKey = primaryKey };
                    _dataFile.ReadFields(primaryKey.StartDataFileOffset, primaryKey.EndDataFileOffset, whereFieldNumbers, fieldValueDictionary.FieldValues);
                    var whereResult = query.WhereClause.GetValue(fieldValueDictionary);
                    if (whereResult)
                    {
                        fieldValueDictionaries.Add(fieldValueDictionary);
                    }
                }
            }
            else
            {
                foreach (var primaryKey in _primaryKeys.OrderBy(x => x.StartDataFileOffset))
                {
                    var fieldValueDictionary = new FieldValueDictionary { PrimaryKey = primaryKey };
                    fieldValueDictionaries.Add(fieldValueDictionary);
                }
            }
            // order by
            if (query.OrderByClause != null)
            {
                var orderbyFieldNumbers = query.OrderByClause.GetAllFieldNumbers().ToHashSet();
                orderbyFieldNumbers.ExceptWith(allFieldNumbers);
                if (orderbyFieldNumbers.Any())
                {
                    allFieldNumbers.AddRange(orderbyFieldNumbers);
                    foreach (var fieldValueDictionary in fieldValueDictionaries)
                    {
                        var primaryKey = fieldValueDictionary.PrimaryKey;
                        _dataFile.ReadFields(primaryKey.StartDataFileOffset, primaryKey.EndDataFileOffset, orderbyFieldNumbers, fieldValueDictionary.FieldValues);
                    }
                }
                fieldValueDictionaries.Sort(query.OrderByClause);
            }
            // skip
            if (query.Skip.HasValue)
            {
                if (query.Skip.Value < fieldValueDictionaries.Count)
                {
                    fieldValueDictionaries.RemoveRange(0, query.Skip.Value);
                }
                else
                {
                    fieldValueDictionaries.Clear();
                }
            }
            // limit
            if (query.Limit.HasValue && query.Limit.Value < fieldValueDictionaries.Count)
            {
                fieldValueDictionaries.RemoveRange(query.Limit.Value, fieldValueDictionaries.Count - query.Limit.Value);
            }
            // select
            var selectFieldNumbers = query.SelectClause.GetAllFieldNumbers().ToHashSet();
            var nonSelectedFieldNumbers = selectFieldNumbers.ToHashSet();
            nonSelectedFieldNumbers.ExceptWith(allFieldNumbers);
            if (nonSelectedFieldNumbers.Any())
            {
                foreach (var fieldValueDictionary in fieldValueDictionaries)
                {
                    var primaryKey = fieldValueDictionary.PrimaryKey;
                    _dataFile.ReadFields(primaryKey.StartDataFileOffset, primaryKey.EndDataFileOffset, nonSelectedFieldNumbers, fieldValueDictionary.FieldValues);
                }
            }
            // result entities
            var includePrimaryKey = query.SelectClause.SelectItems.Any(x => x is SelectClause.PrimaryKey);
            var result = new List<TEntity>();
            foreach (var fieldValueDictionary in fieldValueDictionaries)
            {
                var primaryKey = fieldValueDictionary.PrimaryKey;
                var entity = _mapper.GetEntity(primaryKey.Value, fieldValueDictionary.FieldValues.Values, includePrimaryKey, selectFieldNumbers);
                result.Add(entity);
            }

            return result;
        }
    }
}
