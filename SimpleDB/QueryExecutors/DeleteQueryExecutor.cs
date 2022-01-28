using System.Collections.Generic;
using System.Linq;
using SimpleDB.Core;
using SimpleDB.IndexedSearch;
using SimpleDB.Queries;
using SimpleDB.Utils.EnumerableExtension;

namespace SimpleDB.QueryExecutors
{
    internal class DeleteQueryExecutor
    {
        private readonly EntityMeta _entityMeta;
        private readonly PrimaryKeyFile _primaryKeyFile;
        private readonly DataFile _dataFile;
        private readonly IndexHolder _indexHolder;
        private readonly IndexUpdater _indexUpdater;
        private readonly IDictionary<object, PrimaryKey> _primaryKeysDictionary;

        public DeleteQueryExecutor(
            EntityMeta entityMeta,
            PrimaryKeyFile primaryKeyFile,
            DataFile dataFile,
            IDictionary<object, PrimaryKey> primaryKeysDictionary,
            IndexHolder indexHolder,
            IndexUpdater indexUpdater)
        {
            _entityMeta = entityMeta;
            _primaryKeyFile = primaryKeyFile;
            _primaryKeysDictionary = primaryKeysDictionary;
            _dataFile = dataFile;
            _indexHolder = indexHolder;
            _indexUpdater = indexUpdater;
        }

        public int ExecuteQuery(DeleteQuery query)
        {
            try
            {
                _primaryKeyFile.BeginReadWrite();
                _dataFile.BeginReadWrite();
                return TryExecuteQuery(query);
            }
            finally
            {
                _primaryKeyFile.EndReadWrite();
                _dataFile.EndReadWrite();
            }
        }

        private int TryExecuteQuery(DeleteQuery query)
        {
            var result = 0;
            var primaryKeysForDelete = new List<PrimaryKey>();
            // where
            if (query.WhereClause != null)
            {
                var whereFieldNumbers = query.WhereClause.GetAllFieldNumbers().ToHashSet();
                if (_indexHolder.AnyIndexContainsFields(query.EntityName, whereFieldNumbers))
                {
                    var analyzer = new WhereClauseAnalyzer(query.EntityName, _primaryKeysDictionary, new FieldValueReader(_dataFile), _indexHolder);
                    primaryKeysForDelete.AddRange(analyzer.GetResult(query.WhereClause).Select(x => x.PrimaryKey));
                }
                else
                {
                    var primaryKeys = _primaryKeysDictionary.Values;
                    foreach (var primaryKey in primaryKeys.OrderBy(x => x.StartDataFileOffset))
                    {
                        var fieldValueCollection = new FieldValueCollection { PrimaryKey = primaryKey };
                        _dataFile.ReadFields(primaryKey.StartDataFileOffset, primaryKey.EndDataFileOffset, whereFieldNumbers, fieldValueCollection);
                        var whereResult = query.WhereClause.GetValue(fieldValueCollection);
                        if (whereResult)
                        {
                            primaryKeysForDelete.Add(primaryKey);
                            result++;
                        }
                    }
                }
            }
            else
            {
                foreach (var primaryKey in _primaryKeysDictionary.Values)
                {
                    primaryKeysForDelete.Add(primaryKey);
                    result++;
                }
            }
            foreach (var primaryKey in primaryKeysForDelete)
            {
                Delete(primaryKey);
            }

            if (_indexUpdater != null)
            {
                _indexUpdater.DeleteFromIndexes(_entityMeta, primaryKeysForDelete.Select(x => x.Value));
            }

            return result;
        }

        private void Delete(PrimaryKey primaryKey)
        {
            _primaryKeyFile.Delete(primaryKey.PrimaryKeyFileOffset);
            _primaryKeysDictionary.Remove(primaryKey.Value);
        }
    }
}
