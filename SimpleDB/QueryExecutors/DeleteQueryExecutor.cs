using System.Collections.Generic;
using System.Linq;
using SimpleDB.Core;
using SimpleDB.IndexedSearch;
using SimpleDB.Queries;
using SimpleDB.Utils.EnumerableExtension;

namespace SimpleDB.QueryExecutors
{
    internal class DeleteQueryExecutor<TEntity>
    {
        private readonly PrimaryKeyFile _primaryKeyFile;
        private readonly DataFile _dataFile;
        private readonly IndexHolder _indexHolder;
        private readonly IndexUpdater _indexUpdater;
        private readonly Dictionary<object, PrimaryKey> _primaryKeysDictionary;

        public DeleteQueryExecutor(
            PrimaryKeyFile primaryKeyFile, Dictionary<object, PrimaryKey> primaryKeysDictionary, DataFile dataFile, IndexHolder indexHolder, IndexUpdater indexUpdater)
        {
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
                _primaryKeyFile.BeginWrite();
                _dataFile.BeginRead();
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
                if (_indexHolder.AnyIndexContainsFields(typeof(TEntity), whereFieldNumbers))
                {
                    var analyzer = new WhereClauseAnalyzer(typeof(TEntity), _primaryKeysDictionary, new FieldValueReader(_dataFile), _indexHolder);
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
            _indexUpdater.DeleteFromIndexes<TEntity>(primaryKeysForDelete);

            return result;
        }

        private void Delete(PrimaryKey primaryKey)
        {
            _primaryKeyFile.Delete(primaryKey.PrimaryKeyFileOffset);
            _primaryKeysDictionary.Remove(primaryKey.Value);
        }
    }
}
