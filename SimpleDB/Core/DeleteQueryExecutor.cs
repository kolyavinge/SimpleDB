using System.Collections.Generic;
using System.Linq;
using SimpleDB.Queries;

namespace SimpleDB.Core
{
    internal class DeleteQueryExecutor<TEntity>
    {
        private readonly PrimaryKeyFile _primaryKeyFile;
        private readonly DataFile _dataFile;
        private readonly Dictionary<object, PrimaryKey> _primaryKeysDictionary;

        public DeleteQueryExecutor(PrimaryKeyFile primaryKeyFile, Dictionary<object, PrimaryKey> primaryKeysDictionary, DataFile dataFile)
        {
            _primaryKeyFile = primaryKeyFile;
            _primaryKeysDictionary = primaryKeysDictionary;
            _dataFile = dataFile;
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
            // where
            if (query.WhereClause != null)
            {
                var whereFieldNumbers = query.WhereClause.GetAllFieldNumbers().ToHashSet();
                var primaryKeys = _primaryKeysDictionary.Values;
                foreach (var primaryKey in primaryKeys.OrderBy(x => x.StartDataFileOffset))
                {
                    var fieldValueCollection = new FieldValueCollection { PrimaryKey = primaryKey };
                    _dataFile.ReadFields(primaryKey.StartDataFileOffset, primaryKey.EndDataFileOffset, whereFieldNumbers, fieldValueCollection);
                    var whereResult = query.WhereClause.GetValue(fieldValueCollection);
                    if (whereResult)
                    {
                        Delete(primaryKey);
                        result++;
                    }
                }
            }
            else
            {
                foreach (var primaryKey in _primaryKeysDictionary.Values)
                {
                    Delete(primaryKey);
                    result++;
                }
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
