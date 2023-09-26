using System.Collections.Generic;
using System.Linq;
using SimpleDB.Core;
using SimpleDB.IndexedSearch;
using SimpleDB.Queries;

namespace SimpleDB.QueryExecutors;

internal class DeleteQueryExecutor
{
    private readonly EntityMeta _entityMeta;
    private readonly PrimaryKeyFile _primaryKeyFile;
    private readonly DataFile _dataFile;
    private readonly IndexHolder _indexHolder;
    private readonly IIndexUpdater _indexUpdater;
    private readonly IDictionary<object, PrimaryKey> _primaryKeysDictionary;
    private readonly IFieldValueReader _fieldValueReader;

    public DeleteQueryExecutor(
        EntityMeta entityMeta,
        PrimaryKeyFile primaryKeyFile,
        DataFile dataFile,
        IDictionary<object, PrimaryKey> primaryKeysDictionary,
        IFieldValueReader fieldValueReader,
        IndexHolder indexHolder,
        IIndexUpdater indexUpdater)
    {
        _entityMeta = entityMeta;
        _primaryKeyFile = primaryKeyFile;
        _primaryKeysDictionary = primaryKeysDictionary;
        _fieldValueReader = fieldValueReader;
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
        if (query.WhereClause is not null)
        {
            var whereFieldNumbers = query.WhereClause.GetAllFieldNumbers().ToHashSet();
            if (_indexHolder.AnyIndexContainsFields(query.EntityName, whereFieldNumbers))
            {
                var analyzer = new WhereClauseAnalyzer(query.EntityName, _primaryKeysDictionary, _fieldValueReader, _indexHolder);
                primaryKeysForDelete.AddRange(analyzer.GetResult(query.WhereClause).Select(x => x.PrimaryKey));
            }
            else
            {
                var primaryKeys = _primaryKeysDictionary.Values;
                foreach (var primaryKey in primaryKeys.OrderBy(x => x.StartDataFileOffset))
                {
                    var fieldValueCollection = new FieldValueCollection(primaryKey);
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

        _indexUpdater.DeleteFromIndexes(_entityMeta, primaryKeysForDelete.Select(x => x.Value));

        return result;
    }

    private void Delete(PrimaryKey primaryKey)
    {
        _primaryKeyFile.Delete(primaryKey.PrimaryKeyFileOffset);
        _primaryKeysDictionary.Remove(primaryKey.Value);
    }
}
