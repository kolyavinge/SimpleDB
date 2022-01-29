using System.Collections.Generic;
using SimpleDB.Core;
using SimpleDB.IndexedSearch;

namespace SimpleDB.QueryExecutors
{
    internal class QueryExecutorFactory
    {
        private readonly EntityMeta _entityMeta;
        private readonly PrimaryKeyFile _primaryKeyFile;
        private readonly Dictionary<object, PrimaryKey> _primaryKeys;
        private readonly DataFile _dataFile;
        private readonly IndexHolder _indexHolder;
        private readonly IIndexUpdater _indexUpdater;

        public QueryExecutorFactory(
            EntityMeta entityMeta,
            PrimaryKeyFile primaryKeyFile,
            Dictionary<object, PrimaryKey> primaryKeys,
            DataFile dataFile,
            IndexHolder indexHolder,
            IIndexUpdater indexUpdater)
        {
            _entityMeta = entityMeta;
            _primaryKeyFile = primaryKeyFile;
            _primaryKeys = primaryKeys;
            _dataFile = dataFile;
            _indexHolder = indexHolder;
            _indexUpdater = indexUpdater;
        }

        public SelectQueryExecutor MakeSelectQueryExecutor()
        {
            return new SelectQueryExecutor(_dataFile, _primaryKeys, _indexHolder);
        }

        public UpdateQueryExecutor MakeUpdateQueryExecutor()
        {
            return new UpdateQueryExecutor(_entityMeta, _primaryKeyFile, _dataFile, _primaryKeys, _indexHolder, _indexUpdater);
        }

        public DeleteQueryExecutor MakeDeleteQueryExecutor()
        {
            return new DeleteQueryExecutor(_entityMeta, _primaryKeyFile, _dataFile, _primaryKeys, _indexHolder, _indexUpdater);
        }

        public MergeQueryExecutor<TEntity> MakeMergeQueryExecutor<TEntity>(Mapper<TEntity> mapper)
        {
            return new MergeQueryExecutor<TEntity>(mapper, _primaryKeyFile, _dataFile, _primaryKeys);
        }
    }
}
