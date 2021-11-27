using System.Collections.Generic;
using SimpleDB.Core;
using SimpleDB.IndexedSearch;

namespace SimpleDB.QueryExecutors
{
    internal class QueryExecutorFactory<TEntity>
    {
        private readonly string _workingDirectory;
        private readonly Mapper<TEntity> _mapper;
        private readonly PrimaryKeyFile _primaryKeyFile;
        private readonly Dictionary<object, PrimaryKey> _primaryKeys;
        private readonly DataFile _dataFile;
        private readonly IndexHolder _indexHolder;
        private readonly IndexUpdater _indexUpdater;

        public QueryExecutorFactory(
            string workingDirectory,
            Mapper<TEntity> mapper,
            PrimaryKeyFile primaryKeyFile,
            Dictionary<object, PrimaryKey> primaryKeys,
            DataFile dataFile,
            IndexHolder indexHolder,
            IndexUpdater indexUpdater)
        {
            _workingDirectory = workingDirectory;
            _mapper = mapper;
            _primaryKeyFile = primaryKeyFile;
            _primaryKeys = primaryKeys;
            _dataFile = dataFile;
            _indexHolder = indexHolder;
            _indexUpdater = indexUpdater;
        }

        public SelectQueryExecutor<TEntity> MakeSelectQueryExecutor()
        {
            return new SelectQueryExecutor<TEntity>(_mapper, _dataFile, _primaryKeys, _indexHolder);
        }

        public UpdateQueryExecutor<TEntity> MakeUpdateQueryExecutor()
        {
            return new UpdateQueryExecutor<TEntity>(_workingDirectory, _mapper, _primaryKeyFile, _dataFile, _primaryKeys, _indexHolder, _indexUpdater);
        }

        public DeleteQueryExecutor<TEntity> MakeDeleteQueryExecutor()
        {
            return new DeleteQueryExecutor<TEntity>(_workingDirectory, _primaryKeyFile, _primaryKeys, _dataFile, _indexHolder, _indexUpdater);
        }

        public MergeQueryExecutor<TEntity> MakeMergeQueryExecutor()
        {
            return new MergeQueryExecutor<TEntity>(_mapper, _primaryKeyFile, _dataFile, _primaryKeys);
        }
    }
}
