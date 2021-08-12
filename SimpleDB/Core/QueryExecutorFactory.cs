using System.Collections.Generic;

namespace SimpleDB.Core
{
    internal class QueryExecutorFactory<TEntity>
    {
        private readonly Mapper<TEntity> _mapper;
        private readonly PrimaryKeyFile _primaryKeyFile;
        private readonly Dictionary<object, PrimaryKey> _primaryKeys;
        private readonly DataFile _dataFile;

        public QueryExecutorFactory(
            Mapper<TEntity> mapper,
            PrimaryKeyFile primaryKeyFile,
            Dictionary<object, PrimaryKey> primaryKeys,
            DataFile dataFile)
        {
            _mapper = mapper;
            _primaryKeyFile = primaryKeyFile;
            _primaryKeys = primaryKeys;
            _dataFile = dataFile;
        }

        public SelectQueryExecutor<TEntity> MakeSelectQueryExecutor()
        {
            return new SelectQueryExecutor<TEntity>(_mapper, _dataFile, _primaryKeys.Values);
        }

        public UpdateQueryExecutor<TEntity> MakeUpdateQueryExecutor()
        {
            return new UpdateQueryExecutor<TEntity>(_mapper, _primaryKeyFile, _dataFile, _primaryKeys.Values);
        }

        public DeleteQueryExecutor<TEntity> MakeDeleteQueryExecutor()
        {
            return new DeleteQueryExecutor<TEntity>(_primaryKeyFile, _primaryKeys, _dataFile);
        }
    }
}
