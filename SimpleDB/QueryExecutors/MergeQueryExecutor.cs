using System.Collections.Generic;
using System.Linq;
using SimpleDB.Core;
using SimpleDB.IndexedSearch;
using SimpleDB.Queries;
using SimpleDB.Utils.EnumerableExtension;

namespace SimpleDB.QueryExecutors
{
    internal class MergeQueryExecutor<TEntity>
    {
        private readonly Mapper<TEntity> _mapper;
        private readonly PrimaryKeyFile _primaryKeyFile;
        private readonly DataFile _dataFile;
        private readonly IDictionary<object, PrimaryKey> _primaryKeys;
        private readonly IIndexUpdater _indexUpdater;

        public MergeQueryExecutor(
            Mapper<TEntity> mapper,
            PrimaryKeyFile primaryKeyFile,
            DataFile dataFile,
            IDictionary<object, PrimaryKey> primaryKeys,
            IIndexUpdater indexUpdater)
        {
            _mapper = mapper;
            _primaryKeyFile = primaryKeyFile;
            _dataFile = dataFile;
            _primaryKeys = primaryKeys;
            _indexUpdater = indexUpdater;
        }

        public MergeQueryResult<TEntity> ExecuteQuery(MergeQuery<TEntity> query)
        {
            List<TEntity> newEntities = null;
            try
            {
                _dataFile.BeginRead();
                newEntities = GetNewEntities(query);
            }
            finally
            {
                _dataFile.EndReadWrite();
            }
            try
            {
                _primaryKeyFile.BeginReadWrite();
                _dataFile.BeginReadWrite();
                if (newEntities.Any())
                {
                    EntityOperations.Insert(newEntities, _mapper, _primaryKeyFile, _dataFile, _primaryKeys);
                    _indexUpdater.AddToIndexes(_mapper, newEntities);
                }
            }
            finally
            {
                _primaryKeyFile.EndReadWrite();
                _dataFile.EndReadWrite();
            }

            return new MergeQueryResult<TEntity> { NewItems = newEntities };
        }

        private List<TEntity> GetNewEntities(MergeQuery<TEntity> query)
        {
            var mergeFieldNumbers = query.MergeClause.MergeItems.Select(x => x.FieldNumber).ToHashSet();
            var newEntities = query.Entities.ToDictionary(k => new FieldValueCollection(_mapper.GetFieldValueCollection(k, mergeFieldNumbers)), v => v);
            var dataFileFieldValueCollection = new FieldValueCollection();
            foreach (var primaryKey in _primaryKeys.Values.OrderBy(x => x.StartDataFileOffset))
            {
                dataFileFieldValueCollection.Clear();
                _dataFile.ReadFields(primaryKey.StartDataFileOffset, primaryKey.EndDataFileOffset, mergeFieldNumbers, dataFileFieldValueCollection);
                if (newEntities.ContainsKey(dataFileFieldValueCollection))
                {
                    newEntities.Remove(dataFileFieldValueCollection);
                    if (!newEntities.Any()) new List<TEntity>();
                }
            }

            return newEntities.Values.ToList();
        }
    }

    internal class MergeQueryResult<TEntity> : IMergeQueryResult<TEntity>
    {
        public List<TEntity> NewItems { get; set; }
    }
}
