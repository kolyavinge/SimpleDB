﻿using System.Collections.Generic;
using System.Linq;
using SimpleDB.Queries;

namespace SimpleDB.Core
{
    internal class MergeQueryExecutor<TEntity>
    {
        private readonly Mapper<TEntity> _mapper;
        private readonly PrimaryKeyFile _primaryKeyFile;
        private readonly DataFile _dataFile;
        private readonly IDictionary<object, PrimaryKey> _primaryKeys;

        public MergeQueryExecutor(Mapper<TEntity> mapper, PrimaryKeyFile primaryKeyFile, DataFile dataFile, IDictionary<object, PrimaryKey> primaryKeys)
        {
            _mapper = mapper;
            _primaryKeyFile = primaryKeyFile;
            _dataFile = dataFile;
            _primaryKeys = primaryKeys;
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
                _primaryKeyFile.BeginWrite();
                _dataFile.BeginWrite();
                if (newEntities.Any())
                {
                    foreach (var entity in newEntities)
                    {
                        InsertInternal(entity);
                    }
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

        private void InsertInternal(TEntity entity)
        {
            var fieldValueCollection = _mapper.GetFieldValueCollection(entity);
            var insertResult = _dataFile.Insert(fieldValueCollection);
            var primaryKeyValue = _mapper.GetPrimaryKeyValue(entity);
            var primaryKey = _primaryKeyFile.Insert(primaryKeyValue, insertResult.StartDataFileOffset, insertResult.EndDataFileOffset);
            _primaryKeys.Add(primaryKeyValue, primaryKey);
        }
    }

    internal class MergeQueryResult<TEntity> : IMergeQueryResult<TEntity>
    {
        public List<TEntity> NewItems { get; set; }
    }
}
