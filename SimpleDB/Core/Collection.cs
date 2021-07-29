using System.Collections.Generic;
using System.IO;
using System.Linq;
using SimpleDB.Infrastructure;
using SimpleDB.Queries;

namespace SimpleDB.Core
{
    internal class Collection<TEntity> : ICollection<TEntity>
    {
        private readonly PrimaryKeyFile _primaryKeyFile;
        private readonly DataFile _dataFile;
        private readonly Mapper<TEntity> _mapper;
        private readonly Dictionary<object, PrimaryKey> _primaryKeys;

        public Collection(string workingDirectory, Mapper<TEntity> mapper)
        {
            _mapper = mapper;
            var primaryKeyFileFullPath = Path.Combine(workingDirectory, PrimaryKeyFileName.FromCollectionName(mapper.EntityName));
            var dataFileFileFullPath = Path.Combine(workingDirectory, DataFileFileName.FromCollectionName(mapper.EntityName));
            _primaryKeyFile = new PrimaryKeyFile(primaryKeyFileFullPath, mapper.PrimaryKeyType);
            _dataFile = new DataFile(dataFileFileFullPath, mapper.FieldMetaCollection);
            _primaryKeys = _primaryKeyFile.GetAllPrimaryKeys().Where(x => !x.IsDeleted()).ToDictionary(k => k.Value, v => v);
        }

        public TEntity Get(object id)
        {
            if (_primaryKeys.ContainsKey(id))
            {
                var primaryKey = _primaryKeys[id];
                var fieldValueCollection = new FieldValue[_mapper.FieldMetaCollection.Count];
                _dataFile.ReadFields(primaryKey.StartDataFileOffset, primaryKey.EndDataFileOffset, fieldValueCollection);
                var entity = _mapper.GetEntity(primaryKey.Value, fieldValueCollection, Mapper<TEntity>.IncludePrimaryKey.Yes);

                return entity;
            }
            else
            {
                return default(TEntity);
            }
        }

        public void Insert(TEntity entity)
        {
            var fieldValueCollection = _mapper.GetFieldValueCollection(entity);
            var insertResult = _dataFile.Insert(fieldValueCollection);
            var primaryKeyValue = _mapper.GetPrimaryKeyValue(entity);
            var primaryKey = _primaryKeyFile.Insert(primaryKeyValue, insertResult.StartDataFileOffset, insertResult.EndDataFileOffset);
            _primaryKeys.Add(primaryKeyValue, primaryKey);
        }

        public void Update(TEntity entity)
        {
            var primaryKeyValue = _mapper.GetPrimaryKeyValue(entity);
            var primaryKey = _primaryKeys[primaryKeyValue];
            var fieldValueCollection = _mapper.GetFieldValueCollection(entity);
            var updateResult = _dataFile.Update(primaryKey.StartDataFileOffset, primaryKey.EndDataFileOffset, fieldValueCollection);
            if (primaryKey.StartDataFileOffset != updateResult.NewStartDataFileOffset)
            {
                _primaryKeyFile.UpdateStartEndDataFileOffset(primaryKey.StartDataFileOffset, updateResult.NewStartDataFileOffset, updateResult.NewEndDataFileOffset);
                primaryKey.StartDataFileOffset = updateResult.NewStartDataFileOffset;
                primaryKey.EndDataFileOffset = updateResult.NewEndDataFileOffset;
            }
            else if (primaryKey.EndDataFileOffset != updateResult.NewEndDataFileOffset)
            {
                _primaryKeyFile.UpdateEndDataFileOffset(updateResult.NewStartDataFileOffset, updateResult.NewEndDataFileOffset);
                primaryKey.EndDataFileOffset = updateResult.NewEndDataFileOffset;
            }
        }

        public void Delete(object id)
        {
            var primaryKey = _primaryKeys[id];
            _primaryKeyFile.Delete(primaryKey.PrimaryKeyFileOffset);
            _primaryKeys.Remove(id);
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
                foreach (var primaryKey in _primaryKeys.Values.OrderBy(x => x.StartDataFileOffset))
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
                foreach (var primaryKey in _primaryKeys.Values.OrderBy(x => x.StartDataFileOffset))
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
            if (query.Skip.HasValue && query.Skip.Value < fieldValueDictionaries.Count)
            {
                fieldValueDictionaries.RemoveRange(0, query.Skip.Value);
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
            var includePrimaryKey = query.SelectClause.SelectItems.Any(x => x is SelectClause.PrimaryKey) ? Mapper<TEntity>.IncludePrimaryKey.Yes : Mapper<TEntity>.IncludePrimaryKey.No;
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
