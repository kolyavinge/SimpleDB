using System.Collections.Generic;
using System.IO;
using System.Linq;
using SimpleDB.Queries;

namespace SimpleDB.Core
{
    internal class Collection<TEntity> : ICollection<TEntity>
    {
        private readonly PrimaryKeyFile _primaryKeyFile;
        private readonly DataFile _dataFile;
        private readonly string _collectionName;
        private readonly Mapper<TEntity> _mapper;
        private readonly Dictionary<object, PrimaryKey> _primaryKeys;

        public Collection(string workingDirectory, Mapper<TEntity> mapper)
        {
            _collectionName = mapper.EntityName;
            _mapper = mapper;
            var primaryKeyFileFullPath = Path.Combine(workingDirectory, PrimaryKeyFileName.FromCollectionName(_collectionName));
            var dataFileFileFullPath = Path.Combine(workingDirectory, DataFileFileName.FromCollectionName(_collectionName));
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

        public IEnumerable<TEntity> ExecuteQuery(Query query)
        {
            var selectFieldNumbers = query.SelectClause.SelectItems.Where(x => x is SelectClause.Field).Cast<SelectClause.Field>().Select(x => x.Number).ToHashSet();
            var includePrimaryKey = query.SelectClause.SelectItems.Any(x => x is SelectClause.PrimaryKey) ? Mapper<TEntity>.IncludePrimaryKey.Yes : Mapper<TEntity>.IncludePrimaryKey.No;
            var allFieldNumbers = selectFieldNumbers.ToHashSet();
            if (query.WhereClause != null)
            {
                var whereClauseFieldNumbers = query.WhereClause.ToEnumerable().Where(x => x is WhereClause.Field).Cast<WhereClause.Field>().Select(x => x.Number).ToList();
                whereClauseFieldNumbers.ForEach(x => allFieldNumbers.Add(x));
            }
            var fieldValueCollection = new FieldValue[allFieldNumbers.Count];
            foreach (var primaryKey in _primaryKeys.Values.OrderBy(x => x.StartDataFileOffset))
            {
                _dataFile.ReadFields(primaryKey.StartDataFileOffset, primaryKey.EndDataFileOffset, allFieldNumbers, fieldValueCollection);
                if (query.WhereClause != null)
                {
                    var fieldValueDictionary = fieldValueCollection.ToDictionary(k => k.Number, v => v.Value);
                    var whereResult = query.WhereClause.GetValue(fieldValueDictionary);
                    if (whereResult)
                    {
                        var entity = _mapper.GetEntity(primaryKey.Value, fieldValueCollection, includePrimaryKey, selectFieldNumbers);
                        yield return entity;
                    }
                }
                else
                {
                    var entity = _mapper.GetEntity(primaryKey.Value, fieldValueCollection, includePrimaryKey);
                    yield return entity;
                }
            }
        }
    }
}
