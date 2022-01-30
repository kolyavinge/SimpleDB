using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using SimpleDB.Core;
using SimpleDB.Utils.EnumerableExtension;

namespace SimpleDB.IndexedSearch
{
    internal class IndexInitializer<TEntity>
    {
        private readonly Mapper<TEntity> _mapper;
        private readonly IPrimaryKeyFileFactory _primaryKeyFileFactory;
        private readonly IDataFileFactory _dataFileFactory;
        private readonly IIndexFileFactory _indexFileFactory;

        public IndexInitializer(
            MapperHolder mapperHolder,
            IPrimaryKeyFileFactory primaryKeyFileFactory,
            IDataFileFactory dataFileFactory,
            IIndexFileFactory indexFileFactory)
        {
            _mapper = mapperHolder.Get<TEntity>();
            _primaryKeyFileFactory = primaryKeyFileFactory;
            _dataFileFactory = dataFileFactory;
            _indexFileFactory = indexFileFactory;
        }

        public Index<TField> GetIndex<TField>(
            string indexName, Expression<Func<TEntity, TField>> indexedFieldExpression, IEnumerable<Expression<Func<TEntity, object>>> includedExpressions) where TField : IComparable<TField>
        {
            var indexFile = _indexFileFactory.Make(_mapper.EntityName, indexName, _mapper.PrimaryKeyMapping.PropertyType, _mapper.FieldMetaCollection);
            if (indexFile.IsExist())
            {
                return indexFile.ReadIndex<TField>();
            }
            else
            {
                return MakeNewIndex(indexFile, indexName, indexedFieldExpression, includedExpressions);
            }
        }

        private Index<TField> MakeNewIndex<TField>(
            IndexFile indexFile, string indexName, Expression<Func<TEntity, TField>> indexedFieldExpression, IEnumerable<Expression<Func<TEntity, object>>> includedExpressions) where TField : IComparable<TField>
        {
            var indexedFieldName = FieldMapping<TEntity>.GetPropertyName(indexedFieldExpression);
            var includedFieldNames = (includedExpressions ?? Enumerable.Empty<Expression<Func<TEntity, object>>>()).Select(FieldMapping<TEntity>.GetPropertyName).ToHashSet();
            var fieldMetaCollection = _mapper.EntityMeta.GetPrimaryKeyAndFieldMetaCollection();
            var indexedFieldNumber = fieldMetaCollection.First(fm => fm.Name == indexedFieldName).Number;
            var includedFieldNumbers = fieldMetaCollection.Where(fm => includedFieldNames.Contains(fm.Name)).Select(x => x.Number).ToArray();
            if (includedFieldNumbers.Contains(PrimaryKey.FieldNumber)) throw new ArgumentException("Primary key cannot be included in any index");
            var meta = new IndexMeta { EntityName = _mapper.EntityName, IndexedFieldType = typeof(TField), Name = indexName, IndexedFieldNumber = indexedFieldNumber, IncludedFieldNumbers = includedFieldNumbers };
            var index = new Index<TField>(meta);
            PopulateIndex(index, indexedFieldNumber, includedFieldNumbers);
            indexFile.Create();
            indexFile.WriteIndex(index);

            return index;
        }

        private void PopulateIndex(IIndex index, byte indexedFieldNumber, IEnumerable<byte> includedFieldNumbers)
        {
            PrimaryKeyFile primaryKeyFile = null;
            DataFile dataFile = null;
            try
            {
                primaryKeyFile = _primaryKeyFileFactory.MakeFromEntityName(_mapper.EntityName, _mapper.PrimaryKeyMapping.PropertyType);
                dataFile = _dataFileFactory.MakeFromEntityName(_mapper.EntityName, _mapper.FieldMetaCollection);
                primaryKeyFile.BeginRead();
                dataFile.BeginRead();
                var fieldNumbers = new HashSet<byte>(includedFieldNumbers) { indexedFieldNumber };
                var fieldValueCollection = new FieldValueCollection();
                foreach (var primaryKey in primaryKeyFile.GetAllPrimaryKeys().Where(x => !x.IsDeleted).OrderBy(x => x.StartDataFileOffset))
                {
                    dataFile.ReadFields(primaryKey.StartDataFileOffset, primaryKey.EndDataFileOffset, fieldNumbers, fieldValueCollection);
                    var indexedFieldValue = indexedFieldNumber == PrimaryKey.FieldNumber ? primaryKey.Value : fieldValueCollection[indexedFieldNumber].Value;
                    var includedFieldValues = includedFieldNumbers.Select(fn => fieldValueCollection[fn].Value).ToArray();
                    var indexItem = new IndexItem { PrimaryKeyValue = primaryKey.Value, IncludedFields = includedFieldValues };
                    index.Add(indexedFieldValue, indexItem);
                    fieldValueCollection.Clear();
                }
            }
            finally
            {
                if (primaryKeyFile != null) primaryKeyFile.EndReadWrite();
                if (dataFile != null) dataFile.EndReadWrite();
            }
        }
    }
}
