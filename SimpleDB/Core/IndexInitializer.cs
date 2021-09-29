﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using SimpleDB.Infrastructure;

namespace SimpleDB.Core
{
    internal class IndexInitializer<TEntity>
    {
        private readonly IFileSystem _fileSystem;
        private readonly string _workingDirectory;
        private readonly Mapper<TEntity> _mapper;

        public IndexInitializer(string workingDirectory, MapperHolder mapperHolder)
        {
            _workingDirectory = workingDirectory;
            _mapper = mapperHolder.Get<TEntity>();
            _fileSystem = IOC.Get<IFileSystem>();
        }

        public Index<TField> GetIndex<TField>(
            string indexName, Expression<Func<TEntity, TField>> indexedFieldExpression, IEnumerable<Expression<Func<TEntity, object>>> includedExpressions) where TField : IComparable<TField>
        {
            var indexFileName = Path.Combine(_workingDirectory, IndexFileName.FromEntityName(_mapper.EntityName, indexName));
            if (_fileSystem.FileExists(indexFileName))
            {
                return ReadIndexFromFile<TField>(indexFileName);
            }
            else
            {
                return MakeNewIndex(indexFileName, indexName, indexedFieldExpression, includedExpressions);
            }
        }

        private Index<TField> ReadIndexFromFile<TField>(string indexFileName) where TField : IComparable<TField>
        {
            var indexFile = new IndexFile(indexFileName, _mapper.PrimaryKeyMapping.PropertyType, _mapper.FieldMetaCollection);
            return indexFile.ReadIndex<TField>();
        }

        private Index<TField> MakeNewIndex<TField>(
            string indexFileName, string indexName, Expression<Func<TEntity, TField>> indexedFieldExpression, IEnumerable<Expression<Func<TEntity, object>>> includedExpressions) where TField : IComparable<TField>
        {
            var indexedFieldName = FieldMapping<TEntity>.GetPropertyName(indexedFieldExpression);
            var includedFieldNames = (includedExpressions ?? Enumerable.Empty<Expression<Func<TEntity, object>>>()).Select(FieldMapping<TEntity>.GetPropertyName).ToHashSet();
            var indexedFieldNumber = _mapper.FieldMappings.First(fm => fm.PropertyName == indexedFieldName).Number;
            var includedFieldNumbers = _mapper.FieldMappings.Where(fm => includedFieldNames.Contains(fm.PropertyName)).Select(x => x.Number).ToArray();
            var meta = new IndexMeta { EntityType = typeof(TEntity), Name = indexName, IndexedFieldNumber = indexedFieldNumber, IncludedFieldNumbers = includedFieldNumbers };
            var index = new Index<TField>(meta);
            PopulateIndex(index, indexedFieldNumber, includedFieldNumbers);
            var indexFile = new IndexFile(indexFileName, _mapper.PrimaryKeyMapping.PropertyType, _mapper.FieldMetaCollection);
            indexFile.WriteIndex(index);

            return index;
        }

        private void PopulateIndex<TField>(Index<TField> index, byte indexedFieldNumber, IEnumerable<byte> includedFieldNumbers) where TField : IComparable<TField>
        {
            PrimaryKeyFile primaryKeyFile = null;
            DataFile dataFile = null;
            try
            {
                var primaryKeyFileName = Path.Combine(_workingDirectory, PrimaryKeyFileName.FromEntityName(_mapper.EntityName));
                var dataFileName = Path.Combine(_workingDirectory, DataFileName.FromEntityName(_mapper.EntityName));
                primaryKeyFile = new PrimaryKeyFile(primaryKeyFileName, _mapper.PrimaryKeyMapping.PropertyType);
                dataFile = new DataFile(dataFileName, _mapper.FieldMetaCollection);
                primaryKeyFile.BeginRead();
                dataFile.BeginRead();
                var fieldNumbers = new HashSet<byte>(includedFieldNumbers);
                fieldNumbers.Add(indexedFieldNumber);
                var fieldValueCollection = new FieldValueCollection();
                foreach (var primaryKey in primaryKeyFile.GetAllPrimaryKeys().Where(x => !x.IsDeleted).OrderBy(x => x.StartDataFileOffset))
                {
                    dataFile.ReadFields(primaryKey.StartDataFileOffset, primaryKey.EndDataFileOffset, fieldNumbers, fieldValueCollection);
                    var indexedFieldValue = (TField)fieldValueCollection[indexedFieldNumber].Value;
                    var includedFieldValues = includedFieldNumbers.Select(fn => fieldValueCollection[fn].Value).ToArray();
                    var indexValue = index.Get(indexedFieldValue);
                    if (indexValue == null)
                    {
                        var indexItem = new IndexItem { PrimaryKeyValue = primaryKey.Value, IncludedFields = includedFieldValues };
                        indexValue = new IndexValue { IndexedFieldValue = indexedFieldValue, Items = new List<IndexItem> { indexItem } };
                        index.Insert(indexValue);
                    }
                    else
                    {
                        indexValue.Items.Add(new IndexItem { PrimaryKeyValue = primaryKey.Value, IncludedFields = includedFieldValues });
                    }
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