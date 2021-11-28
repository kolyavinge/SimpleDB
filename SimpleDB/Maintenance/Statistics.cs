﻿using System.Collections.Generic;
using System.IO;
using System.Linq;
using SimpleDB.Core;
using SimpleDB.Infrastructure;
using SimpleDB.Utils.EnumerableExtension;

namespace SimpleDB.Maintenance
{
    internal class Statistics : IStatistics
    {
        private readonly MetaFileCollection _metaFileCollection;
        private readonly string _workingDirectory;
        private readonly IPrimaryKeyFileFactory _primaryKeyFileFactory;
        private readonly IDataFileFactory _dataFileFactory;
        private readonly IFileSystem _fileSystem;

        public Statistics(
            string workingDirectory,
            IPrimaryKeyFileFactory primaryKeyFileFactory,
            IDataFileFactory dataFileFactory,
            IMetaFileFactory metaFileFactory,
            IFileSystem fileSystem)
        {
            _workingDirectory = workingDirectory;
            _primaryKeyFileFactory = primaryKeyFileFactory;
            _dataFileFactory = dataFileFactory;
            _fileSystem = fileSystem;
            _metaFileCollection = new MetaFileCollection(metaFileFactory);
        }

        public IEnumerable<FileStatistics> GetPrimaryKeyFileStatistics()
        {
            foreach (var primaryKeyFileFullPath in GetPrimaryKeyFileFullPathes())
            {
                PrimaryKeyFile primaryKeyFile = null;
                try
                {
                    var entityName = Path.GetFileNameWithoutExtension(primaryKeyFileFullPath);
                    var metaFile = _metaFileCollection.GetMetaFile(entityName);
                    primaryKeyFile = _primaryKeyFileFactory.MakeFromFileFullPath(primaryKeyFileFullPath, metaFile.GetPrimaryKeyType());
                    primaryKeyFile.BeginRead();
                    var primaryKeys = primaryKeyFile.GetAllPrimaryKeys().ToList();
                    // сумма байт удаленных ключей
                    var fragmentationSizeInBytes = primaryKeys.Where(x => x.IsDeleted).Sum(primaryKeyFile.CalculateSize);
                    yield return new FileStatistics
                    {
                        FileName = PrimaryKeyFileName.FromEntityName(entityName),
                        TotalFileSizeInBytes = primaryKeyFile.SizeInBytes,
                        FragmentationSizeInBytes = fragmentationSizeInBytes
                    };
                }
                finally
                {
                    if (primaryKeyFile != null) primaryKeyFile.EndReadWrite();
                }
            }
        }

        public IEnumerable<FileStatistics> GetDataFileStatistics()
        {
            foreach (var primaryKeyFileFullPath in GetPrimaryKeyFileFullPathes())
            {
                PrimaryKeyFile primaryKeyFile = null;
                DataFile dataFile = null;
                try
                {
                    var entityName = Path.GetFileNameWithoutExtension(primaryKeyFileFullPath);
                    var metaFile = _metaFileCollection.GetMetaFile(entityName);
                    primaryKeyFile = _primaryKeyFileFactory.MakeFromFileFullPath(primaryKeyFileFullPath, metaFile.GetPrimaryKeyType());
                    primaryKeyFile.BeginRead();
                    var primaryKeys = primaryKeyFile.GetAllPrimaryKeys().ToList();
                    var fieldMetaCollection = metaFile.GetFieldMetaCollection().ToList();
                    var fieldNumbers = fieldMetaCollection.Select(x => x.Number).ToHashSet();
                    // сумма байт удаленных записей
                    var fragmentationSizeInBytes = primaryKeys.Where(x => x.IsDeleted).Sum(primaryKey => primaryKey.EndDataFileOffset - primaryKey.StartDataFileOffset);
                    dataFile = _dataFileFactory.MakeFromFileFullPath(DataFileName.GetFullFileName(_workingDirectory, entityName), fieldMetaCollection);
                    dataFile.BeginRead();
                    long lastEndDataFileOffset = 0;
                    foreach (var primaryKey in primaryKeys.OrderBy(x => x.StartDataFileOffset))
                    {
                        // сумма байт неактуальных записей (которые были обновлены и помещены в конец файла)
                        fragmentationSizeInBytes += primaryKey.StartDataFileOffset - lastEndDataFileOffset;
                        lastEndDataFileOffset = primaryKey.EndDataFileOffset;
                        // сумма байт удаленных полей
                        fragmentationSizeInBytes += dataFile.GetUnusedFieldsSize(primaryKey.StartDataFileOffset, primaryKey.EndDataFileOffset, fieldNumbers);
                    }
                    yield return new FileStatistics
                    {
                        FileName = DataFileName.FromEntityName(entityName),
                        TotalFileSizeInBytes = dataFile.SizeInBytes,
                        FragmentationSizeInBytes = fragmentationSizeInBytes
                    };
                }
                finally
                {
                    if (primaryKeyFile != null) primaryKeyFile.EndReadWrite();
                    if (dataFile != null) dataFile.EndReadWrite();
                }
            }
        }

        private IEnumerable<string> GetPrimaryKeyFileFullPathes()
        {
            return _fileSystem
                .GetFiles(_workingDirectory).Where(file => Path.GetExtension(file) == PrimaryKeyFileName.Extension)
                .ToList();
        }
    }
}
