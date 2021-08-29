using System.Collections.Generic;
using System.IO;
using System.Linq;
using SimpleDB.Core;
using SimpleDB.Infrastructure;

namespace SimpleDB.Maintenance
{
    internal class Statistics : IStatistics
    {
        private readonly string _workingDirectory;
        private readonly MetaFileCollection _metaFileCollection;

        public Statistics(string workingDirectory)
        {
            _workingDirectory = workingDirectory;
            _metaFileCollection = new MetaFileCollection(_workingDirectory);
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
                    primaryKeyFile = new PrimaryKeyFile(primaryKeyFileFullPath, metaFile.GetPrimaryKeyType());
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
                    primaryKeyFile = new PrimaryKeyFile(primaryKeyFileFullPath, metaFile.GetPrimaryKeyType());
                    primaryKeyFile.BeginRead();
                    var primaryKeys = primaryKeyFile.GetAllPrimaryKeys().ToList();
                    var fieldMetaCollection = metaFile.GetFieldMetaCollection().ToList();
                    var fieldNumbers = fieldMetaCollection.Select(x => x.Number).ToHashSet();
                    // сумма байт удаленных записей
                    var fragmentationSizeInBytes = primaryKeys.Where(x => x.IsDeleted).Sum(primaryKey => primaryKey.EndDataFileOffset - primaryKey.StartDataFileOffset);
                    dataFile = new DataFile(Path.Combine(_workingDirectory, DataFileFileName.FromEntityName(entityName)), fieldMetaCollection);
                    dataFile.BeginRead();
                    var fieldValueCollection = new FieldValueCollection();
                    long lastEndDataFileOffset = 0;
                    foreach (var primaryKey in primaryKeys.OrderBy(x => x.StartDataFileOffset))
                    {
                        // сумма байт неактуальных записей (которые были обновлены и помещены в конец файла)
                        fragmentationSizeInBytes += primaryKey.StartDataFileOffset - lastEndDataFileOffset;
                        lastEndDataFileOffset = primaryKey.EndDataFileOffset;
                        dataFile.ReadFields(primaryKey.StartDataFileOffset, primaryKey.EndDataFileOffset, fieldNumbers, fieldValueCollection);
                        // сумма байт удаленных полей
                        fragmentationSizeInBytes += primaryKey.EndDataFileOffset - primaryKey.StartDataFileOffset - dataFile.CalculateSize(fieldValueCollection);
                        fieldValueCollection.Clear();
                    }
                    yield return new FileStatistics
                    {
                        FileName = DataFileFileName.FromEntityName(entityName),
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
            return IOC.Get<IFileSystem>()
                .GetFiles(_workingDirectory).Where(file => Path.GetExtension(file) == PrimaryKeyFileName.Extension)
                .ToList();
        }
    }
}
