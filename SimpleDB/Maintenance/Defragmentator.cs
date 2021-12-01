using System.IO;
using System.Linq;
using SimpleDB.Core;
using SimpleDB.Infrastructure;
using SimpleDB.Utils.EnumerableExtension;

namespace SimpleDB.Maintenance
{
    internal class Defragmentator : IDefragmentator
    {
        private readonly MetaFileCollection _metaFileCollection;
        private readonly IPrimaryKeyFileFactory _primaryKeyFileFactory;
        private readonly IDataFileFactory _dataFileFactory;
        private readonly IFileSystem _fileSystem;

        public Defragmentator(
            IPrimaryKeyFileFactory primaryKeyFileFactory,
            IDataFileFactory dataFileFactory,
            IMetaFileFactory metaFileFactory,
            IFileSystem fileSystem)
        {
            _primaryKeyFileFactory = primaryKeyFileFactory;
            _dataFileFactory = dataFileFactory;
            _fileSystem = fileSystem;
            _metaFileCollection = new MetaFileCollection(metaFileFactory);
        }

        public void DefragmentDataFile(string dataFileName)
        {
            PrimaryKeyFile currentPrimaryKeyFile = null;
            PrimaryKeyFile defragmentPrimaryKeyFile = null;
            DataFile currentDataFile = null;
            DataFile defragmentDataFile = null;

            var entityName = Path.GetFileNameWithoutExtension(dataFileName);
            var metaFile = _metaFileCollection.GetMetaFile(entityName);
            var metaData = metaFile.GetMetaData();
            var primaryKeyType = metaData.PrimaryKeyType;
            var fieldMetaCollection = metaData.FieldMetaCollection.ToList();
            var fieldNumbers = fieldMetaCollection.Select(x => x.Number).ToHashSet();

            currentPrimaryKeyFile = _primaryKeyFileFactory.MakeFromEntityName(entityName, primaryKeyType);
            currentPrimaryKeyFile.BeginRead();
            var primaryKeys = currentPrimaryKeyFile.GetAllPrimaryKeys().Where(x => !x.IsDeleted).OrderBy(x => x.Value).ToList();

            var defragmentPrimaryKeyFileFullPath = GetDefragmentedFullFileName(currentPrimaryKeyFile.FileFullPath);
            defragmentPrimaryKeyFile = _primaryKeyFileFactory.MakeFromFileFullPath(defragmentPrimaryKeyFileFullPath, primaryKeyType);
            defragmentPrimaryKeyFile.BeginWrite();

            currentDataFile = _dataFileFactory.MakeFromEntityName(entityName, fieldMetaCollection);
            currentDataFile.BeginRead();

            var defragmentDataFileFullPath = GetDefragmentedFullFileName(currentDataFile.FileFullPath);
            defragmentDataFile = _dataFileFactory.MakeFromFileFullPath(defragmentDataFileFullPath, fieldMetaCollection);
            defragmentDataFile.BeginWrite();

            var fieldValueCollection = new FieldValueCollection();
            foreach (var primaryKey in primaryKeys)
            {
                currentDataFile.ReadFields(primaryKey.StartDataFileOffset, primaryKey.EndDataFileOffset, fieldNumbers, fieldValueCollection);
                var insertResult = defragmentDataFile.Insert(fieldValueCollection);
                primaryKey.StartDataFileOffset = insertResult.StartDataFileOffset;
                primaryKey.EndDataFileOffset = insertResult.EndDataFileOffset;
                defragmentPrimaryKeyFile.Insert(primaryKey.Value, primaryKey.StartDataFileOffset, primaryKey.EndDataFileOffset);
                fieldValueCollection.Clear();
            }

            currentPrimaryKeyFile.EndReadWrite();
            currentDataFile.EndReadWrite();
            defragmentPrimaryKeyFile.EndReadWrite();
            defragmentDataFile.EndReadWrite();

            _fileSystem.DeleteFile(currentPrimaryKeyFile.FileFullPath);
            _fileSystem.DeleteFile(currentDataFile.FileFullPath);
            _fileSystem.RenameFile(defragmentPrimaryKeyFileFullPath, currentPrimaryKeyFile.FileFullPath);
            _fileSystem.RenameFile(defragmentDataFileFullPath, currentDataFile.FileFullPath);
        }

        private string GetDefragmentedFullFileName(string currentFile)
        {
            return currentFile + ".defrag";
        }
    }
}
