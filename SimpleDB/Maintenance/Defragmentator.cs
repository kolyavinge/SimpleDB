using System.IO;
using System.Linq;
using SimpleDB.Core;
using SimpleDB.Infrastructure;

namespace SimpleDB.Maintenance
{
    internal class Defragmentator : IDefragmentator
    {
        private readonly MetaFileCollection _metaFileCollection;

        public Defragmentator()
        {
            _metaFileCollection = new MetaFileCollection();
        }

        public void DefragmentDataFile(string dataFileName)
        {
            PrimaryKeyFile currentPrimaryKeyFile = null;
            PrimaryKeyFile defragmentPrimaryKeyFile = null;
            DataFile currentDataFile = null;
            DataFile defragmentDataFile = null;

            var entityName = Path.GetFileNameWithoutExtension(dataFileName);
            var currentPrimaryKeyFileFullPath = PrimaryKeyFileName.GetFullFileName(entityName);
            var currentDataFileFullPath = DataFileName.GetFullFileName(entityName);
            var metaFile = _metaFileCollection.GetMetaFile(entityName);
            var primaryKeyType = metaFile.GetPrimaryKeyType();
            var fieldMetaCollection = metaFile.GetFieldMetaCollection().ToList();
            var fieldNumbers = fieldMetaCollection.Select(x => x.Number).ToHashSet();

            currentPrimaryKeyFile = new PrimaryKeyFile(currentPrimaryKeyFileFullPath, primaryKeyType);
            currentPrimaryKeyFile.BeginRead();
            var primaryKeys = currentPrimaryKeyFile.GetAllPrimaryKeys().Where(x => !x.IsDeleted).OrderBy(x => x.Value).ToList();

            var defragmentPrimaryKeyFileFullPath = GetDefragmentedFullFileName(currentPrimaryKeyFileFullPath);
            defragmentPrimaryKeyFile = new PrimaryKeyFile(defragmentPrimaryKeyFileFullPath, primaryKeyType);
            defragmentPrimaryKeyFile.BeginWrite();

            currentDataFile = new DataFile(currentDataFileFullPath, fieldMetaCollection);
            currentDataFile.BeginRead();

            var defragmentDataFileFullPath = GetDefragmentedFullFileName(currentDataFileFullPath);
            defragmentDataFile = new DataFile(defragmentDataFileFullPath, fieldMetaCollection);
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

            IOC.Get<IFileSystem>().DeleteFile(currentPrimaryKeyFileFullPath);
            IOC.Get<IFileSystem>().DeleteFile(currentDataFileFullPath);
            IOC.Get<IFileSystem>().RenameFile(defragmentPrimaryKeyFileFullPath, currentPrimaryKeyFileFullPath);
            IOC.Get<IFileSystem>().RenameFile(defragmentDataFileFullPath, currentDataFileFullPath);
        }

        private string GetDefragmentedFullFileName(string currentFile)
        {
            return currentFile + ".defrag";
        }
    }
}
