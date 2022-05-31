using System.IO;
using System.Linq;
using SimpleDB.Core;
using SimpleDB.Infrastructure;

namespace SimpleDB.Maintenance;

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
        var entityName = Path.GetFileNameWithoutExtension(dataFileName);
        var metaFile = _metaFileCollection.GetMetaFile(entityName);
        var metaData = metaFile.GetMetaData();
        var primaryKeyType = metaData.PrimaryKeyType;
        var fieldMetaCollection = metaData.FieldMetaCollection.ToList();
        var fieldNumbers = fieldMetaCollection.Select(x => x.Number).ToHashSet();

        var currentPrimaryKeyFileName = PrimaryKeyFileName.FromEntityName(entityName);
        var currentDataFileName = DataFileName.FromEntityName(entityName);
        var defragmentPrimaryKeyFileName = GetDefragmentedFileName(currentPrimaryKeyFileName);
        var defragmentDataFileName = GetDefragmentedFileName(currentDataFileName);
        _fileSystem.CreateFiles(defragmentPrimaryKeyFileName, defragmentDataFileName);

        var currentPrimaryKeyFile = _primaryKeyFileFactory.MakeFromFileName(currentPrimaryKeyFileName, primaryKeyType);
        currentPrimaryKeyFile.BeginReadWrite();
        var primaryKeys = currentPrimaryKeyFile.GetAllPrimaryKeys().Where(x => !x.IsDeleted).OrderBy(x => x.Value).ToList();

        var defragmentPrimaryKeyFile = _primaryKeyFileFactory.MakeFromFileName(defragmentPrimaryKeyFileName, primaryKeyType);
        defragmentPrimaryKeyFile.BeginReadWrite();

        var currentDataFile = _dataFileFactory.MakeFromFileName(currentDataFileName, fieldMetaCollection);
        currentDataFile.BeginReadWrite();

        var defragmentDataFile = _dataFileFactory.MakeFromFileName(defragmentDataFileName, fieldMetaCollection);
        defragmentDataFile.BeginReadWrite();

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

        _fileSystem.DeleteFile(currentPrimaryKeyFile.FileName);
        _fileSystem.DeleteFile(currentDataFile.FileName);
        _fileSystem.RenameFile(defragmentPrimaryKeyFileName, currentPrimaryKeyFile.FileName);
        _fileSystem.RenameFile(defragmentDataFileName, currentDataFile.FileName);

        _fileSystem.DefragmentDatabaseFile();
    }

    private string GetDefragmentedFileName(string currentFile) => currentFile + ".defrag";
}
