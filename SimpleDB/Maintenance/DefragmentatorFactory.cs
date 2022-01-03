using SimpleDB.Core;
using SimpleDB.Infrastructure;

namespace SimpleDB.Maintenance
{
    public static class DefragmentatorFactory
    {
        public static IDefragmentator MakeDefragmentator(string databaseFilePath)
        {
            var fileSystem = new FileSystem(databaseFilePath);

            return new Defragmentator(
                new PrimaryKeyFileFactory(fileSystem),
                new DataFileFactory(fileSystem),
                new MetaFileFactory(fileSystem),
                fileSystem);
        }
    }
}
