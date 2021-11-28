using SimpleDB.Core;
using SimpleDB.Infrastructure;

namespace SimpleDB.Maintenance
{
    public static class DefragmentatorFactory
    {
        public static IDefragmentator MakeDefragmentator(string workingDirectory)
        {
            return new Defragmentator(
                new PrimaryKeyFileFactory(workingDirectory),
                new DataFileFactory(workingDirectory),
                new MetaFileFactory(workingDirectory),
                FileSystem.Instance);
        }
    }
}
