using SimpleDB.Core;
using SimpleDB.IndexedSearch;
using SimpleDB.Infrastructure;

namespace SimpleDB.Sql
{
    internal static class SqlQueryExecutorFactory
    {
        public static ISqlQueryExecutor Make(string databaseFilePath)
        {
            var fileSystem = new FileSystem(databaseFilePath);

            return new SqlQueryExecutor(
                new PrimaryKeyFileFactory(fileSystem),
                new DataFileFactory(fileSystem),
                new IndexHolder());
        }
    }
}
