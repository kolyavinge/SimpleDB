using SimpleDB.Core;
using SimpleDB.IndexedSearch;
using SimpleDB.QueryExecutors;

namespace SimpleDB.Sql
{
    internal static class SqlQueryExecutorFactory
    {
        public static ISqlQueryExecutor Make(string workingDirectory)
        {
            return new SqlQueryExecutor(
                new PrimaryKeyFileFactory(workingDirectory),
                new DataFileFactory(workingDirectory),
                new IndexHolder());
        }
    }
}
