using SimpleDB.Core;
using SimpleDB.IndexedSearch;
using SimpleDB.QueryExecutors;

namespace SimpleDB.Sql
{
    internal static class SqlQueryExecutorFactory
    {
        public static ISqlQueryExecutor MakeQueryExecutor(string workingDirectory)
        {
            return new SqlQueryExecutor(
                new PrimaryKeyFileFactory(workingDirectory),
                new DataFileFactory(workingDirectory),
                new IndexHolder());
        }
    }
}
