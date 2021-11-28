using SimpleDB.Core;
using SimpleDB.Infrastructure;

namespace SimpleDB.Maintenance
{
    public static class StatisticsFactory
    {
        public static IStatistics MakeStatistics(string workingDirectory)
        {
            return new Statistics(
                workingDirectory,
                new PrimaryKeyFileFactory(workingDirectory),
                new DataFileFactory(workingDirectory),
                new MetaFileFactory(workingDirectory),
                FileSystem.Instance);
        }
    }
}
