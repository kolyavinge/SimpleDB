using SimpleDB.Core;
using SimpleDB.Infrastructure;

namespace SimpleDB.Maintenance;

public static class StatisticsFactory
{
    public static IStatistics MakeStatistics(string databaseFilePath)
    {
        var fileSystem = new FileSystem(databaseFilePath);

        return new Statistics(
            new PrimaryKeyFileFactory(fileSystem),
            new DataFileFactory(fileSystem),
            new MetaFileFactory(fileSystem),
            fileSystem);
    }
}
