using System;
using System.Collections.Generic;
using System.Text;

namespace SimpleDB.Maintenance
{
    public static class StatisticsFactory
    {
        public static IStatistics MakeStatistics(string workingDirectory)
        {
            GlobalSettings.WorkingDirectory = workingDirectory;
            return new Statistics();
        }
    }
}
