using System.Collections.Generic;

namespace SimpleDB.Maintenance
{
    public interface IStatistics
    {
        IEnumerable<FileStatistics> GetPrimaryKeyFileStatistics();

        IEnumerable<FileStatistics> GetDataFileStatistics();
    }

    public struct FileStatistics
    {
        public string FileName { get; internal set; }

        public long TotalFileSizeInBytes { get; internal set; }

        public long FragmentationSizeInBytes { get; internal set; }

        public double FragmentationPercent { get { return 100.0 * FragmentationSizeInBytes / TotalFileSizeInBytes; } }
    }
}
