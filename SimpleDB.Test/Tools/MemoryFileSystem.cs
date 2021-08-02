using System.Collections.Generic;
using System.Linq;
using SimpleDB.Infrastructure;

namespace SimpleDB.Test.Tools
{
    internal class MemoryFileSystem : IFileSystem
    {
        public List<MemoryFileStream> FileStreams { get; set; }

        public MemoryFileSystem()
        {
            FileStreams = new List<MemoryFileStream>();
            FullFilePathes = new List<string>();
        }

        public List<string> FullFilePathes { get; set; }

        public void CreateFileIfNeeded(string fullFilePath)
        {
            FullFilePathes.Add(fullFilePath);
        }

        public IFileStream OpenFile(string fullPath)
        {
            var fileStream = FileStreams.FirstOrDefault(x => x.FileFullPath == fullPath);
            if (fileStream != null)
            {
                return fileStream;
            }
            else
            {
                fileStream = new MemoryFileStream { FileFullPath = fullPath };
                FileStreams.Add(fileStream);
                return fileStream;
            }
        }
    }
}
