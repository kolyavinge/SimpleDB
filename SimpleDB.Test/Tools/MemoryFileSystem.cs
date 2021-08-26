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

        public bool FileExists(string fullPath)
        {
            return false;
        }

        public void CreateFileIfNeeded(string fullFilePath)
        {
            FullFilePathes.Add(fullFilePath);
        }

        public IFileStream OpenFileRead(string fullPath)
        {
            var fileStream = FileStreams.FirstOrDefault(x => x.FileFullPath == fullPath);
            if (fileStream != null)
            {
                fileStream.Seek(0, System.IO.SeekOrigin.Begin);
                return fileStream;
            }
            else
            {
                fileStream = new MemoryFileStream { FileFullPath = fullPath };
                FileStreams.Add(fileStream);
                return fileStream;
            }
        }

        public IFileStream OpenFileWrite(string fullPath)
        {
            return OpenFileRead(fullPath);
        }

        public IFileStream OpenFileReadWrite(string fullPath)
        {
            return OpenFileRead(fullPath);
        }
    }
}
