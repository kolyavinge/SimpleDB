using System.Collections.Generic;
using System.Linq;
using SimpleDB.Infrastructure;

namespace SimpleDB.Test.Tools
{
    internal class MemoryFileSystem : IFileSystem
    {
        private List<MemoryFileStream> _fileStreams;

        public MemoryFileSystem()
        {
            _fileStreams = new List<MemoryFileStream>();
            FullFilePathes = new List<string>();
        }

        public List<string> FullFilePathes { get; set; }

        public void Reset()
        {
            _fileStreams = new List<MemoryFileStream>();
            FullFilePathes = new List<string>();
        }

        public void CreateFileIfNeeded(string fullFilePath)
        {
            FullFilePathes.Add(fullFilePath);
        }

        public IFileStream OpenFile(string fullPath)
        {
            var fileStream = _fileStreams.FirstOrDefault(x => x.FileFullPath == fullPath);
            if (fileStream != null)
            {
                return fileStream;
            }
            else
            {
                fileStream = new MemoryFileStream { FileFullPath = fullPath };
                _fileStreams.Add(fileStream);
                return fileStream;
            }
        }
    }
}
