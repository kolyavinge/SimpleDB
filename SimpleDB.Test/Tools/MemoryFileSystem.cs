using System.Collections.Generic;
using System.IO;
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
            return FileStreams.Any(x => x.FileFullPath == fullPath);
        }

        public void CreateFileIfNeeded(string fullFilePath)
        {
            FullFilePathes.Add(fullFilePath);
        }

        public IFileStream OpenFileRead(string fullPath)
        {
            var stream = GetStream(fullPath);
            stream.ReadCount++;
            return stream;
        }

        public IFileStream OpenFileWrite(string fullPath)
        {
            var stream = GetStream(fullPath);
            stream.WriteCount++;
            return stream;
        }

        public IFileStream OpenFileReadWrite(string fullPath)
        {
            var stream = GetStream(fullPath);
            stream.ReadCount++;
            stream.WriteCount++;
            return stream;
        }

        private MemoryFileStream GetStream(string fullPath)
        {
            var fileStream = FileStreams.FirstOrDefault(x => x.FileFullPath == fullPath);
            if (fileStream != null)
            {
                fileStream.Seek(0, SeekOrigin.Begin);
                return fileStream;
            }
            else
            {
                fileStream = new MemoryFileStream { FileFullPath = fullPath };
                FileStreams.Add(fileStream);
                return fileStream;
            }
        }

        public IEnumerable<string> GetFiles(string directory)
        {
            return FileStreams.Where(x => Path.GetDirectoryName(x.FileFullPath) == directory).Select(x => x.FileFullPath);
        }

        public void RenameFile(string fullPath, string renamedFullPath)
        {
        }

        public void DeleteFile(string fullPath)
        {
            FileStreams.RemoveAll(x => x.FileFullPath == fullPath);
        }
    }
}
