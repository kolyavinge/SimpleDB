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
