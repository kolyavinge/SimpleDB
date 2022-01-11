using System;
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
            FileNames = new List<string>();
        }

        public string DatabaseFilePath { get; }

        public List<string> FileNames { get; set; }

        public bool FileExists(string fileName)
        {
            return FileStreams.Any(x => x.Name == fileName);
        }

        public void CreateFiles(params string[] fileNames)
        {
            foreach (var fileName in fileNames)
            {
                FileNames.Add(fileName);
            }
        }

        public void CreateNewFiles(IEnumerable<string> fileNames)
        {
            throw new NotImplementedException();
        }

        public IFileStream OpenFileRead(string fileName)
        {
            return GetStream(fileName);
        }

        public IFileStream OpenFileReadWrite(string fileName)
        {
            return GetStream(fileName);
        }

        private MemoryFileStream GetStream(string fileName)
        {
            var fileStream = FileStreams.FirstOrDefault(x => x.Name == fileName);
            if (fileStream != null)
            {
                fileStream.Seek(0, SeekOrigin.Begin);
                return fileStream;
            }
            else
            {
                fileStream = new MemoryFileStream { Name = fileName };
                FileStreams.Add(fileStream);
                return fileStream;
            }
        }

        public IEnumerable<string> GetFiles()
        {
            return FileStreams.Select(x => x.Name);
        }

        public void RenameFile(string fileName, string renamedFileName)
        {
            FileNames.Remove(fileName);
            FileNames.Add(renamedFileName);
            FileStreams.First(x => x.Name == fileName).Name = renamedFileName;
        }

        public void DeleteFile(string fileName)
        {
            FileNames.Remove(fileName);
            FileStreams.RemoveAll(x => x.Name == fileName);
        }

        public void DefragmentDatabaseFile()
        {
        }
    }
}
