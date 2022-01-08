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
            return FileStreams.Any(x => x.FileName == fileName);
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
            var fileStream = FileStreams.FirstOrDefault(x => x.FileName == fileName);
            if (fileStream != null)
            {
                fileStream.Seek(0, SeekOrigin.Begin);
                return fileStream;
            }
            else
            {
                fileStream = new MemoryFileStream { FileName = fileName };
                FileStreams.Add(fileStream);
                return fileStream;
            }
        }

        public IEnumerable<string> GetFiles()
        {
            return FileStreams.Select(x => x.FileName);
        }

        public void RenameFile(string fileName, string renamedFileName)
        {
            throw new NotImplementedException();
        }

        public void DeleteFile(string fileName)
        {
            FileStreams.RemoveAll(x => x.FileName == fileName);
        }
    }
}
