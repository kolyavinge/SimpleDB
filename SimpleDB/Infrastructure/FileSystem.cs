using System.Collections.Generic;
using System.IO;

namespace SimpleDB.Infrastructure
{
    internal interface IFileSystem
    {
        bool FileExists(string fullPath);

        void CreateFileIfNeeded(string fullPath);

        IFileStream OpenFileRead(string fullPath);

        IFileStream OpenFileWrite(string fullPath);

        IFileStream OpenFileReadWrite(string fullPath);

        IEnumerable<string> GetFiles(string directory);

        void RenameFile(string fullPath, string renamedFullPath);

        void DeleteFile(string fullPath);
    }

    internal class FileSystem : IFileSystem
    {
        public bool FileExists(string fullPath)
        {
            return System.IO.File.Exists(fullPath);
        }

        public void CreateFileIfNeeded(string fullPath)
        {
            if (!System.IO.File.Exists(fullPath))
            {
                using (System.IO.File.Create(fullPath)) { }
            }
        }

        public IFileStream OpenFileRead(string fullPath)
        {
            return new FileStream(System.IO.File.Open(fullPath, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read));
        }

        public IFileStream OpenFileWrite(string fullPath)
        {
            return new FileStream(System.IO.File.OpenWrite(fullPath));
        }

        public IFileStream OpenFileReadWrite(string fullPath)
        {
            return new FileStream(System.IO.File.Open(fullPath, System.IO.FileMode.Open, System.IO.FileAccess.ReadWrite));
        }

        public IEnumerable<string> GetFiles(string directory)
        {
            return Directory.GetFiles(directory);
        }

        public void RenameFile(string fullPath, string renamedFullPath)
        {
            File.Move(fullPath, renamedFullPath);
        }

        public void DeleteFile(string fullPath)
        {
            File.Delete(fullPath);
        }
    }
}
