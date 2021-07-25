using System;
using System.Collections.Generic;
using System.Text;

namespace SimpleDB.Infrastructure
{
    internal interface IFileSystem
    {
        void CreateFileIfNeeded(string fullFilePath);

        IFileStream OpenFile(string fullPath);
    }

    internal class FileSystem : IFileSystem
    {
        public void CreateFileIfNeeded(string fullFilePath)
        {
            if (!System.IO.File.Exists(fullFilePath))
            {
                using (System.IO.File.Create(fullFilePath)) { }
            }
        }

        public IFileStream OpenFile(string fullPath)
        {
            return new FileStream(System.IO.File.Open(fullPath, System.IO.FileMode.Open, System.IO.FileAccess.ReadWrite));
        }
    }
}
