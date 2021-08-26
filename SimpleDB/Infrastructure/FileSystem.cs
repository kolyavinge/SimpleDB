namespace SimpleDB.Infrastructure
{
    internal interface IFileSystem
    {
        bool FileExists(string fullFilePath);

        void CreateFileIfNeeded(string fullFilePath);

        IFileStream OpenFileRead(string fullPath);

        IFileStream OpenFileWrite(string fullPath);

        IFileStream OpenFileReadWrite(string fullPath);
    }

    internal class FileSystem : IFileSystem
    {
        public bool FileExists(string fullFilePath)
        {
            return System.IO.File.Exists(fullFilePath);
        }

        public void CreateFileIfNeeded(string fullFilePath)
        {
            if (!System.IO.File.Exists(fullFilePath))
            {
                using (System.IO.File.Create(fullFilePath)) { }
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
    }
}
