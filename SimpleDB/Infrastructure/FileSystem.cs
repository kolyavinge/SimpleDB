using System.Collections.Generic;
using System.IO;
using System.Linq;
using SingleFileStorage;

namespace SimpleDB.Infrastructure
{
    internal interface IFileSystem
    {
        bool FileExists(string fileName);

        void CreateFiles(params string[] fileNames);

        void CreateNewFiles(IEnumerable<string> fileNames);

        IFileStream OpenFileRead(string fileName);

        IFileStream OpenFileReadWrite(string fileName);

        IEnumerable<string> GetFiles();

        void RenameFile(string fileName, string renamedFileName);

        void DeleteFile(string fileName);
    }

    internal class FileSystem : IFileSystem
    {
        private readonly string _databaseFilePath;
        private readonly List<IFileStream> _openedFiles;
        private IStorage _storage;

        public FileSystem(string databaseFilePath)
        {
            _databaseFilePath = databaseFilePath;
            _openedFiles = new List<IFileStream>();
            if (!File.Exists(databaseFilePath)) StorageFile.Create(databaseFilePath);
        }

        public bool FileExists(string fileName)
        {
            ThrowErrorIfOpenedFilesExists();
            using (var storage = StorageFile.Open(_databaseFilePath, Access.Read))
            {
                return storage.IsRecordExist(fileName);
            }
        }

        public void CreateFiles(params string[] fileNames)
        {
            ThrowErrorIfOpenedFilesExists();
            using (var storage = StorageFile.Open(_databaseFilePath, Access.Modify))
            {
                foreach (var fileName in fileNames)
                {
                    storage.CreateRecord(fileName);
                }
            }
        }

        public void CreateNewFiles(IEnumerable<string> fileNames)
        {
            ThrowErrorIfOpenedFilesExists();
            using (var storage = StorageFile.Open(_databaseFilePath, Access.Modify))
            {
                foreach (var fileName in fileNames)
                {
                    if (!storage.IsRecordExist(fileName))
                    {
                        storage.CreateRecord(fileName);
                    }
                }
            }
        }

        public IFileStream OpenFileRead(string fileName)
        {
            if (_storage == null) _storage = StorageFile.Open(_databaseFilePath, Access.Read);
            else if (_storage.AccessMode != Access.Read) throw new IOException("File must be opened with Read mode.");
            var fileStream = new FileStream(_storage.OpenRecord(fileName), DisposeFileStreamFunc);
            _openedFiles.Add(fileStream);

            return fileStream;
        }

        public IFileStream OpenFileReadWrite(string fileName)
        {
            if (_storage == null) _storage = StorageFile.Open(_databaseFilePath, Access.Modify);
            else if (_storage.AccessMode != Access.Modify) throw new IOException("File must be opened with ReadWrite mode");
            var fileStream = new FileStream(_storage.OpenRecord(fileName), DisposeFileStreamFunc);
            _openedFiles.Add(fileStream);

            return fileStream;
        }

        private void DisposeFileStreamFunc(IFileStream fileStream)
        {
            _openedFiles.Remove(fileStream);
            if (!_openedFiles.Any())
            {
                _storage.Dispose();
                _storage = null;
            }
        }

        public IEnumerable<string> GetFiles()
        {
            ThrowErrorIfOpenedFilesExists();
            using (var storage = StorageFile.Open(_databaseFilePath, Access.Read))
            {
                return storage.GetAllRecordNames();
            }
        }

        public void RenameFile(string fileName, string renamedFileName)
        {
            ThrowErrorIfOpenedFilesExists();
            using (var storage = StorageFile.Open(_databaseFilePath, Access.Modify))
            {
                storage.RenameRecord(fileName, renamedFileName);
            }
        }

        public void DeleteFile(string fileName)
        {
            ThrowErrorIfOpenedFilesExists();
            using (var storage = StorageFile.Open(_databaseFilePath, Access.Modify))
            {
                storage.DeleteRecord(fileName);
            }
        }

        private void ThrowErrorIfOpenedFilesExists()
        {
            if (_storage != null) throw new IOException("All opened files must be closed.");
        }
    }
}
