using System.Collections.Generic;
using System.IO;
using System.Linq;
using SingleFileStorage;

namespace SimpleDB.Infrastructure
{
    internal interface IFileSystem
    {
        string DatabaseFilePath { get; }

        bool FileExists(string fileName);

        void CreateFiles(params string[] fileNames);

        void CreateNewFiles(params string[] fileNames);

        IFileStream OpenFileRead(string fileName);

        IFileStream OpenFileReadWrite(string fileName);

        IEnumerable<string> GetFiles(string extension);

        void RenameFile(string fileName, string renamedFileName);

        void DeleteFile(string fileName);

        void DefragmentDatabaseFile();
    }

    internal class FileSystem : IFileSystem
    {
        private readonly List<IFileStream> _openedFiles;
        private IStorage? _storage;

        public string DatabaseFilePath { get; }

        public FileSystem(string databaseFilePath)
        {
            DatabaseFilePath = databaseFilePath;
            _openedFiles = new List<IFileStream>();
            if (!File.Exists(databaseFilePath)) StorageFile.Create(databaseFilePath);
        }

        public bool FileExists(string fileName)
        {
            ThrowErrorIfOpenedFilesExists();
            using (var storage = StorageFile.Open(DatabaseFilePath, Access.Read))
            {
                return storage.IsRecordExist(fileName);
            }
        }

        public void CreateFiles(params string[] fileNames)
        {
            ThrowErrorIfOpenedFilesExists();
            using (var storage = StorageFile.Open(DatabaseFilePath, Access.Modify))
            {
                foreach (var fileName in fileNames)
                {
                    storage.CreateRecord(fileName);
                }
            }
        }

        public void CreateNewFiles(params string[] fileNames)
        {
            ThrowErrorIfOpenedFilesExists();
            using (var storage = StorageFile.Open(DatabaseFilePath, Access.Modify))
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
            if (_storage == null) _storage = StorageFile.Open(DatabaseFilePath, Access.Read);
            else if (_storage.AccessMode != Access.Read) throw new IOException("File must be opened with Read mode.");
            if (_openedFiles.Any(x => x.Name == fileName)) throw new IOException($"File {fileName} already has been opened.");
            var fileStream = new FileStream(fileName, _storage.OpenRecord(fileName), DisposeFileStreamFunc);
            _openedFiles.Add(fileStream);

            return fileStream;
        }

        public IFileStream OpenFileReadWrite(string fileName)
        {
            if (_storage == null) _storage = StorageFile.Open(DatabaseFilePath, Access.Modify);
            else if (_storage.AccessMode != Access.Modify) throw new IOException("File must be opened with ReadWrite mode");
            if (_openedFiles.Any(x => x.Name == fileName)) throw new IOException($"File {fileName} already has been opened.");
            var fileStream = new FileStream(fileName, _storage.OpenRecord(fileName), DisposeFileStreamFunc);
            _openedFiles.Add(fileStream);

            return fileStream;
        }

        private void DisposeFileStreamFunc(IFileStream fileStream)
        {
            _openedFiles.Remove(fileStream);
            if (!_openedFiles.Any() && _storage != null)
            {
                _storage.Dispose();
                _storage = null;
            }
        }

        public IEnumerable<string> GetFiles(string extension)
        {
            ThrowErrorIfOpenedFilesExists();
            using (var storage = StorageFile.Open(DatabaseFilePath, Access.Read))
            {
                return storage.GetAllRecordNames().Where(file => Path.GetExtension(file) == extension).ToList();
            }
        }

        public void RenameFile(string fileName, string renamedFileName)
        {
            ThrowErrorIfOpenedFilesExists();
            using (var storage = StorageFile.Open(DatabaseFilePath, Access.Modify))
            {
                storage.RenameRecord(fileName, renamedFileName);
            }
        }

        public void DeleteFile(string fileName)
        {
            ThrowErrorIfOpenedFilesExists();
            using (var storage = StorageFile.Open(DatabaseFilePath, Access.Modify))
            {
                storage.DeleteRecord(fileName);
            }
        }

        public void DefragmentDatabaseFile()
        {
            var defragmentator = SingleFileStorage.Maintenance.DefragmentatorFactory.Make();
            defragmentator.Defragment(DatabaseFilePath);
        }

        private void ThrowErrorIfOpenedFilesExists()
        {
            if (_storage != null) throw new IOException("All opened files must be closed.");
        }
    }
}
