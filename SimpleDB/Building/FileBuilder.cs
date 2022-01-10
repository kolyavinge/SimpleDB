using System.Collections.Generic;
using System.Linq;
using SimpleDB.Core;
using SimpleDB.Infrastructure;

namespace SimpleDB.Building
{
    internal class FileBuilder
    {
        private IFileSystem _fileSystem;

        public FileBuilder(IFileSystem fileSystem)
        {
            _fileSystem = fileSystem;
        }

        public void CreateNewFiles(List<IMapper> mappers)
        {
            var primaryKeyFileNames = mappers.Select(x => PrimaryKeyFileName.FromEntityName(x.EntityName)).ToList();
            var dataFileNames = mappers.Select(x => DataFileName.FromEntityName(x.EntityName)).ToList();
            _fileSystem.CreateNewFiles(primaryKeyFileNames.Union(dataFileNames));
        }
    }
}
