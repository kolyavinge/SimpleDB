using System.Collections.Generic;
using System.IO;

namespace SimpleDB.Core
{
    internal class MetaFileCollection
    {
        private readonly string _workingDirectory;
        private readonly Dictionary<string, MetaFile> _metaFiles;

        public MetaFileCollection(string workingDirectory)
        {
            _workingDirectory = workingDirectory;
            _metaFiles = new Dictionary<string, MetaFile>();
        }

        public MetaFile GetMetaFile(string entityName)
        {
            if (!_metaFiles.ContainsKey(entityName))
            {
                var metaFile = new MetaFile(Path.Combine(_workingDirectory, MetaFileName.FromEntityName(entityName)));
                _metaFiles.Add(entityName, metaFile);
            }

            return _metaFiles[entityName];
        }
    }
}
