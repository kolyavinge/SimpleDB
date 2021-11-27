using System.Collections.Generic;
using System.IO;

namespace SimpleDB.Core
{
    internal class MetaFileCollection
    {
        private readonly Dictionary<string, MetaFile> _metaFiles;
        private readonly string _workingDirectory;

        public MetaFileCollection(string workingDirectory)
        {
            _metaFiles = new Dictionary<string, MetaFile>();
            _workingDirectory = workingDirectory;
        }

        public MetaFile GetMetaFile(string entityName)
        {
            if (!_metaFiles.ContainsKey(entityName))
            {
                var metaFile = new MetaFile(MetaFileName.GetFullFileName(_workingDirectory, entityName));
                _metaFiles.Add(entityName, metaFile);
            }

            return _metaFiles[entityName];
        }
    }
}
