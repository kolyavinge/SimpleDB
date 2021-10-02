using System.Collections.Generic;
using System.IO;

namespace SimpleDB.Core
{
    internal class MetaFileCollection
    {
        private readonly Dictionary<string, MetaFile> _metaFiles;

        public MetaFileCollection()
        {
            _metaFiles = new Dictionary<string, MetaFile>();
        }

        public MetaFile GetMetaFile(string entityName)
        {
            if (!_metaFiles.ContainsKey(entityName))
            {
                var metaFile = new MetaFile(MetaFileName.GetFullFileName(entityName));
                _metaFiles.Add(entityName, metaFile);
            }

            return _metaFiles[entityName];
        }
    }
}
