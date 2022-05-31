using System.Collections.Generic;

namespace SimpleDB.Core;

internal class MetaFileCollection
{
    private readonly Dictionary<string, MetaFile> _metaFiles;
    private readonly IMetaFileFactory _metaFileFactory;

    public MetaFileCollection(IMetaFileFactory metaFileFactory)
    {
        _metaFiles = new Dictionary<string, MetaFile>();
        _metaFileFactory = metaFileFactory;
    }

    public MetaFile GetMetaFile(string entityName)
    {
        if (!_metaFiles.ContainsKey(entityName))
        {
            var metaFile = _metaFileFactory.MakeFromEntityName(entityName);
            _metaFiles.Add(entityName, metaFile);
        }

        return _metaFiles[entityName];
    }
}
