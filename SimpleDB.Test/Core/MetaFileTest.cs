using System;
using NUnit.Framework;
using SimpleDB.Core;
using SimpleDB.Test.Tools;

namespace SimpleDB.Test.Core;

class MetaFileTest
{
    private MemoryFileSystem _fileSystem;

    [SetUp]
    public void Setup()
    {
        _fileSystem = new MemoryFileSystem();
    }

    [Test]
    public void SaveAndGet()
    {
        var fieldMetaCollection = new FieldMeta[]
        {
            new FieldMeta(1,  "bool", typeof(bool)),
            new FieldMeta(2,  "sbyte", typeof(sbyte)),
            new FieldMeta(3,  "byte", typeof(byte)),
            new FieldMeta(4,  "char", typeof(char)),
            new FieldMeta(5,  "short", typeof(short)),
            new FieldMeta(6,  "ushort", typeof(ushort)),
            new FieldMeta(7,  "int", typeof(int)),
            new FieldMeta(8,  "uint", typeof(uint)),
            new FieldMeta(9,  "long", typeof(long)),
            new FieldMeta(10, "ulong", typeof(ulong)),
            new FieldMeta(11, "float", typeof(float)),
            new FieldMeta(12, "double", typeof(double)),
            new FieldMeta(13, "decimal", typeof(decimal)),
            new FieldMeta(14, "DateTime", typeof(DateTime)),
            new FieldMeta(15, "string", typeof(string)),
            new FieldMeta(16, "Inner", typeof(Inner))
        };
        var metaFile = new MetaFile("full path", _fileSystem);
        var metaData = new MetaData("EntityTypeName", typeof(Inner), "Primary key name", fieldMetaCollection);
        metaFile.Save(metaData);
        var loadedMetaData = metaFile.GetMetaData();
        Assert.IsTrue(metaData.Equals(loadedMetaData));
    }

    class Inner
    {
        public int Int { get; set; }
    }
}
