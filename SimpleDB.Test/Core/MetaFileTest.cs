using System;
using NUnit.Framework;
using SimpleDB.Core;
using SimpleDB.Test.Tools;

namespace SimpleDB.Test.Core
{
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
                new FieldMeta(0, "bool", typeof(bool)),
                new FieldMeta(1, "sbyte", typeof(sbyte)),
                new FieldMeta(2, "byte", typeof(byte)),
                new FieldMeta(3, "char", typeof(char)),
                new FieldMeta(4, "short", typeof(short)),
                new FieldMeta(5, "ushort", typeof(ushort)),
                new FieldMeta(6, "int", typeof(int)),
                new FieldMeta(7, "uint", typeof(uint)),
                new FieldMeta(8, "long", typeof(long)),
                new FieldMeta(9, "ulong", typeof(ulong)),
                new FieldMeta(10, "float", typeof(float)),
                new FieldMeta(11, "double", typeof(double)),
                new FieldMeta(12, "decimal", typeof(decimal)),
                new FieldMeta(13, "DateTime", typeof(DateTime)),
                new FieldMeta(14, "string", typeof(string)),
                new FieldMeta(15, "Inner", typeof(Inner))
            };
            var metaFile = new MetaFile("full path", _fileSystem);
            var metaData = MetaData.Make("EntityTypeName", typeof(Inner), "Primary key name", fieldMetaCollection);
            metaFile.Save(metaData);
            var loadedMetaData = metaFile.GetMetaData();
            Assert.IsTrue(metaData.Equals(loadedMetaData));
        }

        class Inner
        {
            public int Int { get; set; }
        }
    }
}
