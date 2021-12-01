using System;
using System.Linq;
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
                new FieldMeta(0, typeof(bool)),
                new FieldMeta(1, typeof(sbyte)),
                new FieldMeta(2, typeof(byte)),
                new FieldMeta(3, typeof(char)),
                new FieldMeta(4, typeof(short)),
                new FieldMeta(5, typeof(ushort)),
                new FieldMeta(6, typeof(int)),
                new FieldMeta(7, typeof(uint)),
                new FieldMeta(8, typeof(long)),
                new FieldMeta(9, typeof(ulong)),
                new FieldMeta(10, typeof(float)),
                new FieldMeta(11, typeof(double)),
                new FieldMeta(12, typeof(decimal)),
                new FieldMeta(13, typeof(DateTime)),
                new FieldMeta(14, typeof(string)),
                new FieldMeta(15, typeof(Inner)),
            };
            var fieldNameCollection = new FieldName[]
            {
                new FieldName(0, "bool"),
                new FieldName(1, "sbyte"),
                new FieldName(2, "byte"),
                new FieldName(3, "char"),
                new FieldName(4, "short"),
                new FieldName(5, "ushort"),
                new FieldName(6, "int"),
                new FieldName(7, "uint"),
                new FieldName(8, "long"),
                new FieldName(9, "ulong"),
                new FieldName(10, "float"),
                new FieldName(11, "double"),
                new FieldName(12, "decimal"),
                new FieldName(13, "DateTime"),
                new FieldName(14, "string"),
                new FieldName(15, "Inner"),
            };
            var metaFile = new MetaFile("full path", _fileSystem);
            var metaData = MetaData.Make("EntityTypeName", typeof(Inner), "Primary key name", fieldMetaCollection, fieldNameCollection);
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
