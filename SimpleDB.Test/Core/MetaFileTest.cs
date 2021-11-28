using System;
using System.Linq;
using NUnit.Framework;
using SimpleDB.Core;
using SimpleDB.Infrastructure;
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
            var metaFile = new MetaFile("full path", _fileSystem);
            metaFile.Save(typeof(Inner), fieldMetaCollection);

            Assert.AreEqual(typeof(Inner), metaFile.GetPrimaryKeyType());

            var fieldMetaCollectionFromFile = metaFile.GetFieldMetaCollection().ToHashSet();
            Assert.AreEqual(16, fieldMetaCollectionFromFile.Count);
            Assert.True(fieldMetaCollection.All(fieldMetaCollectionFromFile.Contains));
        }

        class Inner
        {
            public int Int { get; set; }
        }
    }
}
