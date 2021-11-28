using System.Linq;
using NUnit.Framework;
using SimpleDB.Core;
using SimpleDB.Infrastructure;
using SimpleDB.Test.Tools;

namespace SimpleDB.Test.Core
{
    public class PrimaryKeyFileTest
    {
        private MemoryFileSystem _fileSystem;
        private Memory _memory;

        [SetUp]
        public void Setup()
        {
            _fileSystem = new MemoryFileSystem();
            _memory = Memory.Instance;
        }

        [Test]
        public void CreateFile()
        {
            new PrimaryKeyFile(@"working directory\testEntity.primary", typeof(int), _fileSystem, _memory);
            Assert.True(_fileSystem.FullFilePathes.Contains(@"working directory\testEntity.primary"));
        }

        [Test]
        public void Insert()
        {
            var primaryKeyFile = new PrimaryKeyFile("", typeof(int), _fileSystem, _memory);
            primaryKeyFile.BeginWrite();
            var primaryKey1 = primaryKeyFile.Insert(123, 0, 45);
            var primaryKey2 = primaryKeyFile.Insert(456, 45, 60);
            Assert.AreEqual(123, primaryKey1.Value);
            Assert.AreEqual(0, primaryKey1.StartDataFileOffset);
            Assert.AreEqual(45, primaryKey1.EndDataFileOffset);
            Assert.AreEqual(0, primaryKey1.PrimaryKeyFileOffset);
            Assert.AreEqual(456, primaryKey2.Value);
            Assert.AreEqual(45, primaryKey2.StartDataFileOffset);
            Assert.AreEqual(60, primaryKey2.EndDataFileOffset);
            Assert.AreEqual(1 + 8 + 8 + 4, primaryKey2.PrimaryKeyFileOffset);
        }

        [Test]
        public void InsertAndGetAllPrimaryKeys()
        {
            var primaryKeyFile = new PrimaryKeyFile("", typeof(int), _fileSystem, _memory);
            primaryKeyFile.BeginWrite();
            primaryKeyFile.Insert(123, 0, 45);
            var allPrimaryKeys = primaryKeyFile.GetAllPrimaryKeys().ToList();
            Assert.AreEqual(1, allPrimaryKeys.Count);
            Assert.AreEqual(123, allPrimaryKeys[0].Value);
            Assert.AreEqual(0, allPrimaryKeys[0].StartDataFileOffset);
            Assert.AreEqual(45, allPrimaryKeys[0].EndDataFileOffset);
        }

        [Test]
        public void InsertAndGetAllPrimaryKeys_Object()
        {
            var primaryKeyFile = new PrimaryKeyFile("", typeof(TestEntity), _fileSystem, _memory);
            primaryKeyFile.BeginWrite();
            var obj = new TestEntity { Int = 123, Float = 4.56f, String = "123" };
            primaryKeyFile.Insert(obj, 0, 45);

            var allPrimaryKeys = primaryKeyFile.GetAllPrimaryKeys().ToList();
            Assert.AreEqual(1, allPrimaryKeys.Count);
            var resultObj = (TestEntity)allPrimaryKeys[0].Value;
            Assert.AreEqual(123, resultObj.Int);
            Assert.AreEqual(4.56f, resultObj.Float);
            Assert.AreEqual("123", resultObj.String);
            Assert.AreEqual(0, allPrimaryKeys[0].StartDataFileOffset);
            Assert.AreEqual(45, allPrimaryKeys[0].EndDataFileOffset);
        }

        [Test]
        public void UpdateStartEndDataFileOffset()
        {
            var primaryKeyFile = new PrimaryKeyFile("", typeof(int), _fileSystem, _memory);
            primaryKeyFile.BeginWrite();
            primaryKeyFile.Insert(123, 10, 20);
            var second = primaryKeyFile.Insert(456, 30, 35);
            primaryKeyFile.UpdateStartEndDataFileOffset(second.PrimaryKeyFileOffset, 40, 50);
            var allPrimaryKeys = primaryKeyFile.GetAllPrimaryKeys().ToList();
            Assert.AreEqual(10, allPrimaryKeys[0].StartDataFileOffset);
            Assert.AreEqual(20, allPrimaryKeys[0].EndDataFileOffset);
            Assert.AreEqual(40, allPrimaryKeys[1].StartDataFileOffset);
            Assert.AreEqual(50, allPrimaryKeys[1].EndDataFileOffset);
        }

        [Test]
        public void UpdateEndDataFileOffset()
        {
            var primaryKeyFile = new PrimaryKeyFile("", typeof(int), _fileSystem, _memory);
            primaryKeyFile.BeginWrite();
            primaryKeyFile.Insert(123, 10, 20);
            var second = primaryKeyFile.Insert(456, 30, 35);
            primaryKeyFile.UpdateEndDataFileOffset(second.PrimaryKeyFileOffset, 40);
            var allPrimaryKeys = primaryKeyFile.GetAllPrimaryKeys().ToList();
            Assert.AreEqual(10, allPrimaryKeys[0].StartDataFileOffset);
            Assert.AreEqual(20, allPrimaryKeys[0].EndDataFileOffset);
            Assert.AreEqual(30, allPrimaryKeys[1].StartDataFileOffset);
            Assert.AreEqual(40, allPrimaryKeys[1].EndDataFileOffset);
        }

        [Test]
        public void Delete()
        {
            var primaryKeyFile = new PrimaryKeyFile("", typeof(int), _fileSystem, _memory);
            primaryKeyFile.BeginWrite();
            primaryKeyFile.Insert(123, 10, 20);
            var second = primaryKeyFile.Insert(456, 30, 35);
            primaryKeyFile.Delete(second.PrimaryKeyFileOffset);
            var allPrimaryKeys = primaryKeyFile.GetAllPrimaryKeys().ToList();
            Assert.False(allPrimaryKeys[0].IsDeleted);
            Assert.True(allPrimaryKeys[1].IsDeleted);
        }

        class TestEntity
        {
            public int Int { get; set; }

            public float Float { get; set; }

            public string String { get; set; }
        }
    }
}
