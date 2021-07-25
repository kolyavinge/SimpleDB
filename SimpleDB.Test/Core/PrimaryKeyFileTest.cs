using System.Linq;
using NUnit.Framework;
using SimpleDB.Core;
using SimpleDB.Infrastructure;
using SimpleDB.Test.Tools;

namespace SimpleDB.Test.Core
{
    public class PrimaryKeyFileTest
    {
        private MemoryFileSystem _fileSystem = new MemoryFileSystem();

        [SetUp]
        public void Setup()
        {
            IOC.Reset();
            IOC.Set<IFileSystem>(_fileSystem);
        }

        [Test]
        public void InsertAndGetAllPrimaryKeys()
        {
            _fileSystem.Reset();
            var primaryKeyFile = new PrimaryKeyFile("", typeof(int));
            primaryKeyFile.Insert(new PrimaryKey(123, 0, 45));

            var allPrimaryKeys = primaryKeyFile.GetAllPrimaryKeys().ToList();
            Assert.AreEqual(1, allPrimaryKeys.Count);
            Assert.AreEqual(123, allPrimaryKeys[0].Value);
            Assert.AreEqual(0, allPrimaryKeys[0].StartDataFileOffset);
            Assert.AreEqual(45, allPrimaryKeys[0].EndDataFileOffset);
        }

        [Test]
        public void InsertAndGetAllPrimaryKeys_Object()
        {
            _fileSystem.Reset();
            var primaryKeyFile = new PrimaryKeyFile("", typeof(TestEntity));
            var obj = new TestEntity { Int = 123, Float = 4.56f, String = "123" };
            primaryKeyFile.Insert(new PrimaryKey(obj, 0, 45));

            var allPrimaryKeys = primaryKeyFile.GetAllPrimaryKeys().ToList();
            Assert.AreEqual(1, allPrimaryKeys.Count);
            var resultObj = (TestEntity)allPrimaryKeys[0].Value;
            Assert.AreEqual(123, resultObj.Int);
            Assert.AreEqual(4.56f, resultObj.Float);
            Assert.AreEqual("123", resultObj.String);
            Assert.AreEqual(0, allPrimaryKeys[0].StartDataFileOffset);
            Assert.AreEqual(45, allPrimaryKeys[0].EndDataFileOffset);
        }

        class TestEntity
        {
            public int Int { get; set; }

            public float Float { get; set; }

            public string String { get; set; }
        }
    }
}
