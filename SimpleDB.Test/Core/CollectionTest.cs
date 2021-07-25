using NUnit.Framework;
using SimpleDB.Core;
using SimpleDB.Infrastructure;
using SimpleDB.Test.Tools;

namespace SimpleDB.Test.Core
{
    class CollectionTest
    {
        private MemoryFileSystem _fileSystem = new MemoryFileSystem();

        [SetUp]
        public void Setup()
        {
            IOC.Reset();
            IOC.Set<IFileSystem>(_fileSystem);
        }

        [Test]
        public void InsertAndGetById()
        {
            _fileSystem.Reset();
            var entity = new TestEntity { Id = 123, Byte = 45, Float = 6.7f };
            var mapper = new Mapper<TestEntity>(
                "testEntity",
                new PrimaryKeyMapping<TestEntity>(entity => entity.Id),
                new FieldMapping<TestEntity>[]
                {
                    new FieldMapping<TestEntity>(0, entity => entity.Byte),
                    new FieldMapping<TestEntity>(1, entity => entity.Float)
                });
            var collection = new Collection<TestEntity>("working directory", mapper);

            Assert.True(_fileSystem.FullFilePathes.Contains(@"working directory\testEntity.primary"));
            Assert.True(_fileSystem.FullFilePathes.Contains(@"working directory\testEntity.data"));

            collection.Insert(entity);

            var result = collection.GetById(123);
            Assert.AreEqual(123, result.Id);
            Assert.AreEqual((byte)45, result.Byte);
            Assert.AreEqual(6.7f, result.Float);
        }

        class TestEntity
        {
            public int Id { get; set; }

            public byte Byte { get; set; }

            public float Float { get; set; }
        }
    }
}
