using NUnit.Framework;
using SimpleDB.Core;
using SimpleDB.Infrastructure;
using SimpleDB.Test.Tools;

namespace SimpleDB.Test.Core
{
    class CollectionTest
    {
        [SetUp]
        public void Setup()
        {
            IOC.Reset();
            IOC.Set<IMemory>(new Memory());
            IOC.Set<IFileSystem>(new MemoryFileSystem());
        }

        [Test]
        public void InsertAndGet()
        {
            IOC.Reset();
            IOC.Set<IMemory>(new Memory());
            var fileSystem = new MemoryFileSystem();
            IOC.Set<IFileSystem>(fileSystem);
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

            Assert.True(fileSystem.FullFilePathes.Contains(@"working directory\testEntity.primary"));
            Assert.True(fileSystem.FullFilePathes.Contains(@"working directory\testEntity.data"));

            collection.Insert(entity);

            var result = collection.Get(123);
            Assert.AreEqual(123, result.Id);
            Assert.AreEqual((byte)45, result.Byte);
            Assert.AreEqual(6.7f, result.Float);
        }

        [Test]
        public void UpdateAndGet()
        {
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

            collection.Insert(entity);
            entity.Byte = 10;
            entity.Float = 60.7f;
            collection.Update(entity);

            var result = collection.Get(123);
            Assert.AreEqual((byte)10, result.Byte);
            Assert.AreEqual(60.7f, result.Float);
        }

        [Test]
        public void Delete()
        {
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

            collection.Insert(entity);
            collection.Delete(123);

            Assert.IsNull(collection.Get(123));
        }

        class TestEntity
        {
            public int Id { get; set; }

            public byte Byte { get; set; }

            public float Float { get; set; }
        }
    }
}
