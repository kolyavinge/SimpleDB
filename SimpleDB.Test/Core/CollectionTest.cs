using NUnit.Framework;
using SimpleDB.Core;
using SimpleDB.Infrastructure;
using SimpleDB.Test.Tools;

namespace SimpleDB.Test.Core
{
    class CollectionTest
    {
        private Mapper<TestEntity> _mapper;
        private TestEntity _entity;
        private Collection<TestEntity> _collection;

        [SetUp]
        public void Setup()
        {
            IOC.Reset();
            IOC.Set<IMemory>(new Memory());
            IOC.Set<IFileSystem>(new MemoryFileSystem());
            _entity = new TestEntity { Id = 123, Byte = 45, Float = 6.7f };
            _mapper = new Mapper<TestEntity>(
                "testEntity",
                new PrimaryKeyMapping<TestEntity>(entity => entity.Id),
                new FieldMapping<TestEntity>[]
                {
                    new FieldMapping<TestEntity>(0, entity => entity.Byte),
                    new FieldMapping<TestEntity>(1, entity => entity.Float),
                    new FieldMapping<TestEntity>(2, entity => entity.String)
                });
            _collection = new Collection<TestEntity>("working directory", _mapper);
        }

        [Test]
        public void InsertAndGet()
        {
            _collection.Insert(_entity);

            var result = _collection.Get(123);
            Assert.AreEqual(123, result.Id);
            Assert.AreEqual((byte)45, result.Byte);
            Assert.AreEqual(6.7f, result.Float);
        }

        [Test]
        public void UpdateAndGet_StartDataFileOffsetNotChange()
        {
            _collection.Insert(_entity);
            _entity.Byte = 10;
            _entity.Float = 60.7f;
            _collection.Update(_entity);

            var result = _collection.Get(123);
            Assert.AreEqual((byte)10, result.Byte);
            Assert.AreEqual(60.7f, result.Float);
            var primaryKey = _collection.PrimaryKeys[123];
            Assert.AreEqual(0, primaryKey.PrimaryKeyFileOffset);
            Assert.AreEqual(0, primaryKey.StartDataFileOffset);
            Assert.AreEqual(15, primaryKey.EndDataFileOffset);
        }

        [Test]
        public void UpdateAndGet_StartDataFileOffsetChange()
        {
            _collection.Insert(_entity);
            _entity.String = "12345";
            _collection.Update(_entity);

            var result = _collection.Get(123);
            Assert.AreEqual("12345", result.String);
            var primaryKey = _collection.PrimaryKeys[123];
            Assert.AreEqual(0, primaryKey.PrimaryKeyFileOffset);
            Assert.AreEqual(15, primaryKey.StartDataFileOffset);
            Assert.AreEqual(36, primaryKey.EndDataFileOffset);
        }

        [Test]
        public void UpdateAndGet_EndDataFileOffsetChange()
        {
            _entity.String = "1234567890";
            _collection.Insert(_entity);
            _entity.String = "12345";
            _collection.Update(_entity);

            var result = _collection.Get(123);
            Assert.AreEqual("12345", result.String);
            var primaryKey = _collection.PrimaryKeys[123];
            Assert.AreEqual(0, primaryKey.PrimaryKeyFileOffset);
            Assert.AreEqual(0, primaryKey.StartDataFileOffset);
            Assert.AreEqual(21, primaryKey.EndDataFileOffset);
        }

        [Test]
        public void Delete()
        {
            _collection.Insert(_entity);
            _collection.Delete(123);

            Assert.IsNull(_collection.Get(123));
        }

        class TestEntity
        {
            public int Id { get; set; }

            public byte Byte { get; set; }

            public float Float { get; set; }

            public string String { get; set; }
        }
    }
}
