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
            Assert.AreEqual(35, primaryKey.EndDataFileOffset);
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
            Assert.AreEqual(20, primaryKey.EndDataFileOffset);
        }

        [Test]
        public void Delete()
        {
            _collection.Insert(_entity);
            _collection.Delete(123);

            Assert.IsNull(_collection.Get(123));
        }

        [Test]
        public void Linq_Count()
        {
            _collection.Insert(new TestEntity { Id = 1, Byte = 10 });
            _collection.Insert(new TestEntity { Id = 2, Byte = 20 });
            _collection.Insert(new TestEntity { Id = 3, Byte = 30 });
            _collection.Insert(new TestEntity { Id = 4, Byte = 40 });
            _collection.Insert(new TestEntity { Id = 5, Byte = 50 });
            var result = _collection.Query().Count();
            Assert.AreEqual(5, result);
        }

        [Test]
        public void Linq_ToList()
        {
            _collection.Insert(new TestEntity { Id = 1, Byte = 10 });
            _collection.Insert(new TestEntity { Id = 2, Byte = 20 });
            _collection.Insert(new TestEntity { Id = 3, Byte = 30 });
            _collection.Insert(new TestEntity { Id = 4, Byte = 40 });
            _collection.Insert(new TestEntity { Id = 5, Byte = 50 });
            var result = _collection.Query().ToList();
            Assert.AreEqual(5, result.Count);
            Assert.AreEqual(1, result[0].Id);
            Assert.AreEqual((byte)10, result[0].Byte);
            Assert.AreEqual(5, result[4].Id);
            Assert.AreEqual((byte)50, result[4].Byte);
        }

        [Test]
        public void Linq_Select()
        {
            _collection.Insert(new TestEntity { Id = 1, Byte = 10 });
            _collection.Insert(new TestEntity { Id = 2, Byte = 20 });
            _collection.Insert(new TestEntity { Id = 3, Byte = 30 });
            _collection.Insert(new TestEntity { Id = 4, Byte = 40 });
            _collection.Insert(new TestEntity { Id = 5, Byte = 50 });
            var result = _collection.Query().Select().ToList();
            Assert.AreEqual(5, result.Count);
            Assert.AreEqual(1, result[0].Id);
            Assert.AreEqual((byte)10, result[0].Byte);
            Assert.AreEqual(5, result[4].Id);
            Assert.AreEqual((byte)50, result[4].Byte);
        }

        [Test]
        public void Linq_SelectFields()
        {
            _collection.Insert(new TestEntity { Id = 1, Byte = 10 });
            _collection.Insert(new TestEntity { Id = 2, Byte = 20 });
            _collection.Insert(new TestEntity { Id = 3, Byte = 30 });
            _collection.Insert(new TestEntity { Id = 4, Byte = 40 });
            _collection.Insert(new TestEntity { Id = 5, Byte = 50 });
            var result = _collection.Query().Select(x => new { x.Id }).ToList();
            Assert.AreEqual(5, result.Count);
            Assert.AreEqual(1, result[0].Id);
            Assert.AreEqual(default(byte), result[0].Byte);
            Assert.AreEqual(5, result[4].Id);
            Assert.AreEqual(default(byte), result[4].Byte);
        }

        [Test]
        public void Linq_Where()
        {
            _collection.Insert(new TestEntity { Id = 1, Byte = 10 });
            _collection.Insert(new TestEntity { Id = 2, Byte = 20 });
            _collection.Insert(new TestEntity { Id = 3, Byte = 30 });
            _collection.Insert(new TestEntity { Id = 4, Byte = 40 });
            _collection.Insert(new TestEntity { Id = 5, Byte = 50 });
            var result = _collection.Query().Where(x => x.Id > 2).ToList();
            Assert.AreEqual(3, result.Count);
            Assert.AreEqual(3, result[0].Id);
            Assert.AreEqual((byte)30, result[0].Byte);
            Assert.AreEqual(4, result[1].Id);
            Assert.AreEqual(5, result[2].Id);
        }

        [Test]
        public void Linq_OrderBy()
        {
            _collection.Insert(new TestEntity { Id = 1, Byte = 10 });
            _collection.Insert(new TestEntity { Id = 2, Byte = 20 });
            _collection.Insert(new TestEntity { Id = 3, Byte = 30 });
            _collection.Insert(new TestEntity { Id = 4, Byte = 40 });
            _collection.Insert(new TestEntity { Id = 5, Byte = 50 });
            var result = _collection.Query().OrderBy(x => x.Byte, SortDirection.Desc).ToList();
            Assert.AreEqual(5, result.Count);
            Assert.AreEqual(5, result[0].Id);
            Assert.AreEqual((byte)50, result[0].Byte);
            Assert.AreEqual(4, result[1].Id);
            Assert.AreEqual(3, result[2].Id);
            Assert.AreEqual(2, result[3].Id);
            Assert.AreEqual(1, result[4].Id);
        }

        [Test]
        public void Linq_Skip()
        {
            _collection.Insert(new TestEntity { Id = 1, Byte = 10 });
            _collection.Insert(new TestEntity { Id = 2, Byte = 20 });
            _collection.Insert(new TestEntity { Id = 3, Byte = 30 });
            _collection.Insert(new TestEntity { Id = 4, Byte = 40 });
            _collection.Insert(new TestEntity { Id = 5, Byte = 50 });
            var result = _collection.Query().OrderBy(x => x.Byte, SortDirection.Desc).Skip(2).ToList();
            Assert.AreEqual(3, result.Count);
            Assert.AreEqual(3, result[0].Id);
            Assert.AreEqual((byte)30, result[0].Byte);
            Assert.AreEqual(2, result[1].Id);
            Assert.AreEqual(1, result[2].Id);
        }

        [Test]
        public void Linq_Limit()
        {
            _collection.Insert(new TestEntity { Id = 1, Byte = 10 });
            _collection.Insert(new TestEntity { Id = 2, Byte = 20 });
            _collection.Insert(new TestEntity { Id = 3, Byte = 30 });
            _collection.Insert(new TestEntity { Id = 4, Byte = 40 });
            _collection.Insert(new TestEntity { Id = 5, Byte = 50 });
            var result = _collection.Query().OrderBy(x => x.Byte, SortDirection.Desc).Limit(2).ToList();
            Assert.AreEqual(2, result.Count);
            Assert.AreEqual(5, result[0].Id);
            Assert.AreEqual((byte)50, result[0].Byte);
            Assert.AreEqual(4, result[1].Id);
        }

        [Test]
        public void Linq_WhereConvertByte_Equal()
        {
            _collection.Insert(new TestEntity { Id = 1, Byte = 10 });
            _collection.Insert(new TestEntity { Id = 2, Byte = 20 });
            _collection.Insert(new TestEntity { Id = 3, Byte = 30 });
            _collection.Insert(new TestEntity { Id = 4, Byte = 40 });
            _collection.Insert(new TestEntity { Id = 5, Byte = 50 });
            var result = _collection.Query().Where(x => x.Byte == 20).ToList();
            Assert.AreEqual(1, result.Count);
        }

        [Test]
        public void Linq_WhereConvertByte_Less()
        {
            _collection.Insert(new TestEntity { Id = 1, Byte = 10 });
            _collection.Insert(new TestEntity { Id = 2, Byte = 20 });
            _collection.Insert(new TestEntity { Id = 3, Byte = 30 });
            _collection.Insert(new TestEntity { Id = 4, Byte = 40 });
            _collection.Insert(new TestEntity { Id = 5, Byte = 50 });
            var result = _collection.Query().Where(x => x.Byte < 20).ToList();
            Assert.AreEqual(1, result.Count);
        }

        [Test]
        public void Linq_WhereConvertByte_LessEqual()
        {
            _collection.Insert(new TestEntity { Id = 1, Byte = 10 });
            _collection.Insert(new TestEntity { Id = 2, Byte = 20 });
            _collection.Insert(new TestEntity { Id = 3, Byte = 30 });
            _collection.Insert(new TestEntity { Id = 4, Byte = 40 });
            _collection.Insert(new TestEntity { Id = 5, Byte = 50 });
            var result = _collection.Query().Where(x => x.Byte <= 20).ToList();
            Assert.AreEqual(2, result.Count);
        }

        [Test]
        public void Linq_WhereConvertByte_Great()
        {
            _collection.Insert(new TestEntity { Id = 1, Byte = 10 });
            _collection.Insert(new TestEntity { Id = 2, Byte = 20 });
            _collection.Insert(new TestEntity { Id = 3, Byte = 30 });
            _collection.Insert(new TestEntity { Id = 4, Byte = 40 });
            _collection.Insert(new TestEntity { Id = 5, Byte = 50 });
            var result = _collection.Query().Where(x => x.Byte > 20).ToList();
            Assert.AreEqual(3, result.Count);
        }

        [Test]
        public void Linq_WhereConvertByte_GreatEqual()
        {
            _collection.Insert(new TestEntity { Id = 1, Byte = 10 });
            _collection.Insert(new TestEntity { Id = 2, Byte = 20 });
            _collection.Insert(new TestEntity { Id = 3, Byte = 30 });
            _collection.Insert(new TestEntity { Id = 4, Byte = 40 });
            _collection.Insert(new TestEntity { Id = 5, Byte = 50 });
            var result = _collection.Query().Where(x => x.Byte >= 20).ToList();
            Assert.AreEqual(4, result.Count);
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
