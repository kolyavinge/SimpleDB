using System.Linq;
using NUnit.Framework;
using SimpleDB.Core;
using SimpleDB.Infrastructure;
using SimpleDB.Queries;
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
        public void UpdateAndGet()
        {
            _collection.Insert(_entity);
            _entity.Byte = 10;
            _entity.Float = 60.7f;
            _collection.Update(_entity);

            var result = _collection.Get(123);
            Assert.AreEqual((byte)10, result.Byte);
            Assert.AreEqual(60.7f, result.Float);
        }

        [Test]
        public void Delete()
        {
            _collection.Insert(_entity);
            _collection.Delete(123);

            Assert.IsNull(_collection.Get(123));
        }

        [Test]
        public void ExecuteQuery_Equals_WithoutPrimary()
        {
            _collection.Insert(new TestEntity { Id = 1, Byte = 10, Float = 1.2f });
            _collection.Insert(new TestEntity { Id = 2, Byte = 20, Float = 3.4f });
            _collection.Insert(new TestEntity { Id = 3, Byte = 30, Float = 5.6f });
            var query = new Query(
                new SelectClause(new[] { new SelectClause.Field(0) }),
                new WhereClause(new WhereClause.EqualsOperation(new WhereClause.Field(1), new WhereClause.Constant(3.4f))));

            var result = _collection.ExecuteQuery(query).ToList();

            Assert.AreEqual(1, result.Count);
            Assert.AreEqual(20, result[0].Byte);
            Assert.AreEqual(0, result[0].Id); // not select
            Assert.AreEqual(0.0f, result[0].Float); // not select
        }

        [Test]
        public void ExecuteQuery_Equals_WithPrimary()
        {
            _collection.Insert(new TestEntity { Id = 1, Byte = 10, Float = 1.2f });
            _collection.Insert(new TestEntity { Id = 2, Byte = 20, Float = 3.4f });
            _collection.Insert(new TestEntity { Id = 3, Byte = 30, Float = 5.6f });
            var query = new Query(
                new SelectClause(new SelectClause.SelectClauseItem[] { new SelectClause.PrimaryKey(), new SelectClause.Field(0) }),
                new WhereClause(new WhereClause.EqualsOperation(new WhereClause.Field(1), new WhereClause.Constant(3.4f))));

            var result = _collection.ExecuteQuery(query).ToList();

            Assert.AreEqual(1, result.Count);
            Assert.AreEqual(2, result[0].Id);
            Assert.AreEqual(20, result[0].Byte);
            Assert.AreEqual(0.0f, result[0].Float); // not select
        }

        [Test]
        public void ExecuteQuery_Less()
        {
            _collection.Insert(new TestEntity { Id = 1, Byte = 10, Float = 1.2f });
            _collection.Insert(new TestEntity { Id = 2, Byte = 20, Float = 3.4f });
            _collection.Insert(new TestEntity { Id = 3, Byte = 30, Float = 5.6f });
            var query = new Query(
                new SelectClause(new[] { new SelectClause.Field(0) }),
                new WhereClause(new WhereClause.LessOperation(new WhereClause.Field(0), new WhereClause.Constant((byte)20))));

            var result = _collection.ExecuteQuery(query).ToList();

            Assert.AreEqual(1, result.Count);
            Assert.AreEqual(10, result[0].Byte);
        }

        [Test]
        public void ExecuteQuery_Great()
        {
            _collection.Insert(new TestEntity { Id = 1, Byte = 10, Float = 1.2f });
            _collection.Insert(new TestEntity { Id = 2, Byte = 20, Float = 3.4f });
            _collection.Insert(new TestEntity { Id = 3, Byte = 30, Float = 5.6f });
            var query = new Query(
                new SelectClause(new[] { new SelectClause.Field(0) }),
                new WhereClause(new WhereClause.GreatOperation(new WhereClause.Field(0), new WhereClause.Constant((byte)10))));

            var result = _collection.ExecuteQuery(query).ToList();

            Assert.AreEqual(2, result.Count);
            Assert.AreEqual(20, result[0].Byte);
            Assert.AreEqual(30, result[1].Byte);
        }

        [Test]
        public void ExecuteQuery_LessOrEquals()
        {
            _collection.Insert(new TestEntity { Id = 1, Byte = 10, Float = 1.2f });
            _collection.Insert(new TestEntity { Id = 2, Byte = 20, Float = 3.4f });
            _collection.Insert(new TestEntity { Id = 3, Byte = 30, Float = 5.6f });
            var query = new Query(
                new SelectClause(new[] { new SelectClause.Field(0) }),
                new WhereClause(new WhereClause.LessOrEqualsOperation(new WhereClause.Field(0), new WhereClause.Constant((byte)20))));

            var result = _collection.ExecuteQuery(query).ToList();

            Assert.AreEqual(2, result.Count);
            Assert.AreEqual(10, result[0].Byte);
            Assert.AreEqual(20, result[1].Byte);
        }

        [Test]
        public void ExecuteQuery_GreatOrEquals()
        {
            _collection.Insert(new TestEntity { Id = 1, Byte = 10, Float = 1.2f });
            _collection.Insert(new TestEntity { Id = 2, Byte = 20, Float = 3.4f });
            _collection.Insert(new TestEntity { Id = 3, Byte = 30, Float = 5.6f });
            var query = new Query(
                new SelectClause(new[] { new SelectClause.Field(0) }),
                new WhereClause(new WhereClause.GreatOrEqualsOperation(new WhereClause.Field(0), new WhereClause.Constant((byte)20))));

            var result = _collection.ExecuteQuery(query).ToList();

            Assert.AreEqual(2, result.Count);
            Assert.AreEqual(20, result[0].Byte);
            Assert.AreEqual(30, result[1].Byte);
        }

        [Test]
        public void ExecuteQuery_Not()
        {
            _collection.Insert(new TestEntity { Id = 1, Byte = 10, Float = 1.2f });
            _collection.Insert(new TestEntity { Id = 2, Byte = 20, Float = 3.4f });
            _collection.Insert(new TestEntity { Id = 3, Byte = 30, Float = 5.6f });
            var query = new Query(
                new SelectClause(new[] { new SelectClause.Field(0) }),
                new WhereClause(
                    new WhereClause.NotOperation(
                        new WhereClause.EqualsOperation(new WhereClause.Field(0), new WhereClause.Constant((byte)20)))));

            var result = _collection.ExecuteQuery(query).ToList();

            Assert.AreEqual(2, result.Count);
            Assert.AreEqual(10, result[0].Byte);
            Assert.AreEqual(30, result[1].Byte);
        }

        [Test]
        public void ExecuteQuery_Like()
        {
            _collection.Insert(new TestEntity { Id = 1, String = "12345" });
            _collection.Insert(new TestEntity { Id = 2, String = "12" });
            _collection.Insert(new TestEntity { Id = 3, String = "123" });
            var query = new Query(
                new SelectClause(new[] { new SelectClause.Field(2) }),
                new WhereClause(new WhereClause.LikeOperation(new WhereClause.Field(2), new WhereClause.Constant("123"))));

            var result = _collection.ExecuteQuery(query).ToList();

            Assert.AreEqual(2, result.Count);
            Assert.AreEqual("12345", result[0].String);
            Assert.AreEqual("123", result[1].String);
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
