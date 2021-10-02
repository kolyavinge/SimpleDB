using NUnit.Framework;
using SimpleDB.Core;
using SimpleDB.Infrastructure;
using SimpleDB.Queries;
using SimpleDB.QueryExecutors;
using SimpleDB.Test.Tools;

namespace SimpleDB.Test.QueryExecutors
{
    class MergeQueryExecutorTest
    {
        private Mapper<TestEntity> _mapper;
        private Collection<TestEntity> _collection;
        private MergeQueryExecutor<TestEntity> _queryExecutor;

        [SetUp]
        public void Setup()
        {
            GlobalSettings.WorkingDirectory = "working directory";
            IOC.Reset();
            IOC.Set<IMemory>(new Memory());
            IOC.Set<IFileSystem>(new MemoryFileSystem());
            _mapper = new Mapper<TestEntity>(
                "testEntity",
                new PrimaryKeyMapping<TestEntity>(entity => entity.Id),
                new FieldMapping<TestEntity>[]
                {
                    new FieldMapping<TestEntity>(0, entity => entity.Byte),
                    new FieldMapping<TestEntity>(1, entity => entity.Float),
                    new FieldMapping<TestEntity>(2, entity => entity.String)
                });
            _collection = new Collection<TestEntity>(_mapper);
            _queryExecutor = new MergeQueryExecutor<TestEntity>(_mapper, _collection.PrimaryKeyFile, _collection.DataFile, _collection.PrimaryKeys);
        }

        [Test]
        public void ExecuteQuery()
        {
            _collection.Insert(new TestEntity { Id = 1, Byte = 10, Float = 1.2f });
            _collection.Insert(new TestEntity { Id = 2, Byte = 20, Float = 3.4f });
            _collection.Insert(new TestEntity { Id = 3, Byte = 30, Float = 5.6f });
            var newEntities = new[]
            {
                new TestEntity { Id = 10, Byte = 10, Float = 1.2f },
                new TestEntity { Id = 11, Byte = 20, Float = 3.4f },
                new TestEntity { Id = 12, Byte = 20, Float = 9.9f }
            };
            var query = new MergeQuery<TestEntity>(
                new MergeClause(new[] { new MergeClause.MergeClauseItem(0), new MergeClause.MergeClauseItem(1) }),
                newEntities);

            var result = _queryExecutor.ExecuteQuery(query);

            Assert.AreEqual(1, result.NewItems.Count);
            Assert.AreEqual(4, _collection.Count());
            Assert.True(_collection.Exist(1));
            Assert.True(_collection.Exist(2));
            Assert.True(_collection.Exist(3));
            var entity = _collection.Get(12);
            Assert.AreEqual(20, entity.Byte);
            Assert.AreEqual(9.9f, entity.Float);
        }

        [Test]
        public void ExecuteQuery_NoNew()
        {
            _collection.Insert(new TestEntity { Id = 1, Byte = 10, Float = 1.2f });
            _collection.Insert(new TestEntity { Id = 2, Byte = 20, Float = 3.4f });
            _collection.Insert(new TestEntity { Id = 3, Byte = 30, Float = 5.6f });
            var newEntities = new[]
            {
                new TestEntity { Id = 10, Byte = 10, Float = 1.2f },
                new TestEntity { Id = 11, Byte = 20, Float = 3.4f },
                new TestEntity { Id = 12, Byte = 30, Float = 5.6f }
            };
            var query = new MergeQuery<TestEntity>(
                new MergeClause(new[] { new MergeClause.MergeClauseItem(0), new MergeClause.MergeClauseItem(1) }),
                newEntities);

            var result = _queryExecutor.ExecuteQuery(query);

            Assert.AreEqual(0, result.NewItems.Count);
            Assert.AreEqual(3, _collection.Count());
            Assert.True(_collection.Exist(1));
            Assert.True(_collection.Exist(2));
            Assert.True(_collection.Exist(3));
        }

        [Test]
        public void ExecuteQuery_AllNew()
        {
            _collection.Insert(new TestEntity { Id = 1, Byte = 10, Float = 1.2f });
            _collection.Insert(new TestEntity { Id = 2, Byte = 20, Float = 3.4f });
            _collection.Insert(new TestEntity { Id = 3, Byte = 30, Float = 5.6f });
            var newEntities = new[]
            {
                new TestEntity { Id = 10, Byte = 10, Float = 0.2f },
                new TestEntity { Id = 11, Byte = 20, Float = 0.4f },
                new TestEntity { Id = 12, Byte = 30, Float = 0.6f }
            };
            var query = new MergeQuery<TestEntity>(
                new MergeClause(new[] { new MergeClause.MergeClauseItem(0), new MergeClause.MergeClauseItem(1) }),
                newEntities);

            var result = _queryExecutor.ExecuteQuery(query);

            Assert.AreEqual(3, result.NewItems.Count);
            Assert.AreEqual(6, _collection.Count());
            Assert.True(_collection.Exist(1));
            Assert.True(_collection.Exist(2));
            Assert.True(_collection.Exist(3));
            Assert.True(_collection.Exist(10));
            Assert.True(_collection.Exist(11));
            Assert.True(_collection.Exist(12));
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
