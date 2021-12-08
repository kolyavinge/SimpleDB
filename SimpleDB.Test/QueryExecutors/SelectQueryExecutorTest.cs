using System.Linq;
using NUnit.Framework;
using SimpleDB.Core;
using SimpleDB.Infrastructure;
using SimpleDB.Queries;
using SimpleDB.QueryExecutors;
using SimpleDB.Test.Tools;

namespace SimpleDB.Test.QueryExecutors
{
    class SelectQueryExecutorTest
    {
        private readonly string _workingDirectory = "working directory";
        private Mapper<TestEntity> _mapper;
        private Collection<TestEntity> _collection;
        private SelectQueryExecutor<TestEntity> _queryExecutor;

        [SetUp]
        public void Setup()
        {
            var fileSystem = new MemoryFileSystem();
            var memory = Memory.Instance;
            _mapper = new Mapper<TestEntity>(
                "testEntity",
                new PrimaryKeyMapping<TestEntity>(entity => entity.Id),
                new FieldMapping<TestEntity>[]
                {
                    new FieldMapping<TestEntity>(0, entity => entity.Byte),
                    new FieldMapping<TestEntity>(1, entity => entity.Float),
                    new FieldMapping<TestEntity>(2, entity => entity.String)
                });
            _collection = new Collection<TestEntity>(
                _mapper,
                new PrimaryKeyFileFactory(_workingDirectory, fileSystem, memory),
                new DataFileFactory(_workingDirectory, fileSystem, memory),
                new MetaFileFactory(_workingDirectory, fileSystem));
            _queryExecutor = new SelectQueryExecutor<TestEntity>(_mapper, _collection.DataFile, _collection.PrimaryKeys);
        }

        [Test]
        public void ExecuteQuery_Equals_WithoutPrimary()
        {
            _collection.Insert(new TestEntity { Id = 1, Byte = 10, Float = 1.2f });
            _collection.Insert(new TestEntity { Id = 2, Byte = 20, Float = 3.4f });
            _collection.Insert(new TestEntity { Id = 3, Byte = 30, Float = 5.6f });
            var query = new SelectQuery(typeof(TestEntity), new SelectClause(new[] { new SelectClause.Field(0) }))
            {
                WhereClause = new WhereClause(new WhereClause.EqualsOperation(new WhereClause.Field(1), new WhereClause.Constant(3.4f)))
            };

            var result = _queryExecutor.ExecuteQuery(query).Items.ToList();

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
            var query = new SelectQuery(typeof(TestEntity), new SelectClause(new SelectClause.SelectClauseItem[] { new SelectClause.PrimaryKey(), new SelectClause.Field(0) }))
            {
                WhereClause = new WhereClause(new WhereClause.EqualsOperation(new WhereClause.Field(1), new WhereClause.Constant(3.4f)))
            };

            var result = _queryExecutor.ExecuteQuery(query).Items.ToList();

            Assert.AreEqual(1, result.Count);
            Assert.AreEqual(2, result[0].Id);
            Assert.AreEqual(20, result[0].Byte);
            Assert.AreEqual(0.0f, result[0].Float); // not select
        }

        [Test]
        public void ExecuteQuery_Equals_Primary()
        {
            _collection.Insert(new TestEntity { Id = 1, Byte = 10, Float = 1.2f });
            _collection.Insert(new TestEntity { Id = 2, Byte = 20, Float = 3.4f });
            _collection.Insert(new TestEntity { Id = 3, Byte = 30, Float = 5.6f });
            var query = new SelectQuery(typeof(TestEntity), new SelectClause(new SelectClause.SelectClauseItem[] { new SelectClause.PrimaryKey(), new SelectClause.Field(0) }))
            {
                WhereClause = new WhereClause(new WhereClause.EqualsOperation(new WhereClause.PrimaryKey(), new WhereClause.Constant(2)))
            };

            var result = _queryExecutor.ExecuteQuery(query).Items.ToList();

            Assert.AreEqual(1, result.Count);
            Assert.AreEqual(2, result[0].Id);
            Assert.AreEqual(20, result[0].Byte);
        }

        [Test]
        public void ExecuteQuery_And()
        {
            _collection.Insert(new TestEntity { Id = 1, Byte = 10, Float = 1.2f });
            _collection.Insert(new TestEntity { Id = 2, Byte = 20, Float = 3.4f });
            _collection.Insert(new TestEntity { Id = 3, Byte = 30, Float = 5.6f });
            var query = new SelectQuery(typeof(TestEntity), new SelectClause(new SelectClause.SelectClauseItem[] { new SelectClause.PrimaryKey(), new SelectClause.Field(0) }))
            {
                WhereClause = new WhereClause(
                    new WhereClause.AndOperation(
                        new WhereClause.EqualsOperation(new WhereClause.PrimaryKey(), new WhereClause.Constant(2)),
                        new WhereClause.EqualsOperation(new WhereClause.Field(0), new WhereClause.Constant((byte)20))))
            };

            var result = _queryExecutor.ExecuteQuery(query).Items.ToList();

            Assert.AreEqual(1, result.Count);
            Assert.AreEqual(2, result[0].Id);
            Assert.AreEqual(20, result[0].Byte);
        }

        [Test]
        public void ExecuteQuery_Or()
        {
            _collection.Insert(new TestEntity { Id = 1, Byte = 10, Float = 1.2f });
            _collection.Insert(new TestEntity { Id = 2, Byte = 20, Float = 3.4f });
            _collection.Insert(new TestEntity { Id = 3, Byte = 30, Float = 5.6f });
            var query = new SelectQuery(typeof(TestEntity), new SelectClause(new SelectClause.SelectClauseItem[] { new SelectClause.PrimaryKey(), new SelectClause.Field(0) }))
            {
                WhereClause = new WhereClause(
                    new WhereClause.OrOperation(
                        new WhereClause.EqualsOperation(new WhereClause.PrimaryKey(), new WhereClause.Constant(1)),
                        new WhereClause.EqualsOperation(new WhereClause.PrimaryKey(), new WhereClause.Constant(2))))
            };

            var result = _queryExecutor.ExecuteQuery(query).Items.ToList();

            Assert.AreEqual(2, result.Count);
            Assert.AreEqual(1, result[0].Id);
            Assert.AreEqual(2, result[1].Id);
        }

        [Test]
        public void ExecuteQuery_Less()
        {
            _collection.Insert(new TestEntity { Id = 1, Byte = 10, Float = 1.2f });
            _collection.Insert(new TestEntity { Id = 2, Byte = 20, Float = 3.4f });
            _collection.Insert(new TestEntity { Id = 3, Byte = 30, Float = 5.6f });
            var query = new SelectQuery(typeof(TestEntity), new SelectClause(new[] { new SelectClause.Field(0) }))
            {
                WhereClause = new WhereClause(new WhereClause.LessOperation(new WhereClause.Field(0), new WhereClause.Constant((byte)20)))
            };

            var result = _queryExecutor.ExecuteQuery(query).Items.ToList();

            Assert.AreEqual(1, result.Count);
            Assert.AreEqual(10, result[0].Byte);
        }

        [Test]
        public void ExecuteQuery_Great()
        {
            _collection.Insert(new TestEntity { Id = 1, Byte = 10, Float = 1.2f });
            _collection.Insert(new TestEntity { Id = 2, Byte = 20, Float = 3.4f });
            _collection.Insert(new TestEntity { Id = 3, Byte = 30, Float = 5.6f });
            var query = new SelectQuery(typeof(TestEntity), new SelectClause(new[] { new SelectClause.Field(0) }))
            {
                WhereClause = new WhereClause(new WhereClause.GreatOperation(new WhereClause.Field(0), new WhereClause.Constant((byte)10)))
            };

            var result = _queryExecutor.ExecuteQuery(query).Items.ToList();

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
            var query = new SelectQuery(typeof(TestEntity), new SelectClause(new[] { new SelectClause.Field(0) }))
            {
                WhereClause = new WhereClause(new WhereClause.LessOrEqualsOperation(new WhereClause.Field(0), new WhereClause.Constant((byte)20)))
            };

            var result = _queryExecutor.ExecuteQuery(query).Items.ToList();

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
            var query = new SelectQuery(typeof(TestEntity), new SelectClause(new[] { new SelectClause.Field(0) }))
            {
                WhereClause = new WhereClause(new WhereClause.GreatOrEqualsOperation(new WhereClause.Field(0), new WhereClause.Constant((byte)20)))
            };

            var result = _queryExecutor.ExecuteQuery(query).Items.ToList();

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
            var query = new SelectQuery(typeof(TestEntity), new SelectClause(new[] { new SelectClause.Field(0) }))
            {
                WhereClause =
                    new WhereClause(
                        new WhereClause.NotOperation(
                            new WhereClause.EqualsOperation(new WhereClause.Field(0), new WhereClause.Constant((byte)20))))
            };

            var result = _queryExecutor.ExecuteQuery(query).Items.ToList();

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
            var query = new SelectQuery(typeof(TestEntity), new SelectClause(new[] { new SelectClause.Field(2) }))
            {
                WhereClause = new WhereClause(new WhereClause.LikeOperation(new WhereClause.Field(2), new WhereClause.Constant("123")))
            };

            var result = _queryExecutor.ExecuteQuery(query).Items.ToList();

            Assert.AreEqual(2, result.Count);
            Assert.AreEqual("12345", result[0].String);
            Assert.AreEqual("123", result[1].String);
        }

        [Test]
        public void ExecuteQuery_In()
        {
            _collection.Insert(new TestEntity { Id = 1, String = "12345" });
            _collection.Insert(new TestEntity { Id = 2, String = "12" });
            _collection.Insert(new TestEntity { Id = 3, String = "123" });
            var query = new SelectQuery(typeof(TestEntity), new SelectClause(new[] { new SelectClause.Field(2) }))
            {
                WhereClause = new WhereClause(new WhereClause.InOperation(new WhereClause.Field(2), new WhereClause.Set(new[] { "123", "12" })))
            };

            var result = _queryExecutor.ExecuteQuery(query).Items.ToList();

            Assert.AreEqual(2, result.Count);
            Assert.AreEqual("12", result[0].String);
            Assert.AreEqual("123", result[1].String);
        }

        [Test]
        public void ExecuteQuery_OrderByAsc()
        {
            _collection.Insert(new TestEntity { Id = 1, String = "12345" });
            _collection.Insert(new TestEntity { Id = 2, String = "12" });
            _collection.Insert(new TestEntity { Id = 3, String = "123" });
            var query = new SelectQuery(typeof(TestEntity), new SelectClause(new[] { new SelectClause.Field(2) }))
            {
                OrderByClause = new OrderByClause(new[] { new OrderByClause.Field(2, SortDirection.Asc) })
            };

            var result = _queryExecutor.ExecuteQuery(query).Items.ToList();

            Assert.AreEqual("12", result[0].String);
            Assert.AreEqual("123", result[1].String);
            Assert.AreEqual("12345", result[2].String);
        }

        [Test]
        public void ExecuteQuery_OrderByDesc()
        {
            _collection.Insert(new TestEntity { Id = 1, String = "12345" });
            _collection.Insert(new TestEntity { Id = 2, String = "12" });
            _collection.Insert(new TestEntity { Id = 3, String = "123" });
            var query = new SelectQuery(typeof(TestEntity), new SelectClause(new[] { new SelectClause.Field(2) }))
            {
                OrderByClause = new OrderByClause(new[] { new OrderByClause.Field(2, SortDirection.Desc) })
            };

            var result = _queryExecutor.ExecuteQuery(query).Items.ToList();

            Assert.AreEqual("12345", result[0].String);
            Assert.AreEqual("123", result[1].String);
            Assert.AreEqual("12", result[2].String);
        }

        [Test]
        public void ExecuteQuery_OrderByTwoFields()
        {
            _collection.Insert(new TestEntity { Id = 1, Byte = 10, String = "12345" });
            _collection.Insert(new TestEntity { Id = 2, Byte = 20, String = "12" });
            _collection.Insert(new TestEntity { Id = 3, Byte = 20, String = "123" });
            var query = new SelectQuery(typeof(TestEntity), new SelectClause(new[] { new SelectClause.Field(2) }))
            {
                OrderByClause = new OrderByClause(new[] { new OrderByClause.Field(0, SortDirection.Asc), new OrderByClause.Field(2, SortDirection.Asc) })
            };

            var result = _queryExecutor.ExecuteQuery(query).Items.ToList();

            Assert.AreEqual("12345", result[0].String);
            Assert.AreEqual("12", result[1].String);
            Assert.AreEqual("123", result[2].String);
        }

        [Test]
        public void ExecuteQuery_OrderByTwoFieldsDesc()
        {
            _collection.Insert(new TestEntity { Id = 1, Byte = 10, String = "12345" });
            _collection.Insert(new TestEntity { Id = 2, Byte = 10, String = "12" });
            _collection.Insert(new TestEntity { Id = 3, Byte = 10, String = "123" });
            var query = new SelectQuery(typeof(TestEntity), new SelectClause(new[] { new SelectClause.Field(2) }))
            {
                OrderByClause = new OrderByClause(new[] { new OrderByClause.Field(0, SortDirection.Asc), new OrderByClause.Field(2, SortDirection.Desc) })
            };

            var result = _queryExecutor.ExecuteQuery(query).Items.ToList();

            Assert.AreEqual("12345", result[0].String);
            Assert.AreEqual("123", result[1].String);
            Assert.AreEqual("12", result[2].String);
        }

        [Test]
        public void ExecuteQuery_OrderBy_PrimaryKey_Asc()
        {
            _collection.Insert(new TestEntity { Id = 1, Byte = 10, String = "12345" });
            _collection.Insert(new TestEntity { Id = 2, Byte = 10, String = "12" });
            _collection.Insert(new TestEntity { Id = 3, Byte = 10, String = "123" });
            var query = new SelectQuery(typeof(TestEntity), new SelectClause(new[] { new SelectClause.Field(2) }))
            {
                OrderByClause = new OrderByClause(new[] { new OrderByClause.PrimaryKey(SortDirection.Asc) })
            };

            var result = _queryExecutor.ExecuteQuery(query).Items.ToList();

            Assert.AreEqual("12345", result[0].String);
            Assert.AreEqual("12", result[1].String);
            Assert.AreEqual("123", result[2].String);
        }

        [Test]
        public void ExecuteQuery_OrderBy_PrimaryKey_Desc()
        {
            _collection.Insert(new TestEntity { Id = 1, Byte = 10, String = "12345" });
            _collection.Insert(new TestEntity { Id = 2, Byte = 10, String = "12" });
            _collection.Insert(new TestEntity { Id = 3, Byte = 10, String = "123" });
            var query = new SelectQuery(typeof(TestEntity), new SelectClause(new[] { new SelectClause.Field(2) }))
            {
                OrderByClause = new OrderByClause(new[] { new OrderByClause.PrimaryKey(SortDirection.Desc) })
            };

            var result = _queryExecutor.ExecuteQuery(query).Items.ToList();

            Assert.AreEqual("123", result[0].String);
            Assert.AreEqual("12", result[1].String);
            Assert.AreEqual("12345", result[2].String);
        }

        [Test]
        public void ExecuteQuery_Skip()
        {
            _collection.Insert(new TestEntity { Id = 1, Byte = 10 });
            _collection.Insert(new TestEntity { Id = 2, Byte = 20 });
            _collection.Insert(new TestEntity { Id = 3, Byte = 30 });
            var query = new SelectQuery(typeof(TestEntity), new SelectClause(new[] { new SelectClause.Field(0) }))
            {
                OrderByClause = new OrderByClause(new[] { new OrderByClause.Field(0, SortDirection.Asc) }),
                Skip = 1
            };

            var result = _queryExecutor.ExecuteQuery(query).Items.ToList();

            Assert.AreEqual(2, result.Count);
            Assert.AreEqual(20, result[0].Byte);
            Assert.AreEqual(30, result[1].Byte);
        }

        [Test]
        public void ExecuteQuery_Skip_Big()
        {
            _collection.Insert(new TestEntity { Id = 1, Byte = 10 });
            _collection.Insert(new TestEntity { Id = 2, Byte = 20 });
            _collection.Insert(new TestEntity { Id = 3, Byte = 30 });
            var query = new SelectQuery(typeof(TestEntity), new SelectClause(new[] { new SelectClause.Field(0) }))
            {
                OrderByClause = new OrderByClause(new[] { new OrderByClause.Field(0, SortDirection.Asc) }),
                Skip = 100
            };

            var result = _queryExecutor.ExecuteQuery(query).Items.ToList();

            Assert.AreEqual(0, result.Count);
        }

        [Test]
        public void ExecuteQuery_Limit()
        {
            _collection.Insert(new TestEntity { Id = 1, Byte = 10 });
            _collection.Insert(new TestEntity { Id = 2, Byte = 20 });
            _collection.Insert(new TestEntity { Id = 3, Byte = 30 });
            var query = new SelectQuery(typeof(TestEntity), new SelectClause(new[] { new SelectClause.Field(0) }))
            {
                OrderByClause = new OrderByClause(new[] { new OrderByClause.Field(0, SortDirection.Asc) }),
                Limit = 2
            };

            var result = _queryExecutor.ExecuteQuery(query).Items.ToList();

            Assert.AreEqual(2, result.Count);
            Assert.AreEqual(10, result[0].Byte);
            Assert.AreEqual(20, result[1].Byte);
        }

        [Test]
        public void ExecuteQuery_CountAggregate()
        {
            _collection.Insert(new TestEntity { Id = 1, Byte = 10 });
            _collection.Insert(new TestEntity { Id = 2, Byte = 20 });
            _collection.Insert(new TestEntity { Id = 3, Byte = 30 });
            var query = new SelectQuery(typeof(TestEntity), new SelectClause(new[] { new SelectClause.CountAggregate() }));

            var result = (int)_queryExecutor.ExecuteQuery(query).Scalar;

            Assert.AreEqual(3, result);
        }

        [Test]
        public void ExecuteQuery_CountAggregate_Where()
        {
            _collection.Insert(new TestEntity { Id = 1, Byte = 10 });
            _collection.Insert(new TestEntity { Id = 2, Byte = 20 });
            _collection.Insert(new TestEntity { Id = 3, Byte = 30 });
            var query = new SelectQuery(typeof(TestEntity), new SelectClause(new[] { new SelectClause.CountAggregate() }))
            {
                WhereClause = new WhereClause(new WhereClause.EqualsOperation(new WhereClause.Field(0), new WhereClause.Constant((byte)10)))
            };

            var result = (int)_queryExecutor.ExecuteQuery(query).Scalar;

            Assert.AreEqual(1, result);
        }

        [Test]
        public void ExecuteQuery_CountAggregate_Skip()
        {
            _collection.Insert(new TestEntity { Id = 1, Byte = 10 });
            _collection.Insert(new TestEntity { Id = 2, Byte = 20 });
            _collection.Insert(new TestEntity { Id = 3, Byte = 30 });
            var query = new SelectQuery(typeof(TestEntity), new SelectClause(new[] { new SelectClause.CountAggregate() }))
            {
                Skip = 1
            };

            var result = (int)_queryExecutor.ExecuteQuery(query).Scalar;

            Assert.AreEqual(2, result);
        }

        [Test]
        public void ExecuteQuery_CountAggregate_SkipLimit()
        {
            _collection.Insert(new TestEntity { Id = 1, Byte = 10 });
            _collection.Insert(new TestEntity { Id = 2, Byte = 20 });
            _collection.Insert(new TestEntity { Id = 3, Byte = 30 });
            var query = new SelectQuery(typeof(TestEntity), new SelectClause(new[] { new SelectClause.CountAggregate() }))
            {
                Skip = 1,
                Limit = 3
            };

            var result = (int)_queryExecutor.ExecuteQuery(query).Scalar;

            Assert.AreEqual(2, result);
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
