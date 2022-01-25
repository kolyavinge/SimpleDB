﻿using NUnit.Framework;
using SimpleDB.Core;
using SimpleDB.IndexedSearch;
using SimpleDB.Infrastructure;
using SimpleDB.Queries;
using SimpleDB.QueryExecutors;
using SimpleDB.Test.Tools;

namespace SimpleDB.Test.QueryExecutors
{
    class DeleteQueryExecutorTest
    {
        private Collection<TestEntity> _collection;
        private DeleteQueryExecutor _queryExecutor;

        [SetUp]
        public void Setup()
        {
            var fileSystem = new MemoryFileSystem();
            var memory = Memory.Instance;
            var mapper = new Mapper<TestEntity>(
                new PrimaryKeyMapping<TestEntity>(x => x.Id),
                new FieldMapping<TestEntity>[]
                {
                    new FieldMapping<TestEntity>(0, x => x.Byte),
                    new FieldMapping<TestEntity>(1, x => x.Float),
                    new FieldMapping<TestEntity>(2, x => x.String)
                });
            _collection = new Collection<TestEntity>(
                mapper,
                new PrimaryKeyFileFactory(fileSystem, memory),
                new DataFileFactory(fileSystem, memory),
                new MetaFileFactory(fileSystem));
            _queryExecutor = new DeleteQueryExecutor(_collection.PrimaryKeyFile, _collection.DataFile, _collection.PrimaryKeys, new IndexHolder(), new IndexUpdater(mapper));
        }

        [Test]
        public void ExecuteQuery_All()
        {
            _collection.Insert(new TestEntity { Id = 1, Byte = 10, Float = 1.2f, String = "123" });
            _collection.Insert(new TestEntity { Id = 2, Byte = 20, Float = 3.4f, String = "456" });
            _collection.Insert(new TestEntity { Id = 3, Byte = 30, Float = 5.6f, String = "789" });
            var query = new DeleteQuery("TestEntity");

            var result = _queryExecutor.ExecuteQuery(query);

            Assert.AreEqual(3, result);

            Assert.False(_collection.Exist(1));
            Assert.False(_collection.Exist(2));
            Assert.False(_collection.Exist(3));
        }

        [Test]
        public void ExecuteQuery_Where()
        {
            _collection.Insert(new TestEntity { Id = 1, Byte = 10, Float = 1.2f, String = "123" });
            _collection.Insert(new TestEntity { Id = 2, Byte = 20, Float = 3.4f, String = "456" });
            _collection.Insert(new TestEntity { Id = 3, Byte = 30, Float = 5.6f, String = "789" });
            var query = new DeleteQuery("TestEntity")
            {
                WhereClause = new WhereClause(new WhereClause.EqualsOperation(new WhereClause.Field(0), new WhereClause.Constant((byte)10)))
            };

            var result = _queryExecutor.ExecuteQuery(query);

            Assert.AreEqual(1, result);

            Assert.False(_collection.Exist(1));
            Assert.True(_collection.Exist(2));
            Assert.True(_collection.Exist(3));
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
