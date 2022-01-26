﻿using System.Collections.Generic;
using NUnit.Framework;
using SimpleDB.Core;
using SimpleDB.IndexedSearch;
using SimpleDB.Infrastructure;
using SimpleDB.Sql;
using SimpleDB.Test.Tools;

namespace SimpleDB.Test.Sql
{
    class SqlQueryExecutorTest
    {
        private Mapper<TestEntity> _mapper;
        private Collection<TestEntity> _collection;
        private SqlQueryExecutor _executor;

        [SetUp]
        public void Setup()
        {
            var fileSystem = new MemoryFileSystem();
            var memory = Memory.Instance;
            _mapper = new Mapper<TestEntity>(
                new PrimaryKeyMapping<TestEntity>(entity => entity.Id),
                new FieldMapping<TestEntity>[]
                {
                    new FieldMapping<TestEntity>(0, entity => entity.Byte),
                    new FieldMapping<TestEntity>(1, entity => entity.Float),
                    new FieldMapping<TestEntity>(2, entity => entity.String)
                });
            _collection = new Collection<TestEntity>(
                _mapper,
                new PrimaryKeyFileFactory(fileSystem, memory),
                new DataFileFactory(fileSystem, memory),
                new MetaFileFactory(fileSystem));
            _executor = new SqlQueryExecutor(
                new PrimaryKeyFileFactory(fileSystem, memory),
                new DataFileFactory(fileSystem, memory),
                new IndexHolder(),
                null);
        }

        [Test]
        public void ExecuteQuery_Select()
        {
            _collection.Insert(new[]
            {
                new TestEntity { Id = 1, Byte = 10, Float = 10.2f, String = "123" },
                new TestEntity { Id = 2, Byte = 20, Float = 20.2f, String = "456" },
                new TestEntity { Id = 3, Byte = 30, Float = 30.2f, String = "789" },
            });
            var context = new QueryContext
            {
                EntityMetaCollection = new List<EntityMeta> { _mapper.EntityMeta }
            };
            var result = _executor.ExecuteQuery(context, "SELECT * FROM TestEntity");

            Assert.AreEqual("TestEntity", result.EntityName);
            Assert.AreEqual(3, result.FieldValueCollections.Count);

            Assert.AreEqual((byte)10, result.FieldValueCollections[0][0].Value);
            Assert.AreEqual(10.2f, result.FieldValueCollections[0][1].Value);
            Assert.AreEqual("123", result.FieldValueCollections[0][2].Value);

            Assert.AreEqual((byte)20, result.FieldValueCollections[1][0].Value);
            Assert.AreEqual(20.2f, result.FieldValueCollections[1][1].Value);
            Assert.AreEqual("456", result.FieldValueCollections[1][2].Value);

            Assert.AreEqual((byte)30, result.FieldValueCollections[2][0].Value);
            Assert.AreEqual(30.2f, result.FieldValueCollections[2][1].Value);
            Assert.AreEqual("789", result.FieldValueCollections[2][2].Value);
        }

        [Test]
        public void ExecuteQuery_SelectByte()
        {
            _collection.Insert(new[]
            {
                new TestEntity { Id = 1, Byte = 10, Float = 10.2f, String = "123" },
                new TestEntity { Id = 2, Byte = 20, Float = 20.2f, String = "456" },
                new TestEntity { Id = 3, Byte = 30, Float = 30.2f, String = "789" },
            });
            var context = new QueryContext
            {
                EntityMetaCollection = new List<EntityMeta> { _mapper.EntityMeta }
            };
            var result = _executor.ExecuteQuery(context, "SELECT Byte FROM TestEntity");

            Assert.AreEqual("TestEntity", result.EntityName);
            Assert.AreEqual(3, result.FieldValueCollections.Count);
            Assert.AreEqual((byte)10, result.FieldValueCollections[0][0].Value);
            Assert.AreEqual((byte)20, result.FieldValueCollections[1][0].Value);
            Assert.AreEqual((byte)30, result.FieldValueCollections[2][0].Value);
            Assert.AreEqual(1, result.FieldValueCollections[0].Count);
            Assert.AreEqual(1, result.FieldValueCollections[1].Count);
            Assert.AreEqual(1, result.FieldValueCollections[2].Count);
        }

        [Test]
        public void ExecuteQuery_Update()
        {
            _collection.Insert(new[]
            {
                new TestEntity { Id = 1, Byte = 10, Float = 10.2f, String = "123" },
                new TestEntity { Id = 2, Byte = 20, Float = 20.2f, String = "456" },
                new TestEntity { Id = 3, Byte = 30, Float = 30.2f, String = "789" },
            });
            var context = new QueryContext
            {
                EntityMetaCollection = new List<EntityMeta> { _mapper.EntityMeta }
            };
            var result = _executor.ExecuteQuery(context, "UPDATE TestEntity SET String = '123'");

            Assert.AreEqual("TestEntity", result.EntityName);
            Assert.AreEqual(3, (int)result.Scalar);
        }

        [Test]
        public void ExecuteQuery_Delete()
        {
            _collection.Insert(new[]
            {
                new TestEntity { Id = 1, Byte = 10, Float = 10.2f, String = "123" },
                new TestEntity { Id = 2, Byte = 20, Float = 20.2f, String = "456" },
                new TestEntity { Id = 3, Byte = 30, Float = 30.2f, String = "789" },
            });
            var context = new QueryContext
            {
                EntityMetaCollection = new List<EntityMeta> { _mapper.EntityMeta }
            };
            var result = _executor.ExecuteQuery(context, "DELETE TestEntity WHERE String = '123'");

            Assert.AreEqual("TestEntity", result.EntityName);
            Assert.AreEqual(1, (int)result.Scalar);
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
