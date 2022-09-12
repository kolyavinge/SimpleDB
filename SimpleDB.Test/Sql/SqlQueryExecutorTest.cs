using System.Collections.Generic;
using Moq;
using NUnit.Framework;
using SimpleDB.Core;
using SimpleDB.IndexedSearch;
using SimpleDB.Infrastructure;
using SimpleDB.Sql;
using SimpleDB.Test.Tools;

namespace SimpleDB.Test.Sql;

class SqlQueryExecutorTest
{
    private Mock<IIndexUpdater> _indexUpdater;
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
                new FieldMapping<TestEntity>(1, entity => entity.Byte),
                new FieldMapping<TestEntity>(2, entity => entity.Float),
                new FieldMapping<TestEntity>(3, entity => entity.String)
            });
        _collection = new Collection<TestEntity>(
            _mapper,
            new PrimaryKeyFileFactory(fileSystem, memory),
            new DataFileFactory(fileSystem, memory),
            new MetaFileFactory(fileSystem));
        var entityMetaDictionary = new Dictionary<string, EntityMeta>
        {
            { _mapper.EntityName, _mapper.EntityMeta }
        };
        _indexUpdater = new Mock<IIndexUpdater>();
        _executor = new SqlQueryExecutor(
            entityMetaDictionary,
            new PrimaryKeyFileFactory(fileSystem, memory),
            new DataFileFactory(fileSystem, memory),
            new IndexHolder(),
            _indexUpdater.Object);
    }

    [Test]
    public void ExecuteQuery_Select()
    {
        _collection.InsertRange(new[]
        {
            new TestEntity { Id = 1, Byte = 10, Float = 10.2f, String = "123" },
            new TestEntity { Id = 2, Byte = 20, Float = 20.2f, String = "456" },
            new TestEntity { Id = 3, Byte = 30, Float = 30.2f, String = "789" },
        });
        var result = _executor.ExecuteQuery("SELECT * FROM TestEntity");

        Assert.AreEqual("TestEntity", result.EntityName);
        Assert.AreEqual(3, result.FieldValueCollections.Count);

        Assert.AreEqual((byte)10, result.FieldValueCollections[0][1].Value);
        Assert.AreEqual(10.2f, result.FieldValueCollections[0][2].Value);
        Assert.AreEqual("123", result.FieldValueCollections[0][3].Value);

        Assert.AreEqual((byte)20, result.FieldValueCollections[1][1].Value);
        Assert.AreEqual(20.2f, result.FieldValueCollections[1][2].Value);
        Assert.AreEqual("456", result.FieldValueCollections[1][3].Value);

        Assert.AreEqual((byte)30, result.FieldValueCollections[2][1].Value);
        Assert.AreEqual(30.2f, result.FieldValueCollections[2][2].Value);
        Assert.AreEqual("789", result.FieldValueCollections[2][3].Value);
    }

    [Test]
    public void ExecuteQuery_SelectByte()
    {
        _collection.InsertRange(new[]
        {
            new TestEntity { Id = 1, Byte = 10, Float = 10.2f, String = "123" },
            new TestEntity { Id = 2, Byte = 20, Float = 20.2f, String = "456" },
            new TestEntity { Id = 3, Byte = 30, Float = 30.2f, String = "789" },
        });
        var result = _executor.ExecuteQuery("SELECT Byte FROM TestEntity");

        Assert.AreEqual("TestEntity", result.EntityName);
        Assert.AreEqual(3, result.FieldValueCollections.Count);
        Assert.AreEqual((byte)10, result.FieldValueCollections[0][1].Value);
        Assert.AreEqual((byte)20, result.FieldValueCollections[1][1].Value);
        Assert.AreEqual((byte)30, result.FieldValueCollections[2][1].Value);
        Assert.AreEqual(1, result.FieldValueCollections[0].Count);
        Assert.AreEqual(1, result.FieldValueCollections[1].Count);
        Assert.AreEqual(1, result.FieldValueCollections[2].Count);
    }

    [Test]
    public void ExecuteQuery_Update()
    {
        _collection.InsertRange(new[]
        {
            new TestEntity { Id = 1, Byte = 10, Float = 10.2f, String = "123" },
            new TestEntity { Id = 2, Byte = 20, Float = 20.2f, String = "456" },
            new TestEntity { Id = 3, Byte = 30, Float = 30.2f, String = "789" },
        });
        var result = _executor.ExecuteQuery("UPDATE TestEntity SET String = '123'");

        Assert.AreEqual("TestEntity", result.EntityName);
        Assert.AreEqual(3, (int)result.Scalar);
    }

    [Test]
    public void ExecuteQuery_Delete()
    {
        _collection.InsertRange(new[]
        {
            new TestEntity { Id = 1, Byte = 10, Float = 10.2f, String = "123" },
            new TestEntity { Id = 2, Byte = 20, Float = 20.2f, String = "456" },
            new TestEntity { Id = 3, Byte = 30, Float = 30.2f, String = "789" },
        });
        var result = _executor.ExecuteQuery("DELETE TestEntity WHERE String = '123'");

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
