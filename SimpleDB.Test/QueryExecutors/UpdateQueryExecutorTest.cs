using Moq;
using NUnit.Framework;
using SimpleDB.Core;
using SimpleDB.IndexedSearch;
using SimpleDB.Infrastructure;
using SimpleDB.Queries;
using SimpleDB.QueryExecutors;
using SimpleDB.Test.Tools;

namespace SimpleDB.Test.QueryExecutors;

class UpdateQueryExecutorTest
{
    private Mock<IIndexUpdater> _indexUpdater;
    private MemoryFileSystem _fileSystem;
    private Memory _memory;
    private Mapper<TestEntity> _mapper;
    private Collection<TestEntity> _collection;
    private UpdateQueryExecutor _queryExecutor;

    [SetUp]
    public void Setup()
    {
        _fileSystem = new MemoryFileSystem();
        _memory = Memory.Instance;
        _mapper = new Mapper<TestEntity>(
            new PrimaryKeyMapping<TestEntity>(x => x.Id),
            new FieldMapping<TestEntity>[]
            {
                new(1, x => x.Byte),
                new(2, x => x.Float),
                new(3, x => x.String),
                new(4, x => x.ByteArray),
                new(5, x => x.InnerObject)
            });
        _collection = new Collection<TestEntity>(
            _mapper,
            new PrimaryKeyFileFactory(_fileSystem, _memory),
            new DataFileFactory(_fileSystem, _memory),
            new MetaFileFactory(_fileSystem));
        _indexUpdater = new Mock<IIndexUpdater>();
        _queryExecutor = new UpdateQueryExecutor(
            _mapper.EntityMeta,
            _collection.PrimaryKeyFile,
            _collection.DataFile,
            _collection.PrimaryKeys,
            new FieldValueReader(_collection.DataFile),
            new IndexHolder(),
            _indexUpdater.Object);
    }

    [Test]
    public void ExecuteQuery()
    {
        _collection.Insert(new TestEntity { Id = 1, Byte = 10, Float = 1.2f, String = "123" });
        _collection.Insert(new TestEntity { Id = 2, Byte = 20, Float = 3.4f, String = "456" });
        _collection.Insert(new TestEntity { Id = 3, Byte = 30, Float = 5.6f, String = "789" });
        var query = new UpdateQuery("TestEntity", new UpdateClause(new[] { new UpdateClause.Field(1, (byte)100), new UpdateClause.Field(2, 34.5f) }));

        var result = _queryExecutor.ExecuteQuery(query);

        Assert.AreEqual(3, result);

        var entity1 = _collection.Get(1);
        Assert.AreEqual((byte)100, entity1.Byte);
        Assert.AreEqual(34.5f, entity1.Float);
        Assert.AreEqual("123", entity1.String);

        var entity2 = _collection.Get(2);
        Assert.AreEqual((byte)100, entity2.Byte);
        Assert.AreEqual(34.5f, entity2.Float);
        Assert.AreEqual("456", entity2.String);

        var entity3 = _collection.Get(3);
        Assert.AreEqual((byte)100, entity3.Byte);
        Assert.AreEqual(34.5f, entity3.Float);
        Assert.AreEqual("789", entity3.String);
    }

    [Test]
    public void ExecuteQuery_Where()
    {
        _collection.Insert(new TestEntity { Id = 1, Byte = 10, Float = 1.2f, String = "123" });
        _collection.Insert(new TestEntity { Id = 2, Byte = 20, Float = 3.4f, String = "456" });
        _collection.Insert(new TestEntity { Id = 3, Byte = 30, Float = 5.6f, String = "789" });
        var query = new UpdateQuery("TestEntity", new UpdateClause(new[] { new UpdateClause.Field(1, (byte)100), new UpdateClause.Field(2, 34.5f) }))
        {
            WhereClause = new WhereClause(new WhereClause.EqualsOperation(new WhereClause.Field(1), new WhereClause.Constant((byte)10)))
        };

        var result = _queryExecutor.ExecuteQuery(query);

        Assert.AreEqual(1, result);

        var entity1 = _collection.Get(1);
        Assert.AreEqual((byte)100, entity1.Byte);
        Assert.AreEqual(34.5f, entity1.Float);
        Assert.AreEqual("123", entity1.String);

        var entity2 = _collection.Get(2);
        Assert.AreEqual((byte)20, entity2.Byte);
        Assert.AreEqual(3.4f, entity2.Float);
        Assert.AreEqual("456", entity2.String);

        var entity3 = _collection.Get(3);
        Assert.AreEqual((byte)30, entity3.Byte);
        Assert.AreEqual(5.6f, entity3.Float);
        Assert.AreEqual("789", entity3.String);
    }

    [Test]
    public void ExecuteQuery_WhereAndUpdateSameField()
    {
        _collection.Insert(new TestEntity { Id = 1, Byte = 10, Float = 1.2f, String = "123" });
        var query = new UpdateQuery("TestEntity", new UpdateClause(new[] { new UpdateClause.Field(3, "987") }))
        {
            WhereClause = new WhereClause(new WhereClause.EqualsOperation(new WhereClause.Field(3), new WhereClause.Constant("123")))
        };

        var result = _queryExecutor.ExecuteQuery(query);

        Assert.AreEqual(1, result);

        var entity1 = _collection.Get(1);
        Assert.AreEqual((byte)10, entity1.Byte);
        Assert.AreEqual(1.2f, entity1.Float);
        Assert.AreEqual("987", entity1.String);
    }

    [Test]
    public void ExecuteQuery_String()
    {
        _collection.Insert(new TestEntity { Id = 1, Byte = 10, Float = 1.2f, String = "123" });
        _collection.Insert(new TestEntity { Id = 2, Byte = 20, Float = 3.4f, String = "456" });
        _collection.Insert(new TestEntity { Id = 3, Byte = 30, Float = 5.6f, String = "789" });
        var query = new UpdateQuery("TestEntity", new UpdateClause(new[] { new UpdateClause.Field(3, "000") }));

        var result = _queryExecutor.ExecuteQuery(query);

        Assert.AreEqual(3, result);

        var entity1 = _collection.Get(1);
        Assert.AreEqual((byte)10, entity1.Byte);
        Assert.AreEqual(1.2f, entity1.Float);
        Assert.AreEqual("000", entity1.String);

        var entity2 = _collection.Get(2);
        Assert.AreEqual((byte)20, entity2.Byte);
        Assert.AreEqual(3.4f, entity2.Float);
        Assert.AreEqual("000", entity2.String);

        var entity3 = _collection.Get(3);
        Assert.AreEqual((byte)30, entity3.Byte);
        Assert.AreEqual(5.6f, entity3.Float);
        Assert.AreEqual("000", entity3.String);
    }

    [Test]
    public void ExecuteQuery_StringShorter()
    {
        _collection.Insert(new TestEntity { Id = 1, Byte = 10, Float = 1.2f, String = "123" });
        _collection.Insert(new TestEntity { Id = 2, Byte = 20, Float = 3.4f, String = "456" });
        _collection.Insert(new TestEntity { Id = 3, Byte = 30, Float = 5.6f, String = "789" });
        var query = new UpdateQuery("TestEntity", new UpdateClause(new[] { new UpdateClause.Field(3, "0") }));

        var result = _queryExecutor.ExecuteQuery(query);

        Assert.AreEqual(3, result);

        var entity1 = _collection.Get(1);
        Assert.AreEqual((byte)10, entity1.Byte);
        Assert.AreEqual(1.2f, entity1.Float);
        Assert.AreEqual("0", entity1.String);

        var entity2 = _collection.Get(2);
        Assert.AreEqual((byte)20, entity2.Byte);
        Assert.AreEqual(3.4f, entity2.Float);
        Assert.AreEqual("0", entity2.String);

        var entity3 = _collection.Get(3);
        Assert.AreEqual((byte)30, entity3.Byte);
        Assert.AreEqual(5.6f, entity3.Float);
        Assert.AreEqual("0", entity3.String);
    }

    [Test]
    public void ExecuteQuery_StringLonger()
    {
        _collection.Insert(new TestEntity { Id = 1, Byte = 10, Float = 1.2f, String = "123" });
        _collection.Insert(new TestEntity { Id = 2, Byte = 20, Float = 3.4f, String = "456" });
        _collection.Insert(new TestEntity { Id = 3, Byte = 30, Float = 5.6f, String = "789" });
        var query = new UpdateQuery("TestEntity", new UpdateClause(new[] { new UpdateClause.Field(3, "0000000000") }));

        var result = _queryExecutor.ExecuteQuery(query);

        Assert.AreEqual(3, result);

        var entity1 = _collection.Get(1);
        Assert.AreEqual((byte)10, entity1.Byte);
        Assert.AreEqual(1.2f, entity1.Float);
        Assert.AreEqual("0000000000", entity1.String);

        var entity2 = _collection.Get(2);
        Assert.AreEqual((byte)20, entity2.Byte);
        Assert.AreEqual(3.4f, entity2.Float);
        Assert.AreEqual("0000000000", entity2.String);

        var entity3 = _collection.Get(3);
        Assert.AreEqual((byte)30, entity3.Byte);
        Assert.AreEqual(5.6f, entity3.Float);
        Assert.AreEqual("0000000000", entity3.String);
    }

    [Test]
    public void ExecuteQuery_ByteArray()
    {
        _collection.Insert(new TestEntity { Id = 1, Byte = 10, Float = 1.2f, ByteArray = new byte[] { 1, 2, 3 } });
        _collection.Insert(new TestEntity { Id = 2, Byte = 20, Float = 3.4f, ByteArray = new byte[] { 4, 5, 6 } });
        _collection.Insert(new TestEntity { Id = 3, Byte = 30, Float = 5.6f, ByteArray = new byte[] { 7, 8, 9 } });
        var query = new UpdateQuery("TestEntity", new UpdateClause(new[] { new UpdateClause.Field(4, new byte[] { 0, 0, 0 }) }));

        var result = _queryExecutor.ExecuteQuery(query);

        Assert.AreEqual(3, result);

        var entity1 = _collection.Get(1);
        Assert.AreEqual((byte)10, entity1.Byte);
        Assert.AreEqual(1.2f, entity1.Float);
        Assert.AreEqual(0, entity1.ByteArray[0]);
        Assert.AreEqual(0, entity1.ByteArray[1]);
        Assert.AreEqual(0, entity1.ByteArray[2]);

        var entity2 = _collection.Get(2);
        Assert.AreEqual((byte)20, entity2.Byte);
        Assert.AreEqual(3.4f, entity2.Float);
        Assert.AreEqual(0, entity2.ByteArray[0]);
        Assert.AreEqual(0, entity2.ByteArray[1]);
        Assert.AreEqual(0, entity2.ByteArray[2]);

        var entity3 = _collection.Get(3);
        Assert.AreEqual((byte)30, entity3.Byte);
        Assert.AreEqual(5.6f, entity3.Float);
        Assert.AreEqual(0, entity3.ByteArray[0]);
        Assert.AreEqual(0, entity3.ByteArray[1]);
        Assert.AreEqual(0, entity3.ByteArray[2]);
    }

    [Test]
    public void ExecuteQuery_ByteArrayShorter()
    {
        _collection.Insert(new TestEntity { Id = 1, Byte = 10, Float = 1.2f, ByteArray = new byte[] { 1, 2, 3 } });
        _collection.Insert(new TestEntity { Id = 2, Byte = 20, Float = 3.4f, ByteArray = new byte[] { 4, 5, 6 } });
        _collection.Insert(new TestEntity { Id = 3, Byte = 30, Float = 5.6f, ByteArray = new byte[] { 7, 8, 9 } });
        var query = new UpdateQuery("TestEntity", new UpdateClause(new[] { new UpdateClause.Field(4, new byte[] { 0 }) }));

        var result = _queryExecutor.ExecuteQuery(query);

        Assert.AreEqual(3, result);

        var entity1 = _collection.Get(1);
        Assert.AreEqual((byte)10, entity1.Byte);
        Assert.AreEqual(1.2f, entity1.Float);
        Assert.AreEqual(1, entity1.ByteArray.Length);
        Assert.AreEqual(0, entity1.ByteArray[0]);

        var entity2 = _collection.Get(2);
        Assert.AreEqual((byte)20, entity2.Byte);
        Assert.AreEqual(3.4f, entity2.Float);
        Assert.AreEqual(1, entity2.ByteArray.Length);
        Assert.AreEqual(0, entity2.ByteArray[0]);

        var entity3 = _collection.Get(3);
        Assert.AreEqual((byte)30, entity3.Byte);
        Assert.AreEqual(5.6f, entity3.Float);
        Assert.AreEqual(1, entity3.ByteArray.Length);
        Assert.AreEqual(0, entity3.ByteArray[0]);
    }

    [Test]
    public void ExecuteQuery_ByteArrayLonger()
    {
        _collection.Insert(new TestEntity { Id = 1, Byte = 10, Float = 1.2f, ByteArray = new byte[] { 1, 2, 3 } });
        _collection.Insert(new TestEntity { Id = 2, Byte = 20, Float = 3.4f, ByteArray = new byte[] { 4, 5, 6 } });
        _collection.Insert(new TestEntity { Id = 3, Byte = 30, Float = 5.6f, ByteArray = new byte[] { 7, 8, 9 } });
        var query = new UpdateQuery("TestEntity", new UpdateClause(new[] { new UpdateClause.Field(4, new byte[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }) }));

        var result = _queryExecutor.ExecuteQuery(query);

        Assert.AreEqual(3, result);

        var entity1 = _collection.Get(1);
        Assert.AreEqual((byte)10, entity1.Byte);
        Assert.AreEqual(1.2f, entity1.Float);
        Assert.AreEqual(10, entity1.ByteArray.Length);

        var entity2 = _collection.Get(2);
        Assert.AreEqual((byte)20, entity2.Byte);
        Assert.AreEqual(3.4f, entity2.Float);
        Assert.AreEqual(10, entity2.ByteArray.Length);

        var entity3 = _collection.Get(3);
        Assert.AreEqual((byte)30, entity3.Byte);
        Assert.AreEqual(5.6f, entity3.Float);
        Assert.AreEqual(10, entity3.ByteArray.Length);
    }

    [Test]
    public void ExecuteQuery_ByteArrayToNull()
    {
        _collection.Insert(new TestEntity { Id = 1, Byte = 10, Float = 1.2f, ByteArray = new byte[] { 1, 2, 3 } });
        _collection.Insert(new TestEntity { Id = 2, Byte = 20, Float = 3.4f, ByteArray = new byte[] { 4, 5, 6 } });
        _collection.Insert(new TestEntity { Id = 3, Byte = 30, Float = 5.6f, ByteArray = new byte[] { 7, 8, 9 } });
        var query = new UpdateQuery("TestEntity", new UpdateClause(new[] { new UpdateClause.Field(4, null) }));

        var result = _queryExecutor.ExecuteQuery(query);

        Assert.AreEqual(3, result);

        var entity1 = _collection.Get(1);
        Assert.AreEqual((byte)10, entity1.Byte);
        Assert.AreEqual(1.2f, entity1.Float);
        Assert.AreEqual(null, entity1.ByteArray);

        var entity2 = _collection.Get(2);
        Assert.AreEqual((byte)20, entity2.Byte);
        Assert.AreEqual(3.4f, entity2.Float);
        Assert.AreEqual(null, entity2.ByteArray);

        var entity3 = _collection.Get(3);
        Assert.AreEqual((byte)30, entity3.Byte);
        Assert.AreEqual(5.6f, entity3.Float);
        Assert.AreEqual(null, entity3.ByteArray);
    }

    [Test]
    public void ExecuteQuery_ByteArrayFromNull()
    {
        _collection.Insert(new TestEntity { Id = 1, Byte = 10, Float = 1.2f, ByteArray = null });
        _collection.Insert(new TestEntity { Id = 2, Byte = 20, Float = 3.4f, ByteArray = null });
        _collection.Insert(new TestEntity { Id = 3, Byte = 30, Float = 5.6f, ByteArray = null });
        var query = new UpdateQuery("TestEntity", new UpdateClause(new[] { new UpdateClause.Field(4, new byte[] { 1, 2, 3 }) }));

        var result = _queryExecutor.ExecuteQuery(query);

        Assert.AreEqual(3, result);

        var entity1 = _collection.Get(1);
        Assert.AreEqual((byte)10, entity1.Byte);
        Assert.AreEqual(1.2f, entity1.Float);
        Assert.AreEqual(3, entity1.ByteArray.Length);

        var entity2 = _collection.Get(2);
        Assert.AreEqual((byte)20, entity2.Byte);
        Assert.AreEqual(3.4f, entity2.Float);
        Assert.AreEqual(3, entity2.ByteArray.Length);

        var entity3 = _collection.Get(3);
        Assert.AreEqual((byte)30, entity3.Byte);
        Assert.AreEqual(5.6f, entity3.Float);
        Assert.AreEqual(3, entity3.ByteArray.Length);
    }

    [Test]
    public void ExecuteQuery_ByteArrayNull()
    {
        _collection.Insert(new TestEntity { Id = 1, Byte = 10, Float = 1.2f, ByteArray = null });
        _collection.Insert(new TestEntity { Id = 2, Byte = 20, Float = 3.4f, ByteArray = null });
        _collection.Insert(new TestEntity { Id = 3, Byte = 30, Float = 5.6f, ByteArray = null });
        var query = new UpdateQuery("TestEntity", new UpdateClause(new[] { new UpdateClause.Field(4, null) }));

        var result = _queryExecutor.ExecuteQuery(query);

        Assert.AreEqual(3, result);

        var entity1 = _collection.Get(1);
        Assert.AreEqual((byte)10, entity1.Byte);
        Assert.AreEqual(1.2f, entity1.Float);
        Assert.AreEqual(null, entity1.ByteArray);

        var entity2 = _collection.Get(2);
        Assert.AreEqual((byte)20, entity2.Byte);
        Assert.AreEqual(3.4f, entity2.Float);
        Assert.AreEqual(null, entity2.ByteArray);

        var entity3 = _collection.Get(3);
        Assert.AreEqual((byte)30, entity3.Byte);
        Assert.AreEqual(5.6f, entity3.Float);
        Assert.AreEqual(null, entity3.ByteArray);
    }

    [Test]
    public void ExecuteQuery_Object()
    {
        _collection.Insert(new TestEntity { Id = 1, Byte = 10, Float = 1.2f, String = "123", InnerObject = new InnerObject { Value = 123 } });
        _collection.Insert(new TestEntity { Id = 2, Byte = 20, Float = 3.4f, String = "456", InnerObject = new InnerObject { Value = 456 } });
        _collection.Insert(new TestEntity { Id = 3, Byte = 30, Float = 5.6f, String = "789", InnerObject = new InnerObject { Value = 789 } });
        var query = new UpdateQuery("TestEntity", new UpdateClause(new[] { new UpdateClause.Field(5, new InnerObject { Value = 111 }) }));

        var result = _queryExecutor.ExecuteQuery(query);

        Assert.AreEqual(3, result);

        var entity1 = _collection.Get(1);
        Assert.AreEqual((byte)10, entity1.Byte);
        Assert.AreEqual(1.2f, entity1.Float);
        Assert.AreEqual("123", entity1.String);
        Assert.AreEqual(111, entity1.InnerObject.Value);

        var entity2 = _collection.Get(2);
        Assert.AreEqual((byte)20, entity2.Byte);
        Assert.AreEqual(3.4f, entity2.Float);
        Assert.AreEqual("456", entity2.String);
        Assert.AreEqual(111, entity2.InnerObject.Value);

        var entity3 = _collection.Get(3);
        Assert.AreEqual((byte)30, entity3.Byte);
        Assert.AreEqual(5.6f, entity3.Float);
        Assert.AreEqual("789", entity3.String);
        Assert.AreEqual(111, entity3.InnerObject.Value);
    }

    [Test]
    public void ExecuteQuery_ObjectShorter()
    {
        _collection.Insert(new TestEntity { Id = 1, Byte = 10, Float = 1.2f, String = "123", InnerObject = new InnerObject { Value = 123 } });
        _collection.Insert(new TestEntity { Id = 2, Byte = 20, Float = 3.4f, String = "456", InnerObject = new InnerObject { Value = 456 } });
        _collection.Insert(new TestEntity { Id = 3, Byte = 30, Float = 5.6f, String = "789", InnerObject = new InnerObject { Value = 789 } });
        var query = new UpdateQuery("TestEntity", new UpdateClause(new[] { new UpdateClause.Field(5, new InnerObject { Value = 1 }) }));

        var result = _queryExecutor.ExecuteQuery(query);

        Assert.AreEqual(3, result);

        var entity1 = _collection.Get(1);
        Assert.AreEqual((byte)10, entity1.Byte);
        Assert.AreEqual(1.2f, entity1.Float);
        Assert.AreEqual("123", entity1.String);
        Assert.AreEqual(1, entity1.InnerObject.Value);

        var entity2 = _collection.Get(2);
        Assert.AreEqual((byte)20, entity2.Byte);
        Assert.AreEqual(3.4f, entity2.Float);
        Assert.AreEqual("456", entity2.String);
        Assert.AreEqual(1, entity2.InnerObject.Value);

        var entity3 = _collection.Get(3);
        Assert.AreEqual((byte)30, entity3.Byte);
        Assert.AreEqual(5.6f, entity3.Float);
        Assert.AreEqual("789", entity3.String);
        Assert.AreEqual(1, entity3.InnerObject.Value);
    }

    [Test]
    public void ExecuteQuery_ObjectLonger()
    {
        _collection.Insert(new TestEntity { Id = 1, Byte = 10, Float = 1.2f, String = "123", InnerObject = new InnerObject { Value = 123 } });
        _collection.Insert(new TestEntity { Id = 2, Byte = 20, Float = 3.4f, String = "456", InnerObject = new InnerObject { Value = 456 } });
        _collection.Insert(new TestEntity { Id = 3, Byte = 30, Float = 5.6f, String = "789", InnerObject = new InnerObject { Value = 789 } });
        var query = new UpdateQuery("TestEntity", new UpdateClause(new[] { new UpdateClause.Field(5, new InnerObject { Value = 123456789 }) }));

        var result = _queryExecutor.ExecuteQuery(query);

        Assert.AreEqual(3, result);

        var entity1 = _collection.Get(1);
        Assert.AreEqual((byte)10, entity1.Byte);
        Assert.AreEqual(1.2f, entity1.Float);
        Assert.AreEqual("123", entity1.String);
        Assert.AreEqual(123456789, entity1.InnerObject.Value);

        var entity2 = _collection.Get(2);
        Assert.AreEqual((byte)20, entity2.Byte);
        Assert.AreEqual(3.4f, entity2.Float);
        Assert.AreEqual("456", entity2.String);
        Assert.AreEqual(123456789, entity2.InnerObject.Value);

        var entity3 = _collection.Get(3);
        Assert.AreEqual((byte)30, entity3.Byte);
        Assert.AreEqual(5.6f, entity3.Float);
        Assert.AreEqual("789", entity3.String);
        Assert.AreEqual(123456789, entity3.InnerObject.Value);
    }

    [Test]
    public void ExecuteQuery_UpdateIndexes()
    {
        _collection.Insert(new TestEntity { Id = 1, Byte = 10, Float = 1.2f, String = "123", InnerObject = new InnerObject { Value = 123 } });
        _collection.Insert(new TestEntity { Id = 2, Byte = 20, Float = 3.4f, String = "456", InnerObject = new InnerObject { Value = 456 } });
        _collection.Insert(new TestEntity { Id = 3, Byte = 30, Float = 5.6f, String = "789", InnerObject = new InnerObject { Value = 789 } });
        var query = new UpdateQuery("TestEntity", new UpdateClause(new[] { new UpdateClause.Field(1, (byte)123) }));

        var result = _queryExecutor.ExecuteQuery(query);

        _indexUpdater.Verify(x => x.UpdateIndexes(_mapper.EntityMeta, new object[] { 1, 2, 3 }, new[] { new FieldValue(1, (byte)123) }));
    }

    class TestEntity
    {
        public int Id { get; set; }

        public byte Byte { get; set; }

        public float Float { get; set; }

        public string String { get; set; }

        public byte[] ByteArray { get; set; }

        public InnerObject InnerObject { get; set; }
    }

    class InnerObject
    {
        public int Value { get; set; }
    }
}
