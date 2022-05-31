using System.Linq;
using NUnit.Framework;
using SimpleDB.Core;
using SimpleDB.Maintenance;
using SimpleDB.Test.Tools;

namespace SimpleDB.Test.Maintenance;

class DefragmentatorTest
{
    private MemoryFileSystem _fileSystem;
    private Mapper<TestEntity> _mapper;
    private Collection<TestEntity> _collection;
    private Defragmentator _defragmentator;

    [SetUp]
    public void Setup()
    {
        _fileSystem = new MemoryFileSystem();
        _mapper = new Mapper<TestEntity>(
            new PrimaryKeyMapping<TestEntity>(entity => entity.Id),
            new FieldMapping<TestEntity>[]
            {
                new FieldMapping<TestEntity>(1, entity => entity.Byte),
                new FieldMapping<TestEntity>(2, entity => entity.Float),
                new FieldMapping<TestEntity>(3, entity => entity.String)
            });
        InitCollection();
        _defragmentator = new Defragmentator(
            new PrimaryKeyFileFactory(_fileSystem),
            new DataFileFactory(_fileSystem),
            new MetaFileFactory(_fileSystem),
            _fileSystem);
    }

    private void InitCollection()
    {
        _collection = new Collection<TestEntity>(
            _mapper,
            new PrimaryKeyFileFactory(_fileSystem),
            new DataFileFactory(_fileSystem),
            new MetaFileFactory(_fileSystem));
    }

    [Test]
    public void Files()
    {
        _collection.Insert(new TestEntity { Id = 1 });
        _collection.Delete(1);
        _defragmentator.DefragmentDataFile("TestEntity.data");

        Assert.AreEqual(3, _fileSystem.FileNames.Count);
        Assert.IsTrue(_fileSystem.FileNames.Any(x => x == "TestEntity.meta"));
        Assert.IsTrue(_fileSystem.FileNames.Any(x => x == "TestEntity.primary"));
        Assert.IsTrue(_fileSystem.FileNames.Any(x => x == "TestEntity.data"));

        Assert.AreEqual(3, _fileSystem.FileStreams.Count);
        Assert.IsTrue(_fileSystem.FileStreams.Any(x => x.Name == "TestEntity.meta"));
        Assert.IsTrue(_fileSystem.FileStreams.Any(x => x.Name == "TestEntity.primary"));
        Assert.IsTrue(_fileSystem.FileStreams.Any(x => x.Name == "TestEntity.data"));
    }

    [Test]
    public void Empty()
    {
        _collection.Insert(new TestEntity { Id = 1 });
        _collection.Delete(1);
        _defragmentator.DefragmentDataFile("TestEntity.data");
        InitCollection();

        Assert.AreEqual(0, _collection.Count());
        Assert.AreEqual(null, _collection.GetOrDefault(1));
    }

    [Test]
    public void Half()
    {
        _collection.Insert(new TestEntity { Id = 1 });
        _collection.Insert(new TestEntity { Id = 2, String = "123" });
        _collection.Delete(1);
        _defragmentator.DefragmentDataFile("TestEntity.data");
        InitCollection();

        Assert.AreEqual(1, _collection.Count());
        Assert.AreEqual("123", _collection.GetOrDefault(2).String);
    }

    [Test]
    public void PrimaryKeysSorted()
    {
        _collection.Insert(new TestEntity { Id = 3 });
        _collection.Insert(new TestEntity { Id = 2 });
        _collection.Insert(new TestEntity { Id = 1 });
        _defragmentator.DefragmentDataFile("TestEntity.data");

        Assert.AreEqual(3, _collection.Count());

        var primaryKeyFile = _collection.PrimaryKeyFile;
        primaryKeyFile.BeginRead();
        var primaryKeys = primaryKeyFile.GetAllPrimaryKeys().ToList();
        primaryKeyFile.EndReadWrite();
        Assert.AreEqual(1, primaryKeys[0].Value);
        Assert.AreEqual(2, primaryKeys[1].Value);
        Assert.AreEqual(3, primaryKeys[2].Value);
    }

    class TestEntity
    {
        public int Id { get; set; }

        public byte Byte { get; set; }

        public float Float { get; set; }

        public string String { get; set; }
    }
}
