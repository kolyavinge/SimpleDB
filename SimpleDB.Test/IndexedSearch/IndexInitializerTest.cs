using System;
using System.Linq;
using System.Linq.Expressions;
using NUnit.Framework;
using SimpleDB.Core;
using SimpleDB.IndexedSearch;
using SimpleDB.Infrastructure;
using SimpleDB.Test.Tools;

namespace SimpleDB.Test.IndexedSearch
{
    class IndexInitializerTest
    {
        private MemoryFileSystem _fileSystem;
        private Collection<TestEntity> _collection;
        private IndexInitializer<TestEntity> _initializer;

        [SetUp]
        public void Setup()
        {
            _fileSystem = new MemoryFileSystem();
            var memory = Memory.Instance;
            var mapper = new Mapper<TestEntity>("test",
                new PrimaryKeyMapping<TestEntity>(x => x.Id),
                new FieldMapping<TestEntity>[]
                {
                    new FieldMapping<TestEntity>(0, x => x.Int),
                    new FieldMapping<TestEntity>(1, x => x.Float),
                    new FieldMapping<TestEntity>(2, x => x.String),
                });
            _collection = new Collection<TestEntity>(
                mapper,
                new PrimaryKeyFileFactory("working directory", _fileSystem, memory),
                new DataFileFactory("working directory", _fileSystem, memory),
                new MetaFileFactory("working directory", _fileSystem),
                _fileSystem);
            var mapperHolder = new MapperHolder(new[] { mapper });
            _initializer = new IndexInitializer<TestEntity>(
                "working directory",
                mapperHolder,
                new PrimaryKeyFileFactory("working directory", _fileSystem, memory),
                new DataFileFactory("working directory", _fileSystem, memory),
                _fileSystem);
        }

        [Test]
        public void MakeIndex_New()
        {
            _collection.Insert(new TestEntity { Id = 1, Int = 10, Float = 1.0f, String = "1" });
            _collection.Insert(new TestEntity { Id = 2, Int = 10, Float = 2.0f, String = "2" });
            _collection.Insert(new TestEntity { Id = 3, Int = 20, Float = 3.0f, String = "3" });
            _collection.Insert(new TestEntity { Id = 4, Int = 20, Float = 4.0f, String = "4" });
            _collection.Insert(new TestEntity { Id = 5, Int = 20, Float = 5.0f, String = "5" });

            var index = _initializer.GetIndex<int>("test index", x => x.Int, new Expression<Func<TestEntity, object>>[] { x => x.Float, x => x.String });

            Assert.AreEqual(typeof(TestEntity), index.Meta.EntityType);
            Assert.AreEqual(typeof(int), index.Meta.IndexedFieldType);
            Assert.AreEqual(0, index.Meta.IndexedFieldNumber);
            Assert.AreEqual(2, index.Meta.IncludedFieldNumbers.Length);
            Assert.AreEqual(1, index.Meta.IncludedFieldNumbers[0]);
            Assert.AreEqual(2, index.Meta.IncludedFieldNumbers[1]);

            var indexValue = index.GetEquals(10);
            Assert.AreEqual(10, indexValue.IndexedFieldValue);
            Assert.AreEqual(2, indexValue.Items.Count);
            Assert.AreEqual(1, indexValue.Items[0].PrimaryKeyValue);
            Assert.AreEqual(2, indexValue.Items[0].IncludedFields.Length);
            Assert.AreEqual(1.0f, indexValue.Items[0].IncludedFields[0]);
            Assert.AreEqual("1", indexValue.Items[0].IncludedFields[1]);
            Assert.AreEqual(2, indexValue.Items[1].PrimaryKeyValue);
            Assert.AreEqual(2, indexValue.Items[1].IncludedFields.Length);
            Assert.AreEqual(2.0f, indexValue.Items[1].IncludedFields[0]);
            Assert.AreEqual("2", indexValue.Items[1].IncludedFields[1]);

            indexValue = index.GetEquals(20);
            Assert.AreEqual(20, indexValue.IndexedFieldValue);
            Assert.AreEqual(3, indexValue.Items.Count);
            Assert.AreEqual(3, indexValue.Items[0].PrimaryKeyValue);
            Assert.AreEqual(2, indexValue.Items[0].IncludedFields.Length);
            Assert.AreEqual(3.0f, indexValue.Items[0].IncludedFields[0]);
            Assert.AreEqual("3", indexValue.Items[0].IncludedFields[1]);
            Assert.AreEqual(4, indexValue.Items[1].PrimaryKeyValue);
            Assert.AreEqual(2, indexValue.Items[1].IncludedFields.Length);
            Assert.AreEqual(4.0f, indexValue.Items[1].IncludedFields[0]);
            Assert.AreEqual("4", indexValue.Items[1].IncludedFields[1]);
            Assert.AreEqual(5, indexValue.Items[2].PrimaryKeyValue);
            Assert.AreEqual(2, indexValue.Items[2].IncludedFields.Length);
            Assert.AreEqual(5.0f, indexValue.Items[2].IncludedFields[0]);
            Assert.AreEqual("5", indexValue.Items[2].IncludedFields[1]);
        }

        [Test]
        public void MakeIndex_ReadFromFile()
        {
            _collection.Insert(new TestEntity { Id = 1, Int = 10, Float = 1.0f, String = "1" });
            _collection.Insert(new TestEntity { Id = 2, Int = 10, Float = 2.0f, String = "2" });
            _collection.Insert(new TestEntity { Id = 3, Int = 20, Float = 3.0f, String = "3" });
            _collection.Insert(new TestEntity { Id = 4, Int = 20, Float = 4.0f, String = "4" });
            _collection.Insert(new TestEntity { Id = 5, Int = 20, Float = 5.0f, String = "5" });

            _initializer.GetIndex<int>("test index", x => x.Int, new Expression<Func<TestEntity, object>>[] { x => x.Float, x => x.String });
            Assert.AreEqual(2, _fileSystem.FileStreams.First(x => x.FileFullPath == "working directory\\test.primary").ReadCount);
            Assert.AreEqual(1, _fileSystem.FileStreams.First(x => x.FileFullPath == "working directory\\test.data").ReadCount);
            Assert.AreEqual(0, _fileSystem.FileStreams.First(x => x.FileFullPath == "working directory\\test_test index.index").ReadCount);
            Assert.AreEqual(1, _fileSystem.FileStreams.First(x => x.FileFullPath == "working directory\\test_test index.index").WriteCount);
            var index = _initializer.GetIndex<int>("test index", x => x.Int, new Expression<Func<TestEntity, object>>[] { x => x.Float, x => x.String });
            Assert.AreEqual(2, _fileSystem.FileStreams.First(x => x.FileFullPath == "working directory\\test.primary").ReadCount);
            Assert.AreEqual(1, _fileSystem.FileStreams.First(x => x.FileFullPath == "working directory\\test.data").ReadCount);
            Assert.AreEqual(1, _fileSystem.FileStreams.First(x => x.FileFullPath == "working directory\\test_test index.index").ReadCount);
            Assert.AreEqual(1, _fileSystem.FileStreams.First(x => x.FileFullPath == "working directory\\test_test index.index").WriteCount);

            Assert.AreEqual(typeof(TestEntity), index.Meta.EntityType);
            Assert.AreEqual(typeof(int), index.Meta.IndexedFieldType);
            Assert.AreEqual(0, index.Meta.IndexedFieldNumber);
            Assert.AreEqual(2, index.Meta.IncludedFieldNumbers.Length);
            Assert.AreEqual(1, index.Meta.IncludedFieldNumbers[0]);
            Assert.AreEqual(2, index.Meta.IncludedFieldNumbers[1]);

            var indexValue = index.GetEquals(10);
            Assert.AreEqual(10, indexValue.IndexedFieldValue);
            Assert.AreEqual(2, indexValue.Items.Count);
            Assert.AreEqual(1, indexValue.Items[0].PrimaryKeyValue);
            Assert.AreEqual(2, indexValue.Items[0].IncludedFields.Length);
            Assert.AreEqual(1.0f, indexValue.Items[0].IncludedFields[0]);
            Assert.AreEqual("1", indexValue.Items[0].IncludedFields[1]);
            Assert.AreEqual(2, indexValue.Items[1].PrimaryKeyValue);
            Assert.AreEqual(2, indexValue.Items[1].IncludedFields.Length);
            Assert.AreEqual(2.0f, indexValue.Items[1].IncludedFields[0]);
            Assert.AreEqual("2", indexValue.Items[1].IncludedFields[1]);

            indexValue = index.GetEquals(20);
            Assert.AreEqual(20, indexValue.IndexedFieldValue);
            Assert.AreEqual(3, indexValue.Items.Count);
            Assert.AreEqual(3, indexValue.Items[0].PrimaryKeyValue);
            Assert.AreEqual(2, indexValue.Items[0].IncludedFields.Length);
            Assert.AreEqual(3.0f, indexValue.Items[0].IncludedFields[0]);
            Assert.AreEqual("3", indexValue.Items[0].IncludedFields[1]);
            Assert.AreEqual(4, indexValue.Items[1].PrimaryKeyValue);
            Assert.AreEqual(2, indexValue.Items[1].IncludedFields.Length);
            Assert.AreEqual(4.0f, indexValue.Items[1].IncludedFields[0]);
            Assert.AreEqual("4", indexValue.Items[1].IncludedFields[1]);
            Assert.AreEqual(5, indexValue.Items[2].PrimaryKeyValue);
            Assert.AreEqual(2, indexValue.Items[2].IncludedFields.Length);
            Assert.AreEqual(5.0f, indexValue.Items[2].IncludedFields[0]);
            Assert.AreEqual("5", indexValue.Items[2].IncludedFields[1]);
        }

        [Test]
        public void MakeIndex_NoIncluded()
        {
            _collection.Insert(new TestEntity { Id = 1, Int = 10, Float = 1.0f, String = "1" });
            _collection.Insert(new TestEntity { Id = 2, Int = 10, Float = 2.0f, String = "2" });
            _collection.Insert(new TestEntity { Id = 3, Int = 20, Float = 3.0f, String = "3" });
            _collection.Insert(new TestEntity { Id = 4, Int = 20, Float = 4.0f, String = "4" });
            _collection.Insert(new TestEntity { Id = 5, Int = 20, Float = 5.0f, String = "5" });

            var index = _initializer.GetIndex<int>("test index", x => x.Int, null);

            var indexValue = index.GetEquals(10);
            Assert.AreEqual(10, indexValue.IndexedFieldValue);
            Assert.AreEqual(2, indexValue.Items.Count);
            Assert.AreEqual(1, indexValue.Items[0].PrimaryKeyValue);
            Assert.AreEqual(0, indexValue.Items[0].IncludedFields.Length);
            Assert.AreEqual(2, indexValue.Items[1].PrimaryKeyValue);
            Assert.AreEqual(0, indexValue.Items[1].IncludedFields.Length);

            indexValue = index.GetEquals(20);
            Assert.AreEqual(20, indexValue.IndexedFieldValue);
            Assert.AreEqual(3, indexValue.Items.Count);
            Assert.AreEqual(3, indexValue.Items[0].PrimaryKeyValue);
            Assert.AreEqual(0, indexValue.Items[0].IncludedFields.Length);
            Assert.AreEqual(4, indexValue.Items[1].PrimaryKeyValue);
            Assert.AreEqual(0, indexValue.Items[1].IncludedFields.Length);
            Assert.AreEqual(5, indexValue.Items[2].PrimaryKeyValue);
            Assert.AreEqual(0, indexValue.Items[2].IncludedFields.Length);
        }
    }

    class TestEntity
    {
        public int Id { get; set; }
        public int Int { get; set; }
        public float Float { get; set; }
        public string String { get; set; }
    }
}
