using NUnit.Framework;
using SimpleDB.Core;
using SimpleDB.IndexedSearch;
using SimpleDB.Infrastructure;
using SimpleDB.Queries;
using SimpleDB.QueryExecutors;
using SimpleDB.Test.Tools;

namespace SimpleDB.Test.QueryExecutors
{
    class UpdateQueryExecutorTest
    {
        private readonly string _workingDirectory = "working directory";
        private MemoryFileSystem _fileSystem;
        private Memory _memory;
        private Mapper<TestEntity> _mapper;
        private Collection<TestEntity> _collection;
        private UpdateQueryExecutor<TestEntity> _queryExecutor;

        [SetUp]
        public void Setup()
        {
            _fileSystem = new MemoryFileSystem();
            _memory = Memory.Instance;
            _mapper = new Mapper<TestEntity>(
                new PrimaryKeyMapping<TestEntity>(x => x.Id),
                new FieldMapping<TestEntity>[]
                {
                    new FieldMapping<TestEntity>(0, x => x.Byte),
                    new FieldMapping<TestEntity>(1, x => x.Float),
                    new FieldMapping<TestEntity>(2, x => x.String),
                    new FieldMapping<TestEntity>(3, x => x.InnerObject)
                });
            _collection = new Collection<TestEntity>(
                _mapper,
                new PrimaryKeyFileFactory(_workingDirectory, _fileSystem, _memory),
                new DataFileFactory(_workingDirectory, _fileSystem, _memory),
                new MetaFileFactory(_workingDirectory, _fileSystem));
            _queryExecutor = new UpdateQueryExecutor<TestEntity>( _mapper, _collection.PrimaryKeyFile, _collection.DataFile, _collection.PrimaryKeys, new IndexHolder(), new IndexUpdater());
        }

        [Test]
        public void ExecuteQuery()
        {
            _collection.Insert(new TestEntity { Id = 1, Byte = 10, Float = 1.2f, String = "123" });
            _collection.Insert(new TestEntity { Id = 2, Byte = 20, Float = 3.4f, String = "456" });
            _collection.Insert(new TestEntity { Id = 3, Byte = 30, Float = 5.6f, String = "789" });
            var query = new UpdateQuery(new UpdateClause(new[] { new UpdateClause.Field(0, (byte)100), new UpdateClause.Field(1, 34.5f) }));

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
            var query = new UpdateQuery(new UpdateClause(new[] { new UpdateClause.Field(0, (byte)100), new UpdateClause.Field(1, 34.5f) }))
            {
                WhereClause = new WhereClause(new WhereClause.EqualsOperation(new WhereClause.Field(0), new WhereClause.Constant((byte)10)))
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
            var query = new UpdateQuery(new UpdateClause(new[] { new UpdateClause.Field(2, "987") }))
            {
                WhereClause = new WhereClause(new WhereClause.EqualsOperation(new WhereClause.Field(2), new WhereClause.Constant("123")))
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
            var query = new UpdateQuery(new UpdateClause(new[] { new UpdateClause.Field(2, "000") }));

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
            var query = new UpdateQuery(new UpdateClause(new[] { new UpdateClause.Field(2, "0") }));

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
            var query = new UpdateQuery(new UpdateClause(new[] { new UpdateClause.Field(2, "0000000000") }));

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
        public void ExecuteQuery_Object()
        {
            _collection.Insert(new TestEntity { Id = 1, Byte = 10, Float = 1.2f, String = "123", InnerObject = new InnerObject { Value = 123 } });
            _collection.Insert(new TestEntity { Id = 2, Byte = 20, Float = 3.4f, String = "456", InnerObject = new InnerObject { Value = 456 } });
            _collection.Insert(new TestEntity { Id = 3, Byte = 30, Float = 5.6f, String = "789", InnerObject = new InnerObject { Value = 789 } });
            var query = new UpdateQuery(new UpdateClause(new[] { new UpdateClause.Field(3, new InnerObject { Value = 111 }) }));

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
            var query = new UpdateQuery(new UpdateClause(new[] { new UpdateClause.Field(3, new InnerObject { Value = 1 }) }));

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
            var query = new UpdateQuery(new UpdateClause(new[] { new UpdateClause.Field(3, new InnerObject { Value = 123456789 }) }));

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
            var index = new Index<byte>(new IndexMeta
            {
                EntityType = typeof(TestEntity),
                Name = "index",
                IndexedFieldType = typeof(byte),
                IndexedFieldNumber = 0,
                IncludedFieldNumbers = new byte[] { 1 }
            });
            var indexHolder = new IndexHolder(new IIndex[] { index });
            var indexUpdater = new IndexUpdater(
                new IIndex[] { index },
                new MapperHolder(new[] { _mapper }),
                new IndexFileFactory(_workingDirectory, _fileSystem));

            _collection = new Collection<TestEntity>(
                _mapper,
                new PrimaryKeyFileFactory(_workingDirectory, _fileSystem, _memory),
                new DataFileFactory(_workingDirectory, _fileSystem, _memory),
                new MetaFileFactory(_workingDirectory, _fileSystem),
                indexHolder,
                indexUpdater);
            _collection.Insert(new TestEntity { Id = 1, Byte = 10, Float = 1.2f });

            var intResult = index.GetEquals((byte)10);
            Assert.AreEqual((byte)10, intResult.IndexedFieldValue);
            Assert.AreEqual(1, intResult.Items.Count);
            Assert.AreEqual(1, intResult.Items[0].PrimaryKeyValue);
            Assert.AreEqual(1, intResult.Items[0].IncludedFields.Length);
            Assert.AreEqual(1.2f, intResult.Items[0].IncludedFields[0]);
        }

        class TestEntity
        {
            public int Id { get; set; }

            public byte Byte { get; set; }

            public float Float { get; set; }

            public string String { get; set; }

            public InnerObject InnerObject { get; set; }
        }

        class InnerObject
        {
            public int Value { get; set; }
        }
    }
}
