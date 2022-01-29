using NUnit.Framework;
using SimpleDB.Core;
using SimpleDB.IndexedSearch;
using SimpleDB.Test.Tools;

namespace SimpleDB.Test.IndexedSearch
{
    class IndexUpdaterTest
    {
        private Mapper<TestEntity> _mapper;
        private IndexUpdater _indexUpdater;
        private Index<int> _indexInt;
        private Index<double> _indexDouble;

        [SetUp]
        public void Setup()
        {
            var fileSystem = new MemoryFileSystem();
            _mapper = new Mapper<TestEntity>(
                new PrimaryKeyMapping<TestEntity>(entity => entity.Id),
                new FieldMapping<TestEntity>[]
                {
                    new FieldMapping<TestEntity>(1, entity => entity.Int),
                    new FieldMapping<TestEntity>(2, entity => entity.Double),
                });
            _indexInt = new Index<int>(new IndexMeta
            {
                EntityName = "TestEntity",
                Name = "index int",
                IndexedFieldType = typeof(int),
                IndexedFieldNumber = 1,
                IncludedFieldNumbers = new byte[] { 2 }
            });
            _indexDouble = new Index<double>(new IndexMeta
            {
                EntityName = "TestEntity",
                Name = "index double",
                IndexedFieldType = typeof(double),
                IndexedFieldNumber = 2,
                IncludedFieldNumbers = new byte[] { 1 }
            });
            _indexUpdater = new IndexUpdater(
                new IIndex[] { _indexInt, _indexDouble },
                new IndexFileFactory(fileSystem));
        }

        [Test]
        public void AddToIndexes()
        {
            var entity = new TestEntity { Id = 1, Int = 10, Double = 1.2 };
            _indexUpdater.AddToIndexes(_mapper, new[] { entity });

            var intResult = _indexInt.GetEquals(10);
            Assert.AreEqual(10, intResult.IndexedFieldValue);
            Assert.AreEqual(1, intResult.Items.Count);
            Assert.AreEqual(1, intResult.Items[0].PrimaryKeyValue);
            Assert.AreEqual(1, intResult.Items[0].IncludedFields.Length);
            Assert.AreEqual(1.2, intResult.Items[0].IncludedFields[0]);

            var doubleResult = _indexDouble.GetEquals(1.2);
            Assert.AreEqual(1.2, doubleResult.IndexedFieldValue);
            Assert.AreEqual(1, doubleResult.Items.Count);
            Assert.AreEqual(1, doubleResult.Items[0].PrimaryKeyValue);
            Assert.AreEqual(1, doubleResult.Items[0].IncludedFields.Length);
            Assert.AreEqual(10, doubleResult.Items[0].IncludedFields[0]);
        }

        [Test]
        public void UpdateIndexes_Entity()
        {
            _indexInt.Add(10, new IndexItem { PrimaryKeyValue = 1, IncludedFields = new object[] { 1.2 } });
            _indexDouble.Add(1.2, new IndexItem { PrimaryKeyValue = 1, IncludedFields = new object[] { 10 } });
            var entity = new TestEntity { Id = 1, Int = 100, Double = 10.2 };
            _indexUpdater.UpdateIndexes(_mapper, new[] { entity });

            Assert.AreEqual(null, _indexInt.GetEquals(10));
            Assert.AreEqual(null, _indexDouble.GetEquals(1.2));

            var intResult = _indexInt.GetEquals(100);
            Assert.AreEqual(100, intResult.IndexedFieldValue);
            Assert.AreEqual(1, intResult.Items.Count);
            Assert.AreEqual(1, intResult.Items[0].PrimaryKeyValue);
            Assert.AreEqual(1, intResult.Items[0].IncludedFields.Length);
            Assert.AreEqual(10.2, intResult.Items[0].IncludedFields[0]);

            var doubleResult = _indexDouble.GetEquals(10.2);
            Assert.AreEqual(10.2, doubleResult.IndexedFieldValue);
            Assert.AreEqual(1, doubleResult.Items.Count);
            Assert.AreEqual(1, doubleResult.Items[0].PrimaryKeyValue);
            Assert.AreEqual(1, doubleResult.Items[0].IncludedFields.Length);
            Assert.AreEqual(100, doubleResult.Items[0].IncludedFields[0]);
        }

        [Test]
        public void UpdateIndexes_Fields()
        {
            _indexInt.Add(10, new IndexItem { PrimaryKeyValue = 1, IncludedFields = new object[] { 1.2 } });
            _indexDouble.Add(1.2, new IndexItem { PrimaryKeyValue = 1, IncludedFields = new object[] { 10 } });
            _indexUpdater.UpdateIndexes(_mapper.EntityMeta, new object[] { 1 }, new FieldValue[] { new FieldValue(1, 100), new FieldValue(2, 10.2) });

            Assert.AreEqual(null, _indexInt.GetEquals(10));
            Assert.AreEqual(null, _indexDouble.GetEquals(1.2));

            var intResult = _indexInt.GetEquals(100);
            Assert.AreEqual(100, intResult.IndexedFieldValue);
            Assert.AreEqual(1, intResult.Items.Count);
            Assert.AreEqual(1, intResult.Items[0].PrimaryKeyValue);
            Assert.AreEqual(1, intResult.Items[0].IncludedFields.Length);
            Assert.AreEqual(10.2, intResult.Items[0].IncludedFields[0]);

            var doubleResult = _indexDouble.GetEquals(10.2);
            Assert.AreEqual(10.2, doubleResult.IndexedFieldValue);
            Assert.AreEqual(1, doubleResult.Items.Count);
            Assert.AreEqual(1, doubleResult.Items[0].PrimaryKeyValue);
            Assert.AreEqual(1, doubleResult.Items[0].IncludedFields.Length);
            Assert.AreEqual(100, doubleResult.Items[0].IncludedFields[0]);
        }

        [Test]
        public void UpdateIndexes_Fields_NoIncluded()
        {
            _indexInt.Add(10, new IndexItem { PrimaryKeyValue = 1, IncludedFields = new object[] { 1.2 } });
            _indexDouble.Add(1.2, new IndexItem { PrimaryKeyValue = 1, IncludedFields = new object[] { 10 } });
            _indexUpdater.UpdateIndexes(_mapper.EntityMeta, new object[] { 1 }, new FieldValue[] { new FieldValue(1, 100) });

            Assert.AreEqual(null, _indexInt.GetEquals(10));

            var intResult = _indexInt.GetEquals(100);
            Assert.AreEqual(100, intResult.IndexedFieldValue);
            Assert.AreEqual(1, intResult.Items.Count);
            Assert.AreEqual(1, intResult.Items[0].PrimaryKeyValue);
            Assert.AreEqual(1, intResult.Items[0].IncludedFields.Length);
            Assert.AreEqual(1.2, intResult.Items[0].IncludedFields[0]);

            var doubleResult = _indexDouble.GetEquals(1.2);
            Assert.AreEqual(1.2, doubleResult.IndexedFieldValue);
            Assert.AreEqual(1, doubleResult.Items.Count);
            Assert.AreEqual(1, doubleResult.Items[0].PrimaryKeyValue);
            Assert.AreEqual(1, doubleResult.Items[0].IncludedFields.Length);
            Assert.AreEqual(100, doubleResult.Items[0].IncludedFields[0]);
        }

        [Test]
        public void DeleteFromIndexes()
        {
            _indexInt.Add(10, new IndexItem { PrimaryKeyValue = 1, IncludedFields = new object[] { 1.2 } });
            _indexDouble.Add(1.2, new IndexItem { PrimaryKeyValue = 1, IncludedFields = new object[] { 10 } });
            _indexUpdater.DeleteFromIndexes(_mapper.EntityMeta, new object[] { 1 });
            Assert.AreEqual(null, _indexInt.GetEquals(10));
            Assert.AreEqual(null, _indexDouble.GetEquals(1.2));
        }

        class TestEntity
        {
            public int Id { get; set; }
            public int Int { get; set; }
            public double Double { get; set; }
        }
    }
}
