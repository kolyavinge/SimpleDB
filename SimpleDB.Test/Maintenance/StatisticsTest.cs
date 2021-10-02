using System.Linq;
using NUnit.Framework;
using SimpleDB.Core;
using SimpleDB.Infrastructure;
using SimpleDB.Maintenance;
using SimpleDB.Test.Tools;

namespace SimpleDB.Test.Maintenance
{
    class StatisticsTest
    {
        private Mapper<TestEntity> _mapper;
        private Collection<TestEntity> _collection;
        private Statistics _statistics;

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
            _statistics = new Statistics("working directory");
        }

        [Test]
        public void GetPrimaryKeyFileStatistics_Insert()
        {
            _collection.Insert(new TestEntity { Id = 1 });
            var result = _statistics.GetPrimaryKeyFileStatistics().ToList();
            Assert.AreEqual(21, result.First().TotalFileSizeInBytes);
            Assert.AreEqual(0, result.First().FragmentationSizeInBytes);
            Assert.AreEqual(0, result.First().FragmentationPercent);
        }

        [Test]
        public void GetPrimaryKeyFileStatistics_InsertAndDelete()
        {
            _collection.Insert(new TestEntity { Id = 1 });
            _collection.Delete(1);
            var result = _statistics.GetPrimaryKeyFileStatistics().ToList();
            Assert.AreEqual(21, result.First().TotalFileSizeInBytes);
            Assert.AreEqual(21, result.First().FragmentationSizeInBytes);
            Assert.AreEqual(100.0, result.First().FragmentationPercent);
        }

        [Test]
        public void GetDataFileStatistics_Insert()
        {
            _collection.Insert(new TestEntity { Id = 1 });
            var result = _statistics.GetDataFileStatistics().ToList();
            Assert.AreEqual(15, result.First().TotalFileSizeInBytes);
            Assert.AreEqual(0, result.First().FragmentationSizeInBytes);
        }

        [Test]
        public void GetDataFileStatistics_InsertAndDelete()
        {
            _collection.Insert(new TestEntity { Id = 1 });
            _collection.Delete(1);
            var result = _statistics.GetDataFileStatistics().ToList();
            Assert.AreEqual(15, result.First().TotalFileSizeInBytes);
            Assert.AreEqual(15, result.First().FragmentationSizeInBytes);
        }

        [Test]
        public void GetDataFileStatistics_InsertAndUpdate()
        {
            _collection.Insert(new TestEntity { Id = 1, String = "0" });
            Assert.AreEqual(16, _collection.DataFile.SizeInBytes);

            _collection.Update(new TestEntity { Id = 1, String = "0123456789" });

            var result = _statistics.GetDataFileStatistics().ToList();
            Assert.AreEqual(41, result.First().TotalFileSizeInBytes);
            Assert.AreEqual(16, result.First().FragmentationSizeInBytes);
        }

        [Test]
        public void GetDataFileStatistics_UnusedFields()
        {
            _collection.Insert(new TestEntity { Id = 1, String = "0123456789" });

            _mapper = new Mapper<TestEntity>(
               "testEntity",
               new PrimaryKeyMapping<TestEntity>(entity => entity.Id),
               new FieldMapping<TestEntity>[]
               {
                    new FieldMapping<TestEntity>(0, entity => entity.Byte),
                    new FieldMapping<TestEntity>(1, entity => entity.Float)
                    // убрали поле String
               });
            _collection = new Collection<TestEntity>(_mapper); // пересохранили meta файл

            var result = _statistics.GetDataFileStatistics().ToList();
            Assert.AreEqual(16, result.First().FragmentationSizeInBytes);
            Assert.AreEqual(25, result.First().TotalFileSizeInBytes);
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
