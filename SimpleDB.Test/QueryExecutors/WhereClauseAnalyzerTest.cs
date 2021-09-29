using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;
using SimpleDB.Core;
using SimpleDB.Infrastructure;
using SimpleDB.Queries;
using SimpleDB.QueryExecutors;
using SimpleDB.Test.Tools;

namespace SimpleDB.Test.QueryExecutors
{
    class WhereClauseAnalyzerTest
    {
        private Mapper<TestEntity> _mapper;
        private Collection<TestEntity> _collection;
        private IndexHolder _indexHolder;
        private WhereClauseAnalyzer _analyzer;
        private TestFieldValueReader _testFieldValueReader;

        [SetUp]
        public void Setup()
        {
            IOC.Reset();
            IOC.Set<IMemory>(new Memory());
            IOC.Set<IFileSystem>(new MemoryFileSystem());
            _mapper = new Mapper<TestEntity>(
                "testEntity",
                new PrimaryKeyMapping<TestEntity>(entity => entity.Id),
                new FieldMapping<TestEntity>[]
                {
                    new FieldMapping<TestEntity>(0, entity => entity.A),
                    new FieldMapping<TestEntity>(1, entity => entity.B),
                    new FieldMapping<TestEntity>(2, entity => entity.C),
                    new FieldMapping<TestEntity>(3, entity => entity.D),
                    new FieldMapping<TestEntity>(4, entity => entity.S),
                });
            _collection = new Collection<TestEntity>("working directory", _mapper);
            _collection.Insert(new TestEntity { Id = 10, A = 1, B = 2, C = 3, D = 4, S = "123" });
            _collection.Insert(new TestEntity { Id = 20, A = 6, B = 7, C = 8, D = 9, S = "987" });

            var indexA = new Index<int>(new IndexMeta { EntityType = typeof(TestEntity), Name = "indexA", IndexedFieldType = typeof(int), IndexedFieldNumber = 0 });
            indexA.Insert(new IndexValue { IndexedFieldValue = 1, Items = new List<IndexItem> { new IndexItem { PrimaryKeyValue = 10 } } });
            indexA.Insert(new IndexValue { IndexedFieldValue = 6, Items = new List<IndexItem> { new IndexItem { PrimaryKeyValue = 20 } } });

            var indexB = new Index<int>(new IndexMeta { EntityType = typeof(TestEntity), Name = "indexB", IndexedFieldType = typeof(int), IndexedFieldNumber = 1 });
            indexB.Insert(new IndexValue { IndexedFieldValue = 2, Items = new List<IndexItem> { new IndexItem { PrimaryKeyValue = 10 } } });
            indexB.Insert(new IndexValue { IndexedFieldValue = 7, Items = new List<IndexItem> { new IndexItem { PrimaryKeyValue = 20 } } });

            var indexS = new Index<string>(new IndexMeta { EntityType = typeof(TestEntity), Name = "indexS", IndexedFieldType = typeof(string), IndexedFieldNumber = 4 });
            indexS.Insert(new IndexValue { IndexedFieldValue = "123", Items = new List<IndexItem> { new IndexItem { PrimaryKeyValue = 10 } } });
            indexS.Insert(new IndexValue { IndexedFieldValue = "987", Items = new List<IndexItem> { new IndexItem { PrimaryKeyValue = 20 } } });

            _indexHolder = new IndexHolder(new AbstractIndex[] { indexA, indexB, indexS });

            _testFieldValueReader = new TestFieldValueReader(new FieldValueReader(_collection.DataFile));
            _analyzer = new WhereClauseAnalyzer(typeof(TestEntity), _collection.PrimaryKeys, _testFieldValueReader, _indexHolder);
        }

        [Test]
        public void OneIndexed()
        {
            var where = new WhereClause(new WhereClause.EqualsOperation(new WhereClause.Field(0), new WhereClause.Constant(1)));
            var result = _analyzer.GetResult(where).ToList();
            Assert.AreEqual(1, result.Count);
            Assert.AreEqual(10, result[0].PrimaryKey.Value);
            Assert.AreEqual(0, result[0][0].Number);
            Assert.AreEqual(1, result[0][0].Value);
            Assert.AreEqual(0, _testFieldValueReader.CallsCount);
        }

        [Test]
        public void And_OneIndexedAndOneOrdinary()
        {
            var where = new WhereClause(
                new WhereClause.AndOperation(
                    new WhereClause.EqualsOperation(new WhereClause.Field(0), new WhereClause.Constant(1)),
                    new WhereClause.EqualsOperation(new WhereClause.Field(2), new WhereClause.Constant(3))));
            var result = _analyzer.GetResult(where).ToList();
            Assert.AreEqual(1, result.Count);
            Assert.AreEqual(10, result[0].PrimaryKey.Value);
            Assert.AreEqual(0, result[0][0].Number);
            Assert.AreEqual(1, result[0][0].Value);
            Assert.AreEqual(2, result[0][2].Number);
            Assert.AreEqual(3, result[0][2].Value);
            Assert.AreEqual(1, _testFieldValueReader.CallsCount);
        }

        [Test]
        public void And_TwoIndexed()
        {
            var where = new WhereClause(
                new WhereClause.AndOperation(
                    new WhereClause.EqualsOperation(new WhereClause.Field(0), new WhereClause.Constant(1)),
                    new WhereClause.EqualsOperation(new WhereClause.Field(1), new WhereClause.Constant(2))));
            var result = _analyzer.GetResult(where).ToList();
            Assert.AreEqual(1, result.Count);
            Assert.AreEqual(10, result[0].PrimaryKey.Value);
            Assert.AreEqual(0, result[0][0].Number);
            Assert.AreEqual(1, result[0][0].Value);
            Assert.AreEqual(1, result[0][1].Number);
            Assert.AreEqual(2, result[0][1].Value);
            Assert.AreEqual(0, _testFieldValueReader.CallsCount);
        }

        [Test]
        public void And_TwoIndexedOneOrdinary_1()
        {
            var where = new WhereClause(
                new WhereClause.AndOperation(
                    new WhereClause.AndOperation(
                        new WhereClause.EqualsOperation(new WhereClause.Field(0), new WhereClause.Constant(1)),
                        new WhereClause.EqualsOperation(new WhereClause.Field(1), new WhereClause.Constant(2))),
                    new WhereClause.EqualsOperation(new WhereClause.Field(2), new WhereClause.Constant(3))));
            var result = _analyzer.GetResult(where).ToList();
            Assert.AreEqual(1, result.Count);
            Assert.AreEqual(10, result[0].PrimaryKey.Value);
            Assert.AreEqual(0, result[0][0].Number);
            Assert.AreEqual(1, result[0][0].Value);
            Assert.AreEqual(1, result[0][1].Number);
            Assert.AreEqual(2, result[0][1].Value);
            Assert.AreEqual(2, result[0][2].Number);
            Assert.AreEqual(3, result[0][2].Value);
            Assert.AreEqual(1, _testFieldValueReader.CallsCount);
        }

        [Test]
        public void And_TwoIndexedOneOrdinary_2()
        {
            var where = new WhereClause(
                new WhereClause.AndOperation(
                    new WhereClause.AndOperation(
                        new WhereClause.EqualsOperation(new WhereClause.Field(0), new WhereClause.Constant(1)),
                        new WhereClause.EqualsOperation(new WhereClause.Field(2), new WhereClause.Constant(3))),
                    new WhereClause.EqualsOperation(new WhereClause.Field(1), new WhereClause.Constant(2))));
            var result = _analyzer.GetResult(where).ToList();
            Assert.AreEqual(1, result.Count);
            Assert.AreEqual(10, result[0].PrimaryKey.Value);
            Assert.AreEqual(0, result[0][0].Number);
            Assert.AreEqual(1, result[0][0].Value);
            Assert.AreEqual(1, result[0][1].Number);
            Assert.AreEqual(2, result[0][1].Value);
            Assert.AreEqual(2, result[0][2].Number);
            Assert.AreEqual(3, result[0][2].Value);
            Assert.AreEqual(1, _testFieldValueReader.CallsCount);
        }

        [Test]
        public void Or_OneIndexedAndOneOrdinary_1()
        {
            var where = new WhereClause(
                new WhereClause.OrOperation(
                    new WhereClause.EqualsOperation(new WhereClause.Field(0), new WhereClause.Constant(1)),
                    new WhereClause.EqualsOperation(new WhereClause.Field(2), new WhereClause.Constant(3))));
            var result = _analyzer.GetResult(where).ToList();
            Assert.AreEqual(1, result.Count);
            Assert.AreEqual(10, result[0].PrimaryKey.Value);
            Assert.AreEqual(0, result[0][0].Number);
            Assert.AreEqual(1, result[0][0].Value);
            Assert.AreEqual(1, _testFieldValueReader.CallsCount);
        }

        [Test]
        public void Or_OneIndexedAndOneOrdinary_2()
        {
            var where = new WhereClause(
                new WhereClause.OrOperation(
                    new WhereClause.EqualsOperation(new WhereClause.Field(0), new WhereClause.Constant(1)),
                    new WhereClause.EqualsOperation(new WhereClause.Field(2), new WhereClause.Constant(8))));
            var result = _analyzer.GetResult(where).ToList();
            Assert.AreEqual(2, result.Count);
            Assert.AreEqual(10, result[0].PrimaryKey.Value);
            Assert.AreEqual(0, result[0][0].Number);
            Assert.AreEqual(1, result[0][0].Value);
            Assert.AreEqual(20, result[1].PrimaryKey.Value);
            Assert.AreEqual(2, result[1][2].Number);
            Assert.AreEqual(8, result[1][2].Value);
            Assert.AreEqual(1, _testFieldValueReader.CallsCount);
        }

        [Test]
        public void Or_TwoIndexed()
        {
            var where = new WhereClause(
                new WhereClause.OrOperation(
                    new WhereClause.EqualsOperation(new WhereClause.Field(0), new WhereClause.Constant(1)),
                    new WhereClause.EqualsOperation(new WhereClause.Field(1), new WhereClause.Constant(2))));
            var result = _analyzer.GetResult(where).ToList();
            Assert.AreEqual(1, result.Count);
            Assert.AreEqual(10, result[0].PrimaryKey.Value);
            Assert.AreEqual(0, result[0][0].Number);
            Assert.AreEqual(1, result[0][0].Value);
            Assert.AreEqual(1, result[0][1].Number);
            Assert.AreEqual(2, result[0][1].Value);
            Assert.AreEqual(0, _testFieldValueReader.CallsCount);
        }

        [Test]
        public void Or_TwoIndexedOneOrdinary_1()
        {
            var where = new WhereClause(
                new WhereClause.OrOperation(
                    new WhereClause.OrOperation(
                        new WhereClause.EqualsOperation(new WhereClause.Field(0), new WhereClause.Constant(1)),
                        new WhereClause.EqualsOperation(new WhereClause.Field(1), new WhereClause.Constant(2))),
                    new WhereClause.EqualsOperation(new WhereClause.Field(2), new WhereClause.Constant(3))));
            var result = _analyzer.GetResult(where).ToList();
            Assert.AreEqual(1, result.Count);
            Assert.AreEqual(10, result[0].PrimaryKey.Value);
            Assert.AreEqual(0, result[0][0].Number);
            Assert.AreEqual(1, result[0][0].Value);
            Assert.AreEqual(1, result[0][1].Number);
            Assert.AreEqual(2, result[0][1].Value);
            Assert.AreEqual(1, _testFieldValueReader.CallsCount);
        }

        [Test]
        public void Or_TwoIndexedOneOrdinary_2()
        {
            var where = new WhereClause(
                new WhereClause.OrOperation(
                    new WhereClause.OrOperation(
                        new WhereClause.EqualsOperation(new WhereClause.Field(0), new WhereClause.Constant(1)),
                        new WhereClause.EqualsOperation(new WhereClause.Field(2), new WhereClause.Constant(3))),
                    new WhereClause.EqualsOperation(new WhereClause.Field(1), new WhereClause.Constant(2))));
            var result = _analyzer.GetResult(where).ToList();
            Assert.AreEqual(1, result.Count);
            Assert.AreEqual(10, result[0].PrimaryKey.Value);
            Assert.AreEqual(0, result[0][0].Number);
            Assert.AreEqual(1, result[0][0].Value);
            Assert.AreEqual(1, result[0][1].Number);
            Assert.AreEqual(2, result[0][1].Value);
            Assert.AreEqual(1, _testFieldValueReader.CallsCount);
        }

        [Test]
        public void AndOr_OneIndexedTwoOrdinary()
        {
            var where = new WhereClause(
                new WhereClause.AndOperation(
                    new WhereClause.OrOperation(
                        new WhereClause.EqualsOperation(new WhereClause.Field(0), new WhereClause.Constant(1)),
                        new WhereClause.EqualsOperation(new WhereClause.Field(2), new WhereClause.Constant(8))),
                    new WhereClause.EqualsOperation(new WhereClause.Field(1), new WhereClause.Constant(7))));
            var result = _analyzer.GetResult(where).ToList();
            Assert.AreEqual(1, result.Count);
            Assert.AreEqual(20, result[0].PrimaryKey.Value);
            Assert.AreEqual(1, result[0][1].Number);
            Assert.AreEqual(7, result[0][1].Value);
            Assert.AreEqual(2, result[0][2].Number);
            Assert.AreEqual(8, result[0][2].Value);
            Assert.AreEqual(1, _testFieldValueReader.CallsCount);
        }

        [Test]
        public void OrAnd_OneIndexedTwoOrdinary()
        {
            var where = new WhereClause(
                new WhereClause.OrOperation(
                    new WhereClause.AndOperation(
                        new WhereClause.EqualsOperation(new WhereClause.Field(0), new WhereClause.Constant(1)),
                        new WhereClause.EqualsOperation(new WhereClause.Field(2), new WhereClause.Constant(3))),
                    new WhereClause.EqualsOperation(new WhereClause.Field(1), new WhereClause.Constant(7))));
            var result = _analyzer.GetResult(where).ToList();
            Assert.AreEqual(2, result.Count);
            Assert.AreEqual(20, result[0].PrimaryKey.Value);
            Assert.AreEqual(1, result[0][1].Number);
            Assert.AreEqual(7, result[0][1].Value);
            Assert.AreEqual(10, result[1].PrimaryKey.Value);
            Assert.AreEqual(0, result[1][0].Number);
            Assert.AreEqual(1, result[1][0].Value);
            Assert.AreEqual(2, result[1][2].Number);
            Assert.AreEqual(3, result[1][2].Value);
            Assert.AreEqual(1, _testFieldValueReader.CallsCount);
        }

        [Test]
        public void NotAnd()
        {
            var where = new WhereClause(
                new WhereClause.NotOperation(
                    new WhereClause.AndOperation(
                        new WhereClause.EqualsOperation(new WhereClause.Field(2), new WhereClause.Constant(3)),
                        new WhereClause.EqualsOperation(new WhereClause.Field(2), new WhereClause.Constant(8)))));
            var result = _analyzer.GetResult(where).ToList();
            Assert.AreEqual(2, result.Count);
            Assert.AreEqual(10, result[0].PrimaryKey.Value);
            Assert.AreEqual(2, result[0][2].Number);
            Assert.AreEqual(3, result[0][2].Value);
            Assert.AreEqual(20, result[1].PrimaryKey.Value);
            Assert.AreEqual(2, result[1][2].Number);
            Assert.AreEqual(8, result[1][2].Value);
            Assert.AreEqual(1, _testFieldValueReader.CallsCount);
        }

        [Test]
        public void NotOr()
        {
            var where = new WhereClause(
                new WhereClause.NotOperation(
                    new WhereClause.OrOperation(
                        new WhereClause.EqualsOperation(new WhereClause.Field(2), new WhereClause.Constant(3)),
                        new WhereClause.EqualsOperation(new WhereClause.Field(3), new WhereClause.Constant(4)))));
            var result = _analyzer.GetResult(where).ToList();
            Assert.AreEqual(1, result.Count);
            Assert.AreEqual(20, result[0].PrimaryKey.Value);
            Assert.AreEqual(2, result[0][2].Number);
            Assert.AreEqual(8, result[0][2].Value);
            Assert.AreEqual(3, result[0][3].Number);
            Assert.AreEqual(9, result[0][3].Value);
            Assert.AreEqual(1, _testFieldValueReader.CallsCount);
        }

        [Test]
        public void LessOperation()
        {
            var where = new WhereClause(
                new WhereClause.LessOperation(
                    new WhereClause.Field(0), new WhereClause.Constant(6)));
            var result = _analyzer.GetResult(where).ToList();
            Assert.AreEqual(1, result.Count);
            Assert.AreEqual(10, result[0].PrimaryKey.Value);
            Assert.AreEqual(0, _testFieldValueReader.CallsCount);
        }

        [Test]
        public void NotLessOperation()
        {
            var where = new WhereClause(
                new WhereClause.NotOperation(
                    new WhereClause.LessOperation(
                        new WhereClause.Field(0), new WhereClause.Constant(1))));
            var result = _analyzer.GetResult(where).ToList();
            Assert.AreEqual(2, result.Count);
            Assert.AreEqual(10, result[0].PrimaryKey.Value);
            Assert.AreEqual(20, result[1].PrimaryKey.Value);
            Assert.AreEqual(0, _testFieldValueReader.CallsCount);
        }

        [Test]
        public void GreatOperation()
        {
            var where = new WhereClause(
                new WhereClause.GreatOperation(
                    new WhereClause.Field(0), new WhereClause.Constant(1)));
            var result = _analyzer.GetResult(where).ToList();
            Assert.AreEqual(1, result.Count);
            Assert.AreEqual(20, result[0].PrimaryKey.Value);
            Assert.AreEqual(0, _testFieldValueReader.CallsCount);
        }

        [Test]
        public void NotGreatOperation()
        {
            var where = new WhereClause(
                new WhereClause.NotOperation(
                    new WhereClause.GreatOperation(
                        new WhereClause.Field(0), new WhereClause.Constant(6))));
            var result = _analyzer.GetResult(where).ToList();
            Assert.AreEqual(2, result.Count);
            Assert.AreEqual(10, result[0].PrimaryKey.Value);
            Assert.AreEqual(20, result[1].PrimaryKey.Value);
            Assert.AreEqual(0, _testFieldValueReader.CallsCount);
        }

        [Test]
        public void LessOrEqualsOperation()
        {
            var where = new WhereClause(
                new WhereClause.LessOrEqualsOperation(
                    new WhereClause.Field(0), new WhereClause.Constant(6)));
            var result = _analyzer.GetResult(where).ToList();
            Assert.AreEqual(2, result.Count);
            Assert.AreEqual(10, result[0].PrimaryKey.Value);
            Assert.AreEqual(20, result[1].PrimaryKey.Value);
            Assert.AreEqual(0, _testFieldValueReader.CallsCount);
        }

        [Test]
        public void NotLessOrEqualsOperation()
        {
            var where = new WhereClause(
                new WhereClause.NotOperation(
                    new WhereClause.LessOrEqualsOperation(
                        new WhereClause.Field(0), new WhereClause.Constant(1))));
            var result = _analyzer.GetResult(where).ToList();
            Assert.AreEqual(1, result.Count);
            Assert.AreEqual(20, result[0].PrimaryKey.Value);
            Assert.AreEqual(0, _testFieldValueReader.CallsCount);
        }

        [Test]
        public void GreatOrEqualsOperation()
        {
            var where = new WhereClause(
                new WhereClause.GreatOrEqualsOperation(
                    new WhereClause.Field(0), new WhereClause.Constant(1)));
            var result = _analyzer.GetResult(where).ToList();
            Assert.AreEqual(2, result.Count);
            Assert.AreEqual(10, result[0].PrimaryKey.Value);
            Assert.AreEqual(20, result[1].PrimaryKey.Value);
            Assert.AreEqual(0, _testFieldValueReader.CallsCount);
        }

        [Test]
        public void NotGreatOrEqualsOperation()
        {
            var where = new WhereClause(
                new WhereClause.NotOperation(
                    new WhereClause.GreatOrEqualsOperation(
                        new WhereClause.Field(0), new WhereClause.Constant(6))));
            var result = _analyzer.GetResult(where).ToList();
            Assert.AreEqual(1, result.Count);
            Assert.AreEqual(10, result[0].PrimaryKey.Value);
            Assert.AreEqual(0, _testFieldValueReader.CallsCount);
        }

        [Test]
        public void Like()
        {
            var where = new WhereClause(
                new WhereClause.LikeOperation(
                    new WhereClause.Field(4), new WhereClause.Constant("12")));
            var result = _analyzer.GetResult(where).ToList();
            Assert.AreEqual(1, result.Count);
            Assert.AreEqual(10, result[0].PrimaryKey.Value);
            Assert.AreEqual(0, _testFieldValueReader.CallsCount);
        }

        [Test]
        public void NotLike()
        {
            var where = new WhereClause(
                new WhereClause.NotOperation(
                    new WhereClause.LikeOperation(
                        new WhereClause.Field(4), new WhereClause.Constant("12"))));
            var result = _analyzer.GetResult(where).ToList();
            Assert.AreEqual(1, result.Count);
            Assert.AreEqual(20, result[0].PrimaryKey.Value);
            Assert.AreEqual(0, _testFieldValueReader.CallsCount);
        }

        [Test]
        public void In()
        {
            var where = new WhereClause(
                new WhereClause.InOperation(
                    new WhereClause.Field(0), new WhereClause.Set(new[] { 1 })));
            var result = _analyzer.GetResult(where).ToList();
            Assert.AreEqual(1, result.Count);
            Assert.AreEqual(10, result[0].PrimaryKey.Value);
            Assert.AreEqual(0, _testFieldValueReader.CallsCount);
        }

        [Test]
        public void NotIn()
        {
            var where = new WhereClause(
                new WhereClause.NotOperation(
                    new WhereClause.InOperation(
                        new WhereClause.Field(0), new WhereClause.Set(new[] { 1 }))));
            var result = _analyzer.GetResult(where).ToList();
            Assert.AreEqual(1, result.Count);
            Assert.AreEqual(20, result[0].PrimaryKey.Value);
            Assert.AreEqual(0, _testFieldValueReader.CallsCount);
        }

        class TestEntity
        {
            public int Id { get; set; }
            public int A { get; set; }
            public int B { get; set; }
            public int C { get; set; }
            public int D { get; set; }
            public string S { get; set; }
        }
    }
}
