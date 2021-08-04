using System.Linq;
using NUnit.Framework;
using SimpleDB.Core;
using SimpleDB.Linq;
using SimpleDB.Queries;

namespace SimpleDB.Test.Linq
{
    class QueryBuilderTest
    {
        private Mapper<TestEntity> _mapper;

        [SetUp]
        public void Setup()
        {
            _mapper = new Mapper<TestEntity>(
                "testEntity",
                new PrimaryKeyMapping<TestEntity>(x => x.Id),
                new[] { new FieldMapping<TestEntity>(0, x => x.Int) });
        }

        [Test]
        public void Count()
        {
            var builder = new QueryBuilder<TestEntity>(
                _mapper,
                null,
                x => x.Id == 1);
            var query = builder.BuildQueryForCount();
            Assert.AreEqual(1, query.SelectClause.SelectItems.Count());
            Assert.True(query.SelectClause.SelectItems.First() is SelectClause.CountAggregate);
        }

        class TestEntity
        {
            public int Id { get; set; }
            public int Int { get; set; }
        }
    }
}
