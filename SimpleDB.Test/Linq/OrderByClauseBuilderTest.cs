using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;
using SimpleDB.Core;
using SimpleDB.Linq;
using SimpleDB.Queries;

namespace SimpleDB.Test.Linq
{
    class OrderByClauseBuilderTest
    {
        private Mapper<TestEntity> _mapper;
        private OrderByClauseBuilder _builder;

        [SetUp]
        public void Setup()
        {
            _mapper = new Mapper<TestEntity>(
                "testEntity",
                new PrimaryKeyMapping<TestEntity>(x => x.Id),
                new[]
                {
                    new FieldMapping<TestEntity>(0, x => x.Int),
                    new FieldMapping<TestEntity>(1, x => x.String)
                });
            _builder = new OrderByClauseBuilder();
        }

        [Test]
        public void Build_Null()
        {
            var result = _builder.Build(_mapper, null);
            Assert.IsNull(result);
        }

        [Test]
        public void Build_Empty()
        {
            var result = _builder.Build(_mapper, new List<OrderByExpressionItem<TestEntity>>());
            Assert.IsNull(result);
        }

        [Test]
        public void Build_1()
        {
            var items = new List<OrderByExpressionItem<TestEntity>>
            {
                new OrderByExpressionItem<TestEntity> { Expression = x => x.Id, Direction = SortDirection.Asc },
                new OrderByExpressionItem<TestEntity> { Expression = x => x.Int, Direction = SortDirection.Desc }
            };
            var result = _builder.Build(_mapper, items).OrderedItems.ToList();
            Assert.AreEqual(2, result.Count);
            Assert.AreEqual(typeof(OrderByClause.PrimaryKey), result[0].GetType());
            Assert.AreEqual(typeof(OrderByClause.Field), result[1].GetType());
            Assert.AreEqual(SortDirection.Asc, ((OrderByClause.PrimaryKey)result[0]).Direction);
            Assert.AreEqual(SortDirection.Desc, ((OrderByClause.Field)result[1]).Direction);
            Assert.AreEqual(0, ((OrderByClause.Field)result[1]).Number);
        }

        [Test]
        public void Build_2()
        {
            var items = new List<OrderByExpressionItem<TestEntity>>
            {
                new OrderByExpressionItem<TestEntity> { Expression = x => x.Id, Direction = SortDirection.Asc },
                new OrderByExpressionItem<TestEntity> { Expression = x => x.String, Direction = SortDirection.Desc }
            };
            var result = _builder.Build(_mapper, items).OrderedItems.ToList();
            Assert.AreEqual(2, result.Count);
            Assert.AreEqual(typeof(OrderByClause.PrimaryKey), result[0].GetType());
            Assert.AreEqual(typeof(OrderByClause.Field), result[1].GetType());
            Assert.AreEqual(SortDirection.Asc, ((OrderByClause.PrimaryKey)result[0]).Direction);
            Assert.AreEqual(SortDirection.Desc, ((OrderByClause.Field)result[1]).Direction);
            Assert.AreEqual(1, ((OrderByClause.Field)result[1]).Number);
        }

        class TestEntity
        {
            public int Id { get; set; }
            public int Int { get; set; }
            public string String { get; set; }
        }
    }
}
