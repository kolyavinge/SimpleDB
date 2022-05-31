using System;
using System.Linq;
using System.Linq.Expressions;
using NUnit.Framework;
using SimpleDB.Core;
using SimpleDB.Linq;
using SimpleDB.Queries;

namespace SimpleDB.Test.Linq;

class MergeClauseBuilderTest
{
    private Mapper<TestEntity> _mapper;
    private MergeClauseBuilder _builder;

    [SetUp]
    public void Setup()
    {
        _mapper = new Mapper<TestEntity>(
            new PrimaryKeyMapping<TestEntity>(x => x.Id),
            new[]
            {
                new FieldMapping<TestEntity>(1, x => x.Int),
                new FieldMapping<TestEntity>(2, x => x.Float)
            });
        _builder = new MergeClauseBuilder();
    }

    [Test]
    public void Build_1()
    {
        Expression<Func<TestEntity, object>> mergeFieldsExpression = x => x.Int;
        var result = _builder.Build(_mapper, mergeFieldsExpression).MergeItems.ToList();
        Assert.AreEqual(1, result.Count);
        Assert.AreEqual(typeof(MergeClause.MergeClauseItem), result[0].GetType());
        Assert.AreEqual(1, result[0].FieldNumber);
    }

    [Test]
    public void Build_2()
    {
        Expression<Func<TestEntity, object>> mergeFieldsExpression = x => new { x.Int, x.Float };
        var result = _builder.Build(_mapper, mergeFieldsExpression).MergeItems.ToList();
        Assert.AreEqual(2, result.Count);
        Assert.AreEqual(typeof(MergeClause.MergeClauseItem), result[0].GetType());
        Assert.AreEqual(1, result[0].FieldNumber);
        Assert.AreEqual(typeof(MergeClause.MergeClauseItem), result[1].GetType());
        Assert.AreEqual(2, result[1].FieldNumber);
    }

    class TestEntity
    {
        public int Id { get; set; }
        public int Int { get; set; }
        public float Float { get; set; }
    }
}
