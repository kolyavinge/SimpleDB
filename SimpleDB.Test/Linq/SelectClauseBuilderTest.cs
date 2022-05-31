using System;
using System.Linq;
using System.Linq.Expressions;
using NUnit.Framework;
using SimpleDB.Core;
using SimpleDB.Linq;
using SimpleDB.Queries;

namespace SimpleDB.Test.Linq;

class SelectClauseBuilderTest
{
    private Mapper<TestEntity> _mapper;
    private SelectClauseBuilder _builder;

    [SetUp]
    public void Setup()
    {
        _mapper = new Mapper<TestEntity>(
            new PrimaryKeyMapping<TestEntity>(x => x.Id),
            new[] { new FieldMapping<TestEntity>(1, x => x.Int) });
        _builder = new SelectClauseBuilder();
    }

    [Test]
    public void Build_1()
    {
        Expression<Func<TestEntity, object>> selectExpression = x => x.Id;
        var result = _builder.Build(_mapper, selectExpression).SelectItems.ToList();
        Assert.AreEqual(1, result.Count);
        Assert.AreEqual(typeof(SelectClause.PrimaryKey), result[0].GetType());
    }

    [Test]
    public void Build_Object()
    {
        Expression<Func<TestEntity, object>> selectExpression = x => new { x.Id, x.Int };
        var result = _builder.Build(_mapper, selectExpression).SelectItems.ToList();
        Assert.AreEqual(2, result.Count);
        Assert.AreEqual(typeof(SelectClause.PrimaryKey), result[0].GetType());
        Assert.AreEqual(typeof(SelectClause.Field), result[1].GetType());
    }

    [Test]
    public void Build_All()
    {
        var result = _builder.Build(_mapper, null).SelectItems.ToList();
        Assert.AreEqual(2, result.Count);
        Assert.AreEqual(typeof(SelectClause.PrimaryKey), result[0].GetType());
        Assert.AreEqual(typeof(SelectClause.Field), result[1].GetType());
    }

    class TestEntity
    {
        public int Id { get; set; }
        public int Int { get; set; }
    }
}
