﻿using System;
using System.Linq;
using System.Linq.Expressions;
using NUnit.Framework;
using SimpleDB.Core;
using SimpleDB.Linq;
using SimpleDB.Queries;

namespace SimpleDB.Test.Linq;

class UpdateClauseBuilderTest
{
    private Mapper<TestEntity> _mapper;
    private UpdateClauseBuilder _builder;

    [SetUp]
    public void Setup()
    {
        _mapper = new Mapper<TestEntity>(
           new PrimaryKeyMapping<TestEntity>(x => x.Id),
           new[] { new FieldMapping<TestEntity>(1, x => x.Int) });
        _builder = new UpdateClauseBuilder();
    }

    [Test]
    public void Build_UpdateId_UnsupportedQueryException()
    {
        try
        {
            Expression<Func<TestEntity, TestEntity>> selectExpression = x => new TestEntity { Id = 1 };
            _builder.Build(_mapper, selectExpression).UpdateItems.ToList();
            Assert.Fail();
        }
        catch (UnsupportedQueryException)
        {
            Assert.Pass();
        }
    }

    [Test]
    public void Build_UpdateId_UnsupportedQueryException_2()
    {
        try
        {
            Expression<Func<TestEntity, TestEntity>> selectExpression = x => new TestEntity { Id = 1, Int = 2 };
            _builder.Build(_mapper, selectExpression).UpdateItems.ToList();
            Assert.Fail();
        }
        catch (UnsupportedQueryException)
        {
            Assert.Pass();
        }
    }

    [Test]
    public void Build_Int_Constant()
    {
        Expression<Func<TestEntity, TestEntity>> selectExpression = x => new TestEntity { Int = 2 };
        var result = _builder.Build(_mapper, selectExpression).UpdateItems.ToList();
        Assert.AreEqual(1, result.Count);
        Assert.AreEqual(typeof(UpdateClause.Field), result[0].GetType());
    }

    [Test]
    public void Build_Int_Variable()
    {
        int a = 2;
        Expression<Func<TestEntity, TestEntity>> selectExpression = x => new TestEntity { Int = a };
        var result = _builder.Build(_mapper, selectExpression).UpdateItems.ToList();
        Assert.AreEqual(1, result.Count);
        Assert.AreEqual(typeof(UpdateClause.Field), result[0].GetType());
    }

    class TestEntity
    {
        public int Id { get; set; }
        public int Int { get; set; }
    }
}
