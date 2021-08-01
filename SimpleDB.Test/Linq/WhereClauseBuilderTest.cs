using System;
using System.Linq.Expressions;
using NUnit.Framework;
using SimpleDB.Core;
using SimpleDB.Linq;
using SimpleDB.Queries;

namespace SimpleDB.Test.Linq
{
    class WhereClauseBuilderTest
    {
        private Mapper<TestEntity> _mapper;
        private WhereClauseBuilder _builder;

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
            _builder = new WhereClauseBuilder();
        }

        [Test]
        public void Build_Equals()
        {
            Expression<Func<TestEntity, bool>> whereExpression = x => x.Id == 2;
            dynamic result = _builder.Build(_mapper, whereExpression).Root;
            Assert.AreEqual(typeof(WhereClause.EqualsOperation), result.GetType());
            Assert.AreEqual(typeof(WhereClause.PrimaryKey), result.Left.GetType());
            Assert.AreEqual(typeof(WhereClause.Constant), result.Right.GetType());
            Assert.AreEqual(2, result.Right.Value);
        }

        [Test]
        public void Build_NotEquals()
        {
            Expression<Func<TestEntity, bool>> whereExpression = x => x.Id != 2;
            dynamic result = _builder.Build(_mapper, whereExpression).Root;
            Assert.AreEqual(typeof(WhereClause.NotOperation), result.GetType());
            Assert.AreEqual(typeof(WhereClause.EqualsOperation), result.Left.GetType());
            Assert.AreEqual(typeof(WhereClause.PrimaryKey), result.Left.Left.GetType());
            Assert.AreEqual(typeof(WhereClause.Constant), result.Left.Right.GetType());
            Assert.AreEqual(2, result.Left.Right.Value);
        }

        [Test]
        public void Build_Not()
        {
            Expression<Func<TestEntity, bool>> whereExpression = x => !(x.Id == 2);
            dynamic result = _builder.Build(_mapper, whereExpression).Root;
            Assert.AreEqual(typeof(WhereClause.NotOperation), result.GetType());
            Assert.AreEqual(typeof(WhereClause.EqualsOperation), result.Left.GetType());
            Assert.AreEqual(typeof(WhereClause.PrimaryKey), result.Left.Left.GetType());
            Assert.AreEqual(typeof(WhereClause.Constant), result.Left.Right.GetType());
            Assert.AreEqual(2, result.Left.Right.Value);
        }

        [Test]
        public void Build_And()
        {
            Expression<Func<TestEntity, bool>> whereExpression = x => x.Id == 2 && x.Int == 4;
            dynamic result = _builder.Build(_mapper, whereExpression).Root;
            Assert.AreEqual(typeof(WhereClause.AndOperation), result.GetType());
            Assert.AreEqual(typeof(WhereClause.EqualsOperation), result.Left.GetType());
            Assert.AreEqual(typeof(WhereClause.EqualsOperation), result.Right.GetType());
            Assert.AreEqual(typeof(WhereClause.PrimaryKey), result.Left.Left.GetType());
            Assert.AreEqual(typeof(WhereClause.Constant), result.Left.Right.GetType());
            Assert.AreEqual(2, result.Left.Right.Value);
            Assert.AreEqual(typeof(WhereClause.Field), result.Right.Left.GetType());
            Assert.AreEqual(typeof(WhereClause.Constant), result.Right.Right.GetType());
            Assert.AreEqual(4, result.Right.Right.Value);
        }

        [Test]
        public void Build_Or()
        {
            Expression<Func<TestEntity, bool>> whereExpression = x => x.Id == 2 || x.Int == 4;
            dynamic result = _builder.Build(_mapper, whereExpression).Root;
            Assert.AreEqual(typeof(WhereClause.OrOperation), result.GetType());
            Assert.AreEqual(typeof(WhereClause.EqualsOperation), result.Left.GetType());
            Assert.AreEqual(typeof(WhereClause.EqualsOperation), result.Right.GetType());
            Assert.AreEqual(typeof(WhereClause.PrimaryKey), result.Left.Left.GetType());
            Assert.AreEqual(typeof(WhereClause.Constant), result.Left.Right.GetType());
            Assert.AreEqual(2, result.Left.Right.Value);
            Assert.AreEqual(typeof(WhereClause.Field), result.Right.Left.GetType());
            Assert.AreEqual(typeof(WhereClause.Constant), result.Right.Right.GetType());
            Assert.AreEqual(4, result.Right.Right.Value);
        }

        [Test]
        public void Build_Less()
        {
            Expression<Func<TestEntity, bool>> whereExpression = x => x.Id < 2;
            dynamic result = _builder.Build(_mapper, whereExpression).Root;
            Assert.AreEqual(typeof(WhereClause.LessOperation), result.GetType());
            Assert.AreEqual(typeof(WhereClause.PrimaryKey), result.Left.GetType());
            Assert.AreEqual(typeof(WhereClause.Constant), result.Right.GetType());
            Assert.AreEqual(2, result.Right.Value);
        }

        [Test]
        public void Build_Great()
        {
            Expression<Func<TestEntity, bool>> whereExpression = x => x.Id > 2;
            dynamic result = _builder.Build(_mapper, whereExpression).Root;
            Assert.AreEqual(typeof(WhereClause.GreatOperation), result.GetType());
            Assert.AreEqual(typeof(WhereClause.PrimaryKey), result.Left.GetType());
            Assert.AreEqual(typeof(WhereClause.Constant), result.Right.GetType());
            Assert.AreEqual(2, result.Right.Value);
        }

        [Test]
        public void Build_LessOrEquals()
        {
            Expression<Func<TestEntity, bool>> whereExpression = x => x.Id <= 2;
            dynamic result = _builder.Build(_mapper, whereExpression).Root;
            Assert.AreEqual(typeof(WhereClause.LessOrEqualsOperation), result.GetType());
            Assert.AreEqual(typeof(WhereClause.PrimaryKey), result.Left.GetType());
            Assert.AreEqual(typeof(WhereClause.Constant), result.Right.GetType());
            Assert.AreEqual(2, result.Right.Value);
        }

        [Test]
        public void Build_GreatOrEquals()
        {
            Expression<Func<TestEntity, bool>> whereExpression = x => x.Id >= 2;
            dynamic result = _builder.Build(_mapper, whereExpression).Root;
            Assert.AreEqual(typeof(WhereClause.GreatOrEqualsOperation), result.GetType());
            Assert.AreEqual(typeof(WhereClause.PrimaryKey), result.Left.GetType());
            Assert.AreEqual(typeof(WhereClause.Constant), result.Right.GetType());
            Assert.AreEqual(2, result.Right.Value);
        }

        [Test]
        public void Build_Like()
        {
            Expression<Func<TestEntity, bool>> whereExpression = x => x.String.Contains("123");
            dynamic result = _builder.Build(_mapper, whereExpression).Root;
            Assert.AreEqual(typeof(WhereClause.LikeOperation), result.GetType());
            Assert.AreEqual(typeof(WhereClause.Field), result.Left.GetType());
            Assert.AreEqual(typeof(WhereClause.Constant), result.Right.GetType());
            Assert.AreEqual("123", result.Right.Value);
        }

        class TestEntity
        {
            public int Id { get; set; }
            public int Int { get; set; }
            public string String { get; set; }
        }
    }
}
