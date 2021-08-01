using System;
using System.Linq;
using System.Linq.Expressions;
using SimpleDB.Core;
using SimpleDB.Queries;

namespace SimpleDB.Linq
{
    internal class WhereClauseBuilder
    {
        public WhereClause Build<TEntity>(Mapper<TEntity> mapper, Expression<Func<TEntity, bool>> whereExpression)
        {
            var whereClause = new WhereClause(BuildItem(mapper, whereExpression.Body));
            return whereClause;
        }

        private WhereClause.WhereClauseItem BuildItem<TEntity>(Mapper<TEntity> mapper, Expression expression)
        {
            if (expression is MethodCallExpression)
            {
                var methodCallExpression = (MethodCallExpression)expression;
                if (methodCallExpression.Method.Name == "Contains"
                    && methodCallExpression.Arguments.First() is ConstantExpression)
                {
                    var right = ((ConstantExpression)methodCallExpression.Arguments.First()).Value;
                    return new WhereClause.LikeOperation(BuildItem(mapper, methodCallExpression.Object), new WhereClause.Constant(right));
                }
            }
            else if (expression is BinaryExpression)
            {
                var binaryExpression = (BinaryExpression)expression;
                if (binaryExpression.NodeType == ExpressionType.Equal)
                {
                    return new WhereClause.EqualsOperation(BuildItem(mapper, binaryExpression.Left), BuildItem(mapper, binaryExpression.Right));
                }
                else if (binaryExpression.NodeType == ExpressionType.NotEqual)
                {
                    return new WhereClause.NotOperation(
                        new WhereClause.EqualsOperation(BuildItem(mapper, binaryExpression.Left), BuildItem(mapper, binaryExpression.Right)));
                }
                else if (binaryExpression.NodeType == ExpressionType.AndAlso)
                {
                    return new WhereClause.AndOperation(BuildItem(mapper, binaryExpression.Left), BuildItem(mapper, binaryExpression.Right));
                }
                else if (binaryExpression.NodeType == ExpressionType.OrElse)
                {
                    return new WhereClause.OrOperation(BuildItem(mapper, binaryExpression.Left), BuildItem(mapper, binaryExpression.Right));
                }
                else if (binaryExpression.NodeType == ExpressionType.LessThan)
                {
                    return new WhereClause.LessOperation(BuildItem(mapper, binaryExpression.Left), BuildItem(mapper, binaryExpression.Right));
                }
                else if (binaryExpression.NodeType == ExpressionType.GreaterThan)
                {
                    return new WhereClause.GreatOperation(BuildItem(mapper, binaryExpression.Left), BuildItem(mapper, binaryExpression.Right));
                }
                else if (binaryExpression.NodeType == ExpressionType.LessThanOrEqual)
                {
                    return new WhereClause.LessOrEqualsOperation(BuildItem(mapper, binaryExpression.Left), BuildItem(mapper, binaryExpression.Right));
                }
                else if (binaryExpression.NodeType == ExpressionType.GreaterThanOrEqual)
                {
                    return new WhereClause.GreatOrEqualsOperation(BuildItem(mapper, binaryExpression.Left), BuildItem(mapper, binaryExpression.Right));
                }
            }
            else if (expression is UnaryExpression)
            {
                var unaryExpression = (UnaryExpression)expression;
                if (unaryExpression.NodeType == ExpressionType.Not)
                {
                    return new WhereClause.NotOperation(BuildItem(mapper, unaryExpression.Operand));
                }
            }
            else if (expression is MemberExpression)
            {
                var memberExpression = (MemberExpression)expression;
                if (mapper.PrimaryKeyMapping.PropertyName == memberExpression.Member.Name)
                {
                    return new WhereClause.PrimaryKey();
                }
                else
                {
                    var fieldNumber = mapper.FieldMappings.First(x => x.PropertyName == memberExpression.Member.Name).Number;
                    return new WhereClause.Field(fieldNumber);
                }
            }
            else if (expression is ConstantExpression)
            {
                var constantExpression = (ConstantExpression)expression;
                return new WhereClause.Constant(constantExpression.Value);
            }

            throw new Exception();
        }
    }
}
