using System;
using System.Collections;
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
            if (whereExpression is null) return null;
            var whereClause = new WhereClause(BuildItem(mapper, whereExpression.Body));

            return whereClause;
        }

        private WhereClause.WhereClauseItem BuildItem<TEntity>(Mapper<TEntity> mapper, Expression expression)
        {
            if (expression is MethodCallExpression)
            {
                var methodCallExpression = (MethodCallExpression)expression;
                if (methodCallExpression.Method.Name == "Contains"
                    && methodCallExpression.Method.DeclaringType == typeof(string)
                    && methodCallExpression.Arguments.Count == 1
                    && methodCallExpression.Arguments.First() is ConstantExpression)
                {
                    var right = ((ConstantExpression)methodCallExpression.Arguments.First()).Value;
                    return new WhereClause.LikeOperation(BuildItem(mapper, methodCallExpression.Object), new WhereClause.Constant(right));
                }
                else if (methodCallExpression.Method.Name == "Contains"
                    && methodCallExpression.Method.DeclaringType == typeof(string)
                    && methodCallExpression.Arguments.Count == 1
                    && methodCallExpression.Arguments.First() is MemberExpression)
                {
                    var memberExpression = (MemberExpression)methodCallExpression.Arguments.First();
                    if (memberExpression.Expression is ConstantExpression)
                    {
                        var constantExpression = (ConstantExpression)memberExpression.Expression;
                        var right = constantExpression.Value.GetType().GetField(memberExpression.Member.Name).GetValue(constantExpression.Value);
                        return new WhereClause.LikeOperation(BuildItem(mapper, methodCallExpression.Object), new WhereClause.Constant(right));
                    }
                    else if (memberExpression.Expression is MemberExpression)
                    {
                        var innerMemberExpression = (MemberExpression)memberExpression.Expression;
                        var constantExpression = (ConstantExpression)innerMemberExpression.Expression;
                        var constantExpressionValue = constantExpression.Value.GetType().GetField(innerMemberExpression.Member.Name).GetValue(constantExpression.Value);
                        var right = constantExpressionValue.GetType().GetProperty(memberExpression.Member.Name).GetValue(constantExpressionValue);
                        return new WhereClause.LikeOperation(BuildItem(mapper, methodCallExpression.Object), new WhereClause.Constant(right));
                    }
                }
                else if (methodCallExpression.Method.Name == "Contains"
                    && methodCallExpression.Method.DeclaringType.GetInterfaces().Any(x => x.FullName == "System.Collections.IEnumerable")
                    && methodCallExpression.Arguments.Count == 1
                    && methodCallExpression.Arguments.First() is MemberExpression)
                {
                    var left = methodCallExpression.Arguments.First();
                    if (methodCallExpression.Object is MemberExpression)
                    {
                        var objectMemberExpression = (MemberExpression)methodCallExpression.Object;
                        var constantExpression = (ConstantExpression)objectMemberExpression.Expression;
                        var set = (IEnumerable)constantExpression.Value.GetType().GetField(objectMemberExpression.Member.Name).GetValue(constantExpression.Value);
                        return new WhereClause.InOperation(BuildItem(mapper, left), new WhereClause.Set(set));
                    }
                }
                else if (methodCallExpression.Method.Name == "Contains"
                    && methodCallExpression.Method.DeclaringType.FullName == "System.Linq.Enumerable"
                    && methodCallExpression.Arguments.Count == 2
                    && methodCallExpression.Arguments[0] is NewArrayExpression
                    && methodCallExpression.Arguments[1] is MemberExpression)
                {
                    var left = methodCallExpression.Arguments[1];
                    var set = ((NewArrayExpression)methodCallExpression.Arguments[0]).Expressions.Cast<ConstantExpression>().Select(x => x.Value).ToList();
                    return new WhereClause.InOperation(BuildItem(mapper, left), new WhereClause.Set(set));
                }
                else if (methodCallExpression.Method.Name == "Contains"
                    && methodCallExpression.Method.DeclaringType.FullName == "System.Linq.Enumerable"
                    && methodCallExpression.Arguments.Count == 2
                    && methodCallExpression.Arguments[0] is MemberExpression
                    && methodCallExpression.Arguments[1] is MemberExpression)
                {
                    var left = methodCallExpression.Arguments[1];
                    var memberExpression = (MemberExpression)methodCallExpression.Arguments[0];
                    var constantExpression = (ConstantExpression)memberExpression.Expression;
                    var set = (IEnumerable)constantExpression.Value.GetType().GetField(memberExpression.Member.Name).GetValue(constantExpression.Value);
                    return new WhereClause.InOperation(BuildItem(mapper, left), new WhereClause.Set(set));
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
                else if (unaryExpression.NodeType == ExpressionType.Convert)
                {
                    return BuildItem(mapper, unaryExpression.Operand);
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

            throw new UnsupportedQueryException();
        }
    }
}
