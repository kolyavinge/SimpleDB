using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using SimpleDB.Core;
using SimpleDB.Queries;

namespace SimpleDB.Linq
{
    internal class OrderByClauseBuilder
    {
        public OrderByClause Build<TEntity>(Mapper<TEntity> mapper, List<OrderByExpressionItem<TEntity>> orderbyExpressionItems)
        {
            if (orderbyExpressionItems == null || orderbyExpressionItems.Any() == false) return null;
            var orderbyClauseFields = new List<OrderByClause.OrderByClauseItem>();
            foreach (var item in orderbyExpressionItems)
            {
                if (item.Expression.Body is MemberExpression)
                {
                    var body = (MemberExpression)item.Expression.Body;
                    var fieldName = body.Member.Name;
                    if (fieldName == mapper.PrimaryKeyMapping.PropertyName)
                    {
                        orderbyClauseFields.Add(new OrderByClause.PrimaryKey(item.Direction));
                    }
                    else
                    {
                        var fieldNumber = mapper.FieldMappings.First(x => x.PropertyName == fieldName).Number;
                        orderbyClauseFields.Add(new OrderByClause.Field(fieldNumber, item.Direction));
                    }
                }
                else if (item.Expression.Body is UnaryExpression)
                {
                    var body = (UnaryExpression)item.Expression.Body;
                    var operand = (MemberExpression)body.Operand;
                    var fieldName = operand.Member.Name;
                    if (fieldName == mapper.PrimaryKeyMapping.PropertyName)
                    {
                        orderbyClauseFields.Add(new OrderByClause.PrimaryKey(item.Direction));
                    }
                    else
                    {
                        var fieldNumber = mapper.FieldMappings.First(x => x.PropertyName == fieldName).Number;
                        orderbyClauseFields.Add(new OrderByClause.Field(fieldNumber, item.Direction));
                    }
                }
                else
                {
                    throw new UnsupportedQueryException();
                }
            }
            var orderbyClause = new OrderByClause(orderbyClauseFields);

            return orderbyClause;
        }
    }
}
