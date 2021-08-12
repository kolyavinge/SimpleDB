using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using SimpleDB.Core;
using SimpleDB.Queries;

namespace SimpleDB.Linq
{
    internal class UpdateClauseBuilder
    {
        public UpdateClause Build<TEntity>(Mapper<TEntity> mapper, Expression<Func<TEntity, TEntity>> updateExpression)
        {
            var updatedItems = new List<UpdateClause.UpdateClauseItem>();
            if (updateExpression.Body is MemberInitExpression)
            {
                var body = (MemberInitExpression)updateExpression.Body;
                foreach (MemberAssignment binding in body.Bindings)
                {
                    updatedItems.Add(GetItem(mapper, binding.Member.Name, GetValue(binding.Expression)));
                }
            }

            var updateClause = new UpdateClause(updatedItems);

            return updateClause;
        }

        private UpdateClause.UpdateClauseItem GetItem<TEntity>(Mapper<TEntity> mapper, string fieldName, object fieldValue)
        {
            if (fieldName == mapper.PrimaryKeyMapping.PropertyName)
            {
                throw new UnsupportedQueryException("Primary key can not be updated.");
            }
            else
            {
                var fieldNumber = mapper.FieldMappings.First(x => x.PropertyName == fieldName).Number;
                return new UpdateClause.Field(fieldNumber, fieldValue);
            }
        }

        private object GetValue(Expression expression)
        {
            if (expression is ConstantExpression)
            {
                var constantExpression = (ConstantExpression)expression;
                return constantExpression.Value;
            }

            throw new UnsupportedQueryException();
        }
    }
}
