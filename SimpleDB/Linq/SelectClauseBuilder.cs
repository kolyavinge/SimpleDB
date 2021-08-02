using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using SimpleDB.Core;
using SimpleDB.Queries;

namespace SimpleDB.Linq
{
    internal class SelectClauseBuilder
    {
        public SelectClause Build<TEntity>(Mapper<TEntity> mapper, Expression<Func<TEntity, object>> selectExpression)
        {
            if (selectExpression is null) throw new ArgumentNullException(nameof(selectExpression));

            var selectedItems = new List<SelectClause.SelectClauseItem>();
            if (selectExpression.Body is UnaryExpression)
            {
                var body = (UnaryExpression)selectExpression.Body;
                var operand = (MemberExpression)body.Operand;
                var fieldName = operand.Member.Name;
                selectedItems.Add(GetItem(mapper, fieldName));
            }
            else if (selectExpression.Body is NewExpression)
            {
                var body = (NewExpression)selectExpression.Body;
                foreach (var member in body.Members)
                {
                    selectedItems.Add(GetItem(mapper, member.Name));
                }
            }
            var selectClause = new SelectClause(selectedItems);

            return selectClause;
        }

        private SelectClause.SelectClauseItem GetItem<TEntity>(Mapper<TEntity> mapper, string fieldName)
        {
            if (fieldName == mapper.PrimaryKeyMapping.PropertyName)
            {
                return new SelectClause.PrimaryKey();
            }
            else
            {
                var fieldNumber = mapper.FieldMappings.First(x => x.PropertyName == fieldName).Number;
                return new SelectClause.Field(fieldNumber);
            }
        }
    }
}
