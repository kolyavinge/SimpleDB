using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using SimpleDB.Core;
using SimpleDB.Queries;

namespace SimpleDB.Linq;

internal class MergeClauseBuilder
{
    public MergeClause Build<TEntity>(Mapper<TEntity> mapper, Expression<Func<TEntity, object>> mergeFieldsExpression)
    {
        var mergeItems = new List<MergeClause.MergeClauseItem>();
        if (mergeFieldsExpression.Body is UnaryExpression)
        {
            var body = (UnaryExpression)mergeFieldsExpression.Body;
            var operand = (MemberExpression)body.Operand;
            var fieldName = operand.Member.Name;
            mergeItems.Add(GetItem(mapper, fieldName));
        }
        else if (mergeFieldsExpression.Body is NewExpression)
        {
            var body = (NewExpression)mergeFieldsExpression.Body;
            foreach (var member in body.Members)
            {
                mergeItems.Add(GetItem(mapper, member.Name));
            }
        }
        var mergeClause = new MergeClause(mergeItems);

        return mergeClause;
    }

    private MergeClause.MergeClauseItem GetItem<TEntity>(Mapper<TEntity> mapper, string fieldName)
    {
        //if (fieldName == mapper.PrimaryKeyMapping.PropertyName)
        //{
        //    return new MergeClause.MergeItem();
        //}
        //else
        {
            var fieldNumber = mapper.FieldMappings.First(x => x.PropertyName == fieldName).Number;
            return new MergeClause.MergeClauseItem(fieldNumber);
        }
    }
}
