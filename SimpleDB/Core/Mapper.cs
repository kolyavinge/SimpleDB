﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace SimpleDB.Core
{
    internal class Mapper<TEntity>
    {
        private readonly Dictionary<byte, FieldMapping<TEntity>> _fieldMappings;

        public PrimaryKeyMapping<TEntity> PrimaryKeyMapping { get; private set; }

        public List<FieldMapping<TEntity>> FieldMappings { get; private set; }

        public List<FieldMeta> FieldMetaCollection { get; private set; }

        public string EntityName { get; private set; }

        public Func<TEntity> MakeFunction { get; set; }

        public PrimaryKeySetFunctionDelegate<TEntity> PrimaryKeySetFunction { get; set; }

        public FieldSetFunctionDelegate<TEntity> FieldSetFunction { get; set; }

        public Mapper(
            string entityName,
            PrimaryKeyMapping<TEntity> primaryKeyMapping,
            IEnumerable<FieldMapping<TEntity>> fieldMappings)
        {
            EntityName = entityName;
            PrimaryKeyMapping = primaryKeyMapping;
            _fieldMappings = fieldMappings.ToDictionary(k => k.Number, v => v);
            FieldMappings = _fieldMappings.Values.ToList();
            FieldMetaCollection = GetFieldMetaCollection(fieldMappings).ToList();
        }

        private IEnumerable<FieldMeta> GetFieldMetaCollection(IEnumerable<FieldMapping<TEntity>> fieldMappings)
        {
            foreach (var fieldMapping in fieldMappings)
            {
                yield return new FieldMeta(fieldMapping.Number, fieldMapping.PropertyType) { Settings = fieldMapping.Settings };
            }
        }

        public object GetPrimaryKeyValue(TEntity entity)
        {
            return PrimaryKeyMapping.Func.Invoke(entity);
        }

        public IEnumerable<FieldValue> GetFieldValueCollection(TEntity entity)
        {
            foreach (var fieldMapping in _fieldMappings.Values)
            {
                yield return new FieldValue(fieldMapping.Number, fieldMapping.Func.Invoke(entity));
            }
        }

        public TEntity GetEntity(object primaryKeyValue, IEnumerable<FieldValue> fieldValueCollection, bool includePrimaryKey, ISet<byte> selectedFieldNumbers = null)
        {
            if (MakeFunction != null && PrimaryKeySetFunction != null && FieldSetFunction != null)
            {
                return GetEntityBySetFunctions(primaryKeyValue, fieldValueCollection, includePrimaryKey, selectedFieldNumbers);
            }
            else
            {
                return GetEntityByReflection(primaryKeyValue, fieldValueCollection, includePrimaryKey, selectedFieldNumbers);
            }
        }

        private TEntity GetEntityByReflection(object primaryKeyValue, IEnumerable<FieldValue> fieldValueCollection, bool includePrimaryKey, ISet<byte> selectedFieldNumbers = null)
        {
            var entity = Activator.CreateInstance<TEntity>();
            if (includePrimaryKey)
            {
                var primaryKeyProperty = entity.GetType().GetProperty(PrimaryKeyMapping.PropertyName);
                primaryKeyProperty.SetValue(entity, primaryKeyValue);
            }
            if (selectedFieldNumbers != null)
            {
                foreach (var fieldValue in fieldValueCollection)
                {
                    if (selectedFieldNumbers.Contains(fieldValue.Number))
                    {
                        var fieldMapping = _fieldMappings[fieldValue.Number];
                        var fieldProperty = entity.GetType().GetProperty(fieldMapping.PropertyName);
                        fieldProperty.SetValue(entity, fieldValue.Value);
                    }
                }
            }
            else
            {
                foreach (var fieldValue in fieldValueCollection)
                {
                    var fieldMapping = _fieldMappings[fieldValue.Number];
                    var fieldProperty = entity.GetType().GetProperty(fieldMapping.PropertyName);
                    fieldProperty.SetValue(entity, fieldValue.Value);
                }
            }

            return entity;
        }

        private TEntity GetEntityBySetFunctions(object primaryKeyValue, IEnumerable<FieldValue> fieldValueCollection, bool includePrimaryKey, ISet<byte> selectedFieldNumbers = null)
        {
            var entity = MakeFunction();
            if (includePrimaryKey)
            {
                PrimaryKeySetFunction(primaryKeyValue, entity);
            }
            if (selectedFieldNumbers != null)
            {
                foreach (var fieldValue in fieldValueCollection)
                {
                    if (selectedFieldNumbers.Contains(fieldValue.Number))
                    {
                        FieldSetFunction(fieldValue.Number, fieldValue.Value, entity);
                    }
                }
            }
            else
            {
                foreach (var fieldValue in fieldValueCollection)
                {
                    FieldSetFunction(fieldValue.Number, fieldValue.Value, entity);
                }
            }

            return entity;
        }
    }

    internal class PrimaryKeyMapping<TEntity>
    {
        public Expression<Func<TEntity, object>> Expression { get; set; }

        public string PropertyName { get; private set; }

        public Type PropertyType { get; private set; }

        public Func<TEntity, object> Func { get; private set; }

        public PrimaryKeyMapping(Expression<Func<TEntity, object>> primaryKeyExpression)
        {
            Expression = primaryKeyExpression;
            if (primaryKeyExpression.Body is UnaryExpression)
            {
                PropertyName = ((MemberExpression)((UnaryExpression)primaryKeyExpression.Body).Operand).Member.Name;
                PropertyType = ((MemberExpression)((UnaryExpression)primaryKeyExpression.Body).Operand).Type;
            }
            else if (primaryKeyExpression.Body is MemberExpression)
            {
                PropertyName = ((MemberExpression)primaryKeyExpression.Body).Member.Name;
                PropertyType = ((MemberExpression)primaryKeyExpression.Body).Type;
            }
            Func = Expression.Compile();
        }
    }

    internal class FieldMapping<TEntity>
    {
        public byte Number { get; set; }

        public Expression<Func<TEntity, object>> Expression { get; set; }

        public string PropertyName { get; private set; }

        public Type PropertyType { get; private set; }

        public Func<TEntity, object> Func { get; private set; }

        public FieldSettings Settings { get; set; }

        public FieldMapping(byte number, Expression<Func<TEntity, object>> fieldExpression)
        {
            Number = number;
            Expression = fieldExpression;
            if (fieldExpression.Body is UnaryExpression)
            {
                PropertyName = ((MemberExpression)((UnaryExpression)fieldExpression.Body).Operand).Member.Name;
                PropertyType = ((MemberExpression)((UnaryExpression)fieldExpression.Body).Operand).Type;
            }
            else if (fieldExpression.Body is MemberExpression)
            {
                PropertyName = ((MemberExpression)fieldExpression.Body).Member.Name;
                PropertyType = ((MemberExpression)fieldExpression.Body).Type;
            }
            Func = Expression.Compile();
        }
    }
}
