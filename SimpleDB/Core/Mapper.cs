﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace SimpleDB.Core;

internal interface IMapper
{
    Type EntityType { get; }
    string EntityName { get; }
    Type PrimaryKeyType { get; }
    List<FieldMeta> FieldMetaCollection { get; }
    EntityMeta EntityMeta { get; }
}

internal class Mapper<TEntity> : IMapper
{
    private readonly Dictionary<byte, FieldMapping<TEntity>> _fieldMappings;

    public Type EntityType => typeof(TEntity);

    public string EntityName => EntityType.Name;

    public Type PrimaryKeyType => PrimaryKeyMapping.PropertyType;

    public List<FieldMeta> FieldMetaCollection { get; }

    public PrimaryKeyMapping<TEntity> PrimaryKeyMapping { get; }

    public EntityMeta EntityMeta { get; }

    public List<FieldMapping<TEntity>> FieldMappings { get; }

    public Func<TEntity>? MakeFunction { get; set; }

    public PrimaryKeySetFunctionDelegate<TEntity>? PrimaryKeySetFunction { get; set; }

    public FieldSetFunctionDelegate<TEntity>? FieldSetFunction { get; set; }

    public Mapper(PrimaryKeyMapping<TEntity> primaryKeyMapping, IReadOnlyCollection<FieldMapping<TEntity>> fieldMappings)
    {
        PrimaryKeyMapping = primaryKeyMapping;
        _fieldMappings = fieldMappings.ToDictionary(k => k.Number, v => v);
        FieldMappings = _fieldMappings.Values.ToList();
        FieldMetaCollection = GetFieldMetaCollection(fieldMappings).ToList();
        EntityMeta = new EntityMeta(EntityType.Name, new PrimaryKeyFieldMeta(PrimaryKeyMapping.PropertyName, PrimaryKeyMapping.PropertyType), FieldMetaCollection);
    }

    private IEnumerable<FieldMeta> GetFieldMetaCollection(IEnumerable<FieldMapping<TEntity>> fieldMappings)
    {
        foreach (var fieldMapping in fieldMappings)
        {
            yield return new FieldMeta(fieldMapping.Number, fieldMapping.PropertyName, fieldMapping.PropertyType) { Settings = fieldMapping.Settings };
        }
    }

    public object GetPrimaryKeyValue(TEntity entity)
    {
        return PrimaryKeyMapping.Func.Invoke(entity);
    }

    public IEnumerable<FieldValue> GetFieldValueCollection(TEntity entity, ISet<byte>? fieldNumbers = null)
    {
        var fieldMappings = _fieldMappings.Values.ToList();
        if (fieldNumbers is not null)
        {
            fieldMappings.RemoveAll(x => !fieldNumbers.Contains(x.Number));
        }
        foreach (var fieldMapping in fieldMappings)
        {
            yield return new FieldValue(fieldMapping.Number, fieldMapping.Func.Invoke(entity));
        }
    }

    public TEntity MakeEntity(object primaryKeyValue, IEnumerable<FieldValue> fieldValueCollection, bool includePrimaryKey, ISet<byte>? selectedFieldNumbers = null)
    {
        if (MakeFunction is not null && PrimaryKeySetFunction is not null && FieldSetFunction is not null)
        {
            return GetEntityBySetFunctions(primaryKeyValue, fieldValueCollection, includePrimaryKey, selectedFieldNumbers);
        }
        else
        {
            return GetEntityByReflection(primaryKeyValue, fieldValueCollection, includePrimaryKey, selectedFieldNumbers);
        }
    }

    private TEntity GetEntityByReflection(object primaryKeyValue, IEnumerable<FieldValue> fieldValueCollection, bool includePrimaryKey, ISet<byte>? selectedFieldNumbers = null)
    {
        var entity = Activator.CreateInstance<TEntity>();
        if (entity is null) throw new DBEngineException($"Cannot make instance of type {typeof(TEntity)}");
        if (includePrimaryKey)
        {
            var primaryKeyProperty = entity.GetType().GetProperty(PrimaryKeyMapping.PropertyName);
            if (primaryKeyProperty is null) throw new DBEngineException($"Cannot get property {PrimaryKeyMapping.PropertyName}");
            primaryKeyProperty.SetValue(entity, primaryKeyValue);
        }
        if (selectedFieldNumbers is not null)
        {
            foreach (var fieldValue in fieldValueCollection)
            {
                if (selectedFieldNumbers.Contains(fieldValue.Number))
                {
                    var fieldMapping = _fieldMappings[fieldValue.Number];
                    var fieldProperty = entity.GetType().GetProperty(fieldMapping.PropertyName);
                    if (fieldProperty is null) throw new DBEngineException($"Cannot get property {fieldMapping.PropertyName}");
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
                if (fieldProperty is null) throw new DBEngineException($"Cannot get property {fieldMapping.PropertyName}");
                fieldProperty.SetValue(entity, fieldValue.Value);
            }
        }

        return entity;
    }

    private TEntity GetEntityBySetFunctions(
        object primaryKeyValue, IEnumerable<FieldValue> fieldValueCollection, bool includePrimaryKey, ISet<byte>? selectedFieldNumbers = null)
    {
        if (MakeFunction is null || PrimaryKeySetFunction is null || FieldSetFunction is null) throw new InvalidOperationException();
        var entity = MakeFunction();
        if (includePrimaryKey)
        {
            PrimaryKeySetFunction(primaryKeyValue, entity);
        }
        if (selectedFieldNumbers is not null)
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

    public string PropertyName { get; }

    public Type PropertyType { get; }

    public Func<TEntity, object> Func { get; }

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
        else
        {
            throw new DBEngineException("Incorrect primaryKeyExpression");
        }
        Func = Expression.Compile();
    }
}

internal class FieldMapping<TEntity>
{
    public byte Number { get; set; }

    public Expression<Func<TEntity, object>> Expression { get; set; }

    public string PropertyName { get; }

    public Type PropertyType { get; }

    public Func<TEntity, object> Func { get; }

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
        else
        {
            throw new DBEngineException("Incorrect fieldExpression");
        }
        Func = Expression.Compile();
    }

    public static string GetPropertyName<T>(Expression<Func<TEntity, T>> fieldExpression)
    {
        if (fieldExpression.Body is UnaryExpression)
        {
            return ((MemberExpression)((UnaryExpression)fieldExpression.Body).Operand).Member.Name;
        }
        else if (fieldExpression.Body is MemberExpression)
        {
            return ((MemberExpression)fieldExpression.Body).Member.Name;
        }
        else
        {
            throw new DBEngineException("Incorrect fieldExpression");
        }
    }
}
