using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using SimpleDB.Core;

namespace SimpleDB.Building
{
    internal abstract class MapperBuilder
    {
        public abstract IMapper Build();
    }

    internal class MapperBuilder<TEntity> : MapperBuilder, IMapperBuilder<TEntity>
    {
        private readonly FieldMappingValidator _fieldMappingValidator = new();
        private readonly List<FieldMapping<TEntity>> _fieldMappings = new();
        private PrimaryKeyMapping<TEntity>? _primaryKeyMapping;
        private Func<TEntity>? _makeFunction;
        private PrimaryKeySetFunctionDelegate<TEntity>? _primaryKeySetFunction;
        private FieldSetFunctionDelegate<TEntity>? _fieldSetFunction;

        public IMapperBuilder<TEntity> PrimaryKey(Expression<Func<TEntity, object>> primaryKeyExpression)
        {
            _primaryKeyMapping = new PrimaryKeyMapping<TEntity>(primaryKeyExpression);
            return this;
        }

        public IMapperBuilder<TEntity> Field(byte number, Expression<Func<TEntity, object>> fieldExpression, FieldSettings settings = default)
        {
            var fieldMapping = new FieldMapping<TEntity>(number, fieldExpression) { Settings = settings };
            _fieldMappingValidator.Validate(fieldMapping);
            _fieldMappings.Add(fieldMapping);
            return this;
        }

        public IMapperBuilder<TEntity> MakeFunction(Func<TEntity> func)
        {
            _makeFunction = func;
            return this;
        }

        public IMapperBuilder<TEntity> PrimaryKeySetFunction(PrimaryKeySetFunctionDelegate<TEntity> func)
        {
            _primaryKeySetFunction = func;
            return this;
        }

        public IMapperBuilder<TEntity> FieldSetFunction(FieldSetFunctionDelegate<TEntity> func)
        {
            _fieldSetFunction = func;
            return this;
        }

        public override IMapper Build()
        {
            if (_primaryKeyMapping == null) throw new InvalidOperationException("PrimaryKeyMapping cannot be null");
            return new Mapper<TEntity>(_primaryKeyMapping, _fieldMappings)
            {
                MakeFunction = _makeFunction,
                PrimaryKeySetFunction = _primaryKeySetFunction,
                FieldSetFunction = _fieldSetFunction
            };
        }
    }
}
