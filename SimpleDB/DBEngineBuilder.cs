using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using SimpleDB.Core;
using SimpleDB.IndexedSearch;
using SimpleDB.Infrastructure;

namespace SimpleDB
{
    public sealed class DBEngineBuilder
    {
        private List<MapperBuilder> _mapperBuilders = new List<MapperBuilder>();
        private List<IndexBuilder> _indexBuilders = new List<IndexBuilder>();

        static DBEngineBuilder()
        {
            IOC.Set<IFileSystem>(new FileSystem());
            IOC.Set<IMemory>(new Memory());
        }

        public static DBEngineBuilder Make()
        {
            return new DBEngineBuilder();
        }

        internal DBEngineBuilder() { }

        public void WorkingDirectory(string workingDirectory)
        {
            GlobalSettings.WorkingDirectory = workingDirectory;
        }

        public IMapperBuilder<TEntity> Map<TEntity>()
        {
            var mapperBuilder = new MapperBuilder<TEntity>();
            _mapperBuilders.Add(mapperBuilder);
            return mapperBuilder;
        }

        public IIndexBuilder<TEntity> Index<TEntity>()
        {
            var indexBuilder = new IndexBuilder<TEntity>();
            _indexBuilders.Add(indexBuilder);
            return indexBuilder;
        }

        public IDBEngine BuildEngine()
        {
            var mappers = _mapperBuilders.Select(x => x.Build()).ToList();
            var mapperHolder = new MapperHolder(mappers);
            var indexes = _indexBuilders.Select(x => x.BuildFunction(mapperHolder)).ToList();
            var indexHolder = new IndexHolder(indexes);
            var indexUpdater = new IndexUpdater(indexes, mapperHolder);
            return new DBEngine(mapperHolder, indexHolder, indexUpdater);
        }
    }

    public interface IMapperBuilder<TEntity>
    {
        IMapperBuilder<TEntity> Name(string name);

        IMapperBuilder<TEntity> PrimaryKey(Expression<Func<TEntity, object>> primaryKeyExpression);

        IMapperBuilder<TEntity> Field(byte number, Expression<Func<TEntity, object>> fieldExpression, FieldSettings settings = default(FieldSettings));

        IMapperBuilder<TEntity> MakeFunction(Func<TEntity> func);

        IMapperBuilder<TEntity> PrimaryKeySetFunction(PrimaryKeySetFunctionDelegate<TEntity> func);

        IMapperBuilder<TEntity> FieldSetFunction(FieldSetFunctionDelegate<TEntity> func);
    }

    public delegate void PrimaryKeySetFunctionDelegate<TEntity>(object primaryKeyValue, TEntity entity);

    public delegate void FieldSetFunctionDelegate<TEntity>(byte fieldNumber, object fieldValue, TEntity entity);

    internal abstract class MapperBuilder
    {
        public abstract object Build();
    }

    internal class MapperBuilder<TEntity> : MapperBuilder, IMapperBuilder<TEntity>
    {
        private readonly FieldMappingValidator _fieldMappingValidator = new FieldMappingValidator();
        private string _name;
        private PrimaryKeyMapping<TEntity> _primaryKeyMapping;
        private List<FieldMapping<TEntity>> _fieldMappings = new List<FieldMapping<TEntity>>();
        private Func<TEntity> _makeFunction;
        private PrimaryKeySetFunctionDelegate<TEntity> _primaryKeySetFunction;
        private FieldSetFunctionDelegate<TEntity> _fieldSetFunction;

        public IMapperBuilder<TEntity> Name(string name)
        {
            _name = name;
            return this;
        }

        public IMapperBuilder<TEntity> PrimaryKey(Expression<Func<TEntity, object>> primaryKeyExpression)
        {
            _primaryKeyMapping = new PrimaryKeyMapping<TEntity>(primaryKeyExpression);
            return this;
        }

        public IMapperBuilder<TEntity> Field(byte number, Expression<Func<TEntity, object>> fieldExpression, FieldSettings settings = default(FieldSettings))
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

        public override object Build()
        {
            return new Mapper<TEntity>(_name, _primaryKeyMapping, _fieldMappings)
            {
                MakeFunction = _makeFunction,
                PrimaryKeySetFunction = _primaryKeySetFunction,
                FieldSetFunction = _fieldSetFunction
            };
        }
    }

    public interface IIndexBuilder<TEntity>
    {
        IIndexBuilder<TEntity> Name(string indexName);

        IIndexBuilder<TEntity> For<TField>(Expression<Func<TEntity, TField>> forExpression) where TField : IComparable<TField>;

        IIndexBuilder<TEntity> Include(Expression<Func<TEntity, object>> includeExpression);
    }

    internal abstract class IndexBuilder
    {
        public Func<MapperHolder, IIndex> BuildFunction { get; protected set; }
    }

    internal class IndexBuilder<TEntity> : IndexBuilder, IIndexBuilder<TEntity>
    {
        private string _name;
        private List<Expression<Func<TEntity, object>>> _includeExpressions;

        public IndexBuilder()
        {
            _includeExpressions = new List<Expression<Func<TEntity, object>>>();
        }

        public IIndexBuilder<TEntity> Name(string name)
        {
            _name = name;
            return this;
        }

        public IIndexBuilder<TEntity> For<TField>(Expression<Func<TEntity, TField>> indexedFieldExpression) where TField : IComparable<TField>
        {
            BuildFunction = (mapperHolder) =>
            {
                var initializer = new IndexInitializer<TEntity>(mapperHolder);
                return initializer.GetIndex(_name, indexedFieldExpression, _includeExpressions);
            };
            return this;
        }

        public IIndexBuilder<TEntity> Include(Expression<Func<TEntity, object>> includeExpression)
        {
            _includeExpressions.Add(includeExpression);
            return this;
        }
    }
}
