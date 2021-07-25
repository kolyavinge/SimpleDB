using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using SimpleDB.Core;
using SimpleDB.Infrastructure;

namespace SimpleDB
{
    public sealed class DBEngineBuilder
    {
        private string _workingDirectory;
        private List<MapperBuilder> _mapperBuilders = new List<MapperBuilder>();

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
            _workingDirectory = workingDirectory;
        }

        public IMapperBuilder<TEntity> Map<TEntity>()
        {
            var mapperBuilder = new MapperBuilder<TEntity>();
            _mapperBuilders.Add(mapperBuilder);
            return mapperBuilder;
        }

        public IDBEngine BuildEngine()
        {
            var mappers = _mapperBuilders.Select(x => x.Build()).ToList();
            return new DBEngine(_workingDirectory, new MapperHolder(mappers));
        }
    }

    public interface IMapperBuilder<TEntity>
    {
        IMapperBuilder<TEntity> Name(string name);

        IMapperBuilder<TEntity> PrimaryKey(Expression<Func<TEntity, object>> primaryKeyExpression);

        IMapperBuilder<TEntity> Field(byte number, Expression<Func<TEntity, object>> fieldExpression);
    }

    internal abstract class MapperBuilder
    {
        public abstract object Build();
    }

    internal class MapperBuilder<TEntity> : MapperBuilder, IMapperBuilder<TEntity>
    {
        private string _name;
        private PrimaryKeyMapping<TEntity> _primaryKeyMapping;
        private List<FieldMapping<TEntity>> _fieldMappings = new List<FieldMapping<TEntity>>();

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

        public IMapperBuilder<TEntity> Field(byte number, Expression<Func<TEntity, object>> fieldExpression)
        {
            _fieldMappings.Add(new FieldMapping<TEntity>(number, fieldExpression));
            return this;
        }

        public override object Build()
        {
            return new Mapper<TEntity>(_name, _primaryKeyMapping, _fieldMappings);
        }
    }
}
