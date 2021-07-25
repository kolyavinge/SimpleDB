using NUnit.Framework;
using SimpleDB.Infrastructure;
using SimpleDB.Test.Tools;

namespace SimpleDB.Test
{
    class DBEngineBuilderTest
    {
        [SetUp]
        public void Setup()
        {
            IOC.Reset();
            IOC.Set<IFileSystem>(new MemoryFileSystem());
        }

        [Test]
        public void BuildEngine()
        {
            var builder = DBEngineBuilder.Make();
            builder.WorkingDirectory("WorkingDirectory");
            builder.Map<TestEntity>()
                .Name("test entity")
                .PrimaryKey(x => x.Id)
                .Field(0, x => x.Name);
            var engine = builder.BuildEngine();
            Assert.IsNotNull(engine);
            var collection = engine.GetCollection<TestEntity>();
            Assert.IsNotNull(collection);
        }

        class TestEntity
        {
            public int Id { get; set; }

            public string Name { get; set; }
        }
    }
}
