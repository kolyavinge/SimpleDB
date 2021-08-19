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
                .Field(0, x => x.Int)
                .Field(1, x => x.Name, new FieldSettings { Compressed = true });
            var engine = builder.BuildEngine();
            Assert.IsNotNull(engine);
            var collection = engine.GetCollection<TestEntity>();
            Assert.IsNotNull(collection);
        }

        [Test]
        public void BuildEngine_ValueTypeCompressed_Error()
        {
            try
            {
                var builder = DBEngineBuilder.Make();
                builder.WorkingDirectory("WorkingDirectory");
                builder.Map<TestEntity>()
                    .Name("test entity")
                    .PrimaryKey(x => x.Id)
                    .Field(0, x => x.Int, new FieldSettings { Compressed = true })
                    .Field(1, x => x.Name);

                Assert.Fail();
            }
            catch (DBEngineException exp)
            {
                Assert.AreEqual("Value type cannot be compressed", exp.Message);
            }
        }

        [Test]
        public void BuildEngine_ValueTypeNotCompressed_Error()
        {

            var builder = DBEngineBuilder.Make();
            builder.WorkingDirectory("WorkingDirectory");
            builder.Map<TestEntity>()
                .Name("test entity")
                .PrimaryKey(x => x.Id)
                .Field(0, x => x.Int, new FieldSettings { Compressed = false })
                .Field(1, x => x.Name);

            Assert.Pass();
        }

        class TestEntity
        {
            public int Id { get; set; }

            public int Int { get; set; }

            public string Name { get; set; }
        }
    }
}
