using System.Linq;
using NUnit.Framework;
using SimpleDB.Core;

namespace SimpleDB.Test.Core
{
    class MapperTest
    {
        private TestEntity _entity;
        private Mapper<TestEntity> _mapper;

        [SetUp]
        public void Setup()
        {
            _entity = new TestEntity { Id = 123, Byte = 45, Float = 6.7f, String = "123" };

            _mapper = new Mapper<TestEntity>(
                "testEntity",
                new PrimaryKeyMapping<TestEntity>(entity => entity.Id),
                new FieldMapping<TestEntity>[]
                {
                    new FieldMapping<TestEntity>(0, entity => entity.Byte),
                    new FieldMapping<TestEntity>(1, entity => entity.Float),
                    new FieldMapping<TestEntity>(2, entity => entity.String)
                });
        }

        [Test]
        public void PrimaryKeyType()
        {
            Assert.AreEqual(typeof(int), _mapper.PrimaryKeyMapping.PropertyType);
        }

        [Test]
        public void FieldMetaCollection()
        {
            var result = _mapper.FieldMetaCollection.ToList();
            Assert.AreEqual(3, result.Count);
            Assert.AreEqual(0, result[0].Number);
            Assert.AreEqual(1, result[1].Number);
            Assert.AreEqual(2, result[2].Number);
            Assert.AreEqual(typeof(byte), result[0].Type);
            Assert.AreEqual(typeof(float), result[1].Type);
            Assert.AreEqual(typeof(string), result[2].Type);
        }

        [Test]
        public void GetPrimaryKeyValue()
        {
            Assert.AreEqual(123, _mapper.GetPrimaryKeyValue(_entity));
        }

        [Test]
        public void GetFieldValueCollection()
        {
            var result = _mapper.GetFieldValueCollection(_entity).ToList();
            Assert.AreEqual(3, result.Count);
            Assert.AreEqual(0, result[0].Number);
            Assert.AreEqual(1, result[1].Number);
            Assert.AreEqual(2, result[2].Number);
            Assert.AreEqual((byte)45, result[0].Value);
            Assert.AreEqual(6.7f, result[1].Value);
            Assert.AreEqual("123", result[2].Value);
        }

        [Test]
        public void GetEntity_IncludePrimaryKeyYes()
        {
            var fieldValueCollection = new FieldValue[]
            {
                new FieldValue(0, (byte)45),
                new FieldValue(1, 6.7f),
                new FieldValue(2, "123")
            };
            var result = _mapper.GetEntity(123, fieldValueCollection, true);
            Assert.AreEqual(123, result.Id);
            Assert.AreEqual((byte)45, result.Byte);
            Assert.AreEqual(6.7f, result.Float);
            Assert.AreEqual("123", result.String);
        }

        [Test]
        public void GetEntity_IncludePrimaryKeyNo()
        {
            var fieldValueCollection = new FieldValue[]
            {
                new FieldValue(0, (byte)45),
                new FieldValue(1, 6.7f),
                new FieldValue(2, "123")
            };
            var result = _mapper.GetEntity(123, fieldValueCollection, false);
            Assert.AreEqual(0, result.Id);
            Assert.AreEqual((byte)45, result.Byte);
            Assert.AreEqual(6.7f, result.Float);
            Assert.AreEqual("123", result.String);
        }

        [Test]
        public void GetEntity_SetFunctions()
        {
            _mapper = new Mapper<TestEntity>(
                "testEntity",
                new PrimaryKeyMapping<TestEntity>(entity => entity.Id),
                new FieldMapping<TestEntity>[]
                {
                    new FieldMapping<TestEntity>(0, entity => entity.Byte),
                    new FieldMapping<TestEntity>(1, entity => entity.Float),
                    new FieldMapping<TestEntity>(2, entity => entity.String)
                });
            _mapper.MakeFunction = () => new TestEntity();
            _mapper.PrimaryKeySetFunction = (primaryKeyValue, entity) => entity.Id = (int)primaryKeyValue;
            _mapper.FieldSetFunction = (fieldNumber, fieldValue, entity) =>
            {
                if (fieldNumber == 0) entity.Byte = (byte)fieldValue;
                if (fieldNumber == 1) entity.Float = (float)fieldValue;
                if (fieldNumber == 2) entity.String = (string)fieldValue;
            };

            var fieldValueCollection = new FieldValue[]
            {
                new FieldValue(0, (byte)45),
                new FieldValue(1, 6.7f),
                new FieldValue(2, "123")
            };
            var result = _mapper.GetEntity(123, fieldValueCollection, false);
            Assert.AreEqual(0, result.Id);
            Assert.AreEqual((byte)45, result.Byte);
            Assert.AreEqual(6.7f, result.Float);
            Assert.AreEqual("123", result.String);
        }

        [Test]
        public void PrimaryKeyObject()
        {
            var entity = new TestEntityPrimaryKeyObject { Id = new TestPrimaryKey() };

            var mapper = new Mapper<TestEntityPrimaryKeyObject>(
                "testEntity",
                new PrimaryKeyMapping<TestEntityPrimaryKeyObject>(entity => entity.Id),
                new FieldMapping<TestEntityPrimaryKeyObject>[0]);
        }

        class TestEntity
        {
            public int Id { get; set; }

            public byte Byte { get; set; }

            public float Float { get; set; }

            public string String { get; set; }
        }

        class TestEntityPrimaryKeyObject
        {
            public TestPrimaryKey Id { get; set; }
        }

        class TestPrimaryKey
        {
            public int Value { get; set; }
        }
    }
}
