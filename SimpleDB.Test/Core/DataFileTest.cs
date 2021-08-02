using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;
using SimpleDB.Core;
using SimpleDB.Infrastructure;
using SimpleDB.Test.Tools;

namespace SimpleDB.Test.Core
{
    class DataFileTest
    {
        private FieldMeta[] _fieldMetaCollection;

        [SetUp]
        public void Setup()
        {
            IOC.Reset();
            IOC.Set<IFileSystem>(new MemoryFileSystem());
            IOC.Set<IMemory>(new Memory());
            _fieldMetaCollection = new FieldMeta[]
            {
                new FieldMeta(0, typeof(bool)),
                new FieldMeta(1, typeof(sbyte)),
                new FieldMeta(2, typeof(byte)),
                new FieldMeta(3, typeof(char)),
                new FieldMeta(4, typeof(short)),
                new FieldMeta(5, typeof(ushort)),
                new FieldMeta(6, typeof(int)),
                new FieldMeta(7, typeof(uint)),
                new FieldMeta(8, typeof(long)),
                new FieldMeta(9, typeof(ulong)),
                new FieldMeta(10, typeof(float)),
                new FieldMeta(11, typeof(double)),
                new FieldMeta(12, typeof(decimal)),
                new FieldMeta(13, typeof(string))
            };
        }

        [Test]
        public void CreateFile()
        {
            IOC.Reset();
            IOC.Set<IMemory>(new Memory());
            var fileSystem = new MemoryFileSystem();
            IOC.Set<IFileSystem>(fileSystem);
            new DataFile(@"working directory\testEntity.data", Enumerable.Empty<FieldMeta>());
            Assert.True(fileSystem.FullFilePathes.Contains(@"working directory\testEntity.data"));
        }

        [Test]
        public void InsertAndReadFields()
        {
            var dataFile = new DataFile("", _fieldMetaCollection);
            var fieldValueCollection = new FieldValue[]
            {
                new FieldValue(0, false),
                new FieldValue(1, (sbyte)1),
                new FieldValue(2, (byte)2),
                new FieldValue(3, 'a'),
                new FieldValue(4, (short)4),
                new FieldValue(5, (ushort)5),
                new FieldValue(6, 6),
                new FieldValue(7, (uint)7),
                new FieldValue(8, (long)8),
                new FieldValue(9, (ulong)9),
                new FieldValue(10, (float)1.2),
                new FieldValue(11, (double)3.4),
                new FieldValue(12, (decimal)5.6),
                new FieldValue(13, "1234567890"),
            };
            var insertResult = dataFile.Insert(fieldValueCollection);

            var readFieldsResult = new FieldValue[_fieldMetaCollection.Length];
            dataFile.ReadFields(0, insertResult.EndDataFileOffset, readFieldsResult);
            Assert.AreEqual(14, readFieldsResult.Length);

            Assert.AreEqual(0, readFieldsResult[0].Number);
            Assert.AreEqual(1, readFieldsResult[1].Number);
            Assert.AreEqual(2, readFieldsResult[2].Number);
            Assert.AreEqual(3, readFieldsResult[3].Number);
            Assert.AreEqual(4, readFieldsResult[4].Number);
            Assert.AreEqual(5, readFieldsResult[5].Number);
            Assert.AreEqual(6, readFieldsResult[6].Number);
            Assert.AreEqual(7, readFieldsResult[7].Number);
            Assert.AreEqual(8, readFieldsResult[8].Number);
            Assert.AreEqual(9, readFieldsResult[9].Number);
            Assert.AreEqual(10, readFieldsResult[10].Number);
            Assert.AreEqual(11, readFieldsResult[11].Number);
            Assert.AreEqual(12, readFieldsResult[12].Number);
            Assert.AreEqual(13, readFieldsResult[13].Number);

            Assert.AreEqual(false, readFieldsResult[0].Value);
            Assert.AreEqual((sbyte)1, readFieldsResult[1].Value);
            Assert.AreEqual((byte)2, readFieldsResult[2].Value);
            Assert.AreEqual('a', readFieldsResult[3].Value);
            Assert.AreEqual((short)4, readFieldsResult[4].Value);
            Assert.AreEqual((ushort)5, readFieldsResult[5].Value);
            Assert.AreEqual(6, readFieldsResult[6].Value);
            Assert.AreEqual((uint)7, readFieldsResult[7].Value);
            Assert.AreEqual((long)8, readFieldsResult[8].Value);
            Assert.AreEqual((ulong)9, readFieldsResult[9].Value);
            Assert.AreEqual((float)1.2, readFieldsResult[10].Value);
            Assert.AreEqual((double)3.4, readFieldsResult[11].Value);
            Assert.AreEqual((decimal)5.6, readFieldsResult[12].Value);
            Assert.AreEqual("1234567890", readFieldsResult[13].Value);
        }

        [Test]
        public void InsertAndReadFields_Object()
        {
            var fieldMetaCollection = new FieldMeta[]
            {
                new FieldMeta(0, typeof(TestEntity))
            };
            var dataFile = new DataFile("", fieldMetaCollection);
            var obj = new TestEntity
            {
                Bool = false,
                SByte = 1,
                Byte = 2,
                Char = 'a',
                Short = 4,
                UShort = 5,
                Int = 6,
                UInt = 7,
                Long = 8,
                ULong = 9,
                Float = 1.2f,
                Double = 3.4,
                Decimal = 5.6m,
                String = "1234567890"
            };
            var fieldValueCollection = new FieldValue[]
            {
                new FieldValue(0, obj)
            };
            var insertResult = dataFile.Insert(fieldValueCollection);

            var readFieldsResult = new FieldValue[fieldMetaCollection.Length];
            dataFile.ReadFields(0, insertResult.EndDataFileOffset, readFieldsResult);
            Assert.AreEqual(1, readFieldsResult.Length);
            var resultObject = (TestEntity)readFieldsResult.First().Value;

            Assert.AreEqual(false, resultObject.Bool);
            Assert.AreEqual((sbyte)1, resultObject.SByte);
            Assert.AreEqual((byte)2, resultObject.Byte);
            Assert.AreEqual('a', resultObject.Char);
            Assert.AreEqual((short)4, resultObject.Short);
            Assert.AreEqual((ushort)5, resultObject.UShort);
            Assert.AreEqual(6, resultObject.Int);
            Assert.AreEqual((uint)7, resultObject.UInt);
            Assert.AreEqual((long)8, resultObject.Long);
            Assert.AreEqual((ulong)9, resultObject.ULong);
            Assert.AreEqual((float)1.2, resultObject.Float);
            Assert.AreEqual((double)3.4, resultObject.Double);
            Assert.AreEqual((decimal)5.6, resultObject.Decimal);
            Assert.AreEqual("1234567890", resultObject.String);
        }

        [Test]
        public void InsertManyEntities()
        {
            var dataFile = new DataFile("", _fieldMetaCollection);
            var fieldValueCollection = new FieldValue[]
            {
                new FieldValue(0, false),
                new FieldValue(1, (sbyte)1),
                new FieldValue(2, (byte)2),
                new FieldValue(3, 'a'),
                new FieldValue(4, (short)4),
                new FieldValue(5, (ushort)5),
                new FieldValue(6, 6),
                new FieldValue(7, (uint)7),
                new FieldValue(8, (long)8),
                new FieldValue(9, (ulong)9),
                new FieldValue(10, (float)1.2),
                new FieldValue(11, (double)3.4),
                new FieldValue(12, (decimal)5.6),
                new FieldValue(13, "1234567890"),
            };
            var insertResult = dataFile.Insert(fieldValueCollection);
            dataFile.Insert(fieldValueCollection);
            dataFile.Insert(fieldValueCollection);

            var readFieldsResult = new FieldValue[_fieldMetaCollection.Length];
            dataFile.ReadFields(0, insertResult.EndDataFileOffset, readFieldsResult);
            Assert.AreEqual(14, readFieldsResult.Length);
        }

        [Test]
        public void UpdateLengthEqual()
        {
            var dataFile = new DataFile("", _fieldMetaCollection);
            var fieldValueCollection = new FieldValue[]
            {
                new FieldValue(0, false),
                new FieldValue(1, (sbyte)1),
                new FieldValue(2, (byte)2),
                new FieldValue(3, 'a'),
                new FieldValue(4, (short)4),
                new FieldValue(5, (ushort)5),
                new FieldValue(6, 6),
                new FieldValue(7, (uint)7),
                new FieldValue(8, (long)8),
                new FieldValue(9, (ulong)9),
                new FieldValue(10, (float)1.2),
                new FieldValue(11, (double)3.4),
                new FieldValue(12, (decimal)5.6),
                new FieldValue(13, "1234567890"),
            };
            var insertResult = dataFile.Insert(fieldValueCollection);

            fieldValueCollection = new FieldValue[]
            {
                new FieldValue(0, true),
                new FieldValue(1, (sbyte)10),
                new FieldValue(2, (byte)20),
                new FieldValue(3, 'b'),
                new FieldValue(4, (short)40),
                new FieldValue(5, (ushort)50),
                new FieldValue(6, 60),
                new FieldValue(7, (uint)70),
                new FieldValue(8, (long)80),
                new FieldValue(9, (ulong)90),
                new FieldValue(10, (float)10.2),
                new FieldValue(11, (double)30.4),
                new FieldValue(12, (decimal)50.6),
                new FieldValue(13, "0987654321"),
            };
            var updateResult = dataFile.Update(insertResult.StartDataFileOffset, insertResult.EndDataFileOffset, fieldValueCollection);
            Assert.AreEqual(insertResult.StartDataFileOffset, updateResult.NewStartDataFileOffset);
            Assert.AreEqual(insertResult.EndDataFileOffset, updateResult.NewEndDataFileOffset);

            var readFieldsResult = new FieldValue[_fieldMetaCollection.Length];
            dataFile.ReadFields(updateResult.NewStartDataFileOffset, updateResult.NewEndDataFileOffset, readFieldsResult);
            Assert.AreEqual(true, readFieldsResult[0].Value);
            Assert.AreEqual((sbyte)10, readFieldsResult[1].Value);
            Assert.AreEqual((byte)20, readFieldsResult[2].Value);
            Assert.AreEqual('b', readFieldsResult[3].Value);
            Assert.AreEqual((short)40, readFieldsResult[4].Value);
            Assert.AreEqual((ushort)50, readFieldsResult[5].Value);
            Assert.AreEqual(60, readFieldsResult[6].Value);
            Assert.AreEqual((uint)70, readFieldsResult[7].Value);
            Assert.AreEqual((long)80, readFieldsResult[8].Value);
            Assert.AreEqual((ulong)90, readFieldsResult[9].Value);
            Assert.AreEqual((float)10.2, readFieldsResult[10].Value);
            Assert.AreEqual((double)30.4, readFieldsResult[11].Value);
            Assert.AreEqual((decimal)50.6, readFieldsResult[12].Value);
            Assert.AreEqual("0987654321", readFieldsResult[13].Value);
        }

        [Test]
        public void UpdateLengthLess()
        {
            var dataFile = new DataFile("", _fieldMetaCollection);
            var fieldValueCollection = new FieldValue[]
            {
                new FieldValue(0, false),
                new FieldValue(1, (sbyte)1),
                new FieldValue(2, (byte)2),
                new FieldValue(3, 'a'),
                new FieldValue(4, (short)4),
                new FieldValue(5, (ushort)5),
                new FieldValue(6, 6),
                new FieldValue(7, (uint)7),
                new FieldValue(8, (long)8),
                new FieldValue(9, (ulong)9),
                new FieldValue(10, (float)1.2),
                new FieldValue(11, (double)3.4),
                new FieldValue(12, (decimal)5.6),
                new FieldValue(13, "1234567890"),
            };
            var insertResult = dataFile.Insert(fieldValueCollection);

            fieldValueCollection = new FieldValue[]
            {
                new FieldValue(0, true),
                new FieldValue(1, (sbyte)10),
                new FieldValue(2, (byte)20),
                new FieldValue(3, 'b'),
                new FieldValue(4, (short)40),
                new FieldValue(5, (ushort)50),
                new FieldValue(6, 60),
                new FieldValue(7, (uint)70),
                new FieldValue(8, (long)80),
                new FieldValue(9, (ulong)90),
                new FieldValue(10, (float)10.2),
                new FieldValue(11, (double)30.4),
                new FieldValue(12, (decimal)50.6),
                new FieldValue(13, "0"),
            };
            var updateResult = dataFile.Update(insertResult.StartDataFileOffset, insertResult.EndDataFileOffset, fieldValueCollection);
            Assert.AreEqual(insertResult.StartDataFileOffset, updateResult.NewStartDataFileOffset);
            Assert.IsTrue(insertResult.EndDataFileOffset > updateResult.NewEndDataFileOffset);

            var readFieldsResult = new FieldValue[_fieldMetaCollection.Length];
            dataFile.ReadFields(updateResult.NewStartDataFileOffset, updateResult.NewEndDataFileOffset, readFieldsResult);
            Assert.AreEqual(true, readFieldsResult[0].Value);
            Assert.AreEqual((sbyte)10, readFieldsResult[1].Value);
            Assert.AreEqual((byte)20, readFieldsResult[2].Value);
            Assert.AreEqual('b', readFieldsResult[3].Value);
            Assert.AreEqual((short)40, readFieldsResult[4].Value);
            Assert.AreEqual((ushort)50, readFieldsResult[5].Value);
            Assert.AreEqual(60, readFieldsResult[6].Value);
            Assert.AreEqual((uint)70, readFieldsResult[7].Value);
            Assert.AreEqual((long)80, readFieldsResult[8].Value);
            Assert.AreEqual((ulong)90, readFieldsResult[9].Value);
            Assert.AreEqual((float)10.2, readFieldsResult[10].Value);
            Assert.AreEqual((double)30.4, readFieldsResult[11].Value);
            Assert.AreEqual((decimal)50.6, readFieldsResult[12].Value);
            Assert.AreEqual("0", readFieldsResult[13].Value);
        }

        [Test]
        public void UpdateLengthChange()
        {
            var dataFile = new DataFile("", _fieldMetaCollection);
            var fieldValueCollection = new FieldValue[]
            {
                new FieldValue(0, false),
                new FieldValue(1, (sbyte)1),
                new FieldValue(2, (byte)2),
                new FieldValue(3, 'a'),
                new FieldValue(4, (short)4),
                new FieldValue(5, (ushort)5),
                new FieldValue(6, 6),
                new FieldValue(7, (uint)7),
                new FieldValue(8, (long)8),
                new FieldValue(9, (ulong)9),
                new FieldValue(10, (float)1.2),
                new FieldValue(11, (double)3.4),
                new FieldValue(12, (decimal)5.6),
                new FieldValue(13, "1234567890"),
            };
            var insertResult = dataFile.Insert(fieldValueCollection);

            fieldValueCollection = new FieldValue[]
            {
                new FieldValue(0, true),
                new FieldValue(1, (sbyte)10),
                new FieldValue(2, (byte)20),
                new FieldValue(3, 'b'),
                new FieldValue(4, (short)40),
                new FieldValue(5, (ushort)50),
                new FieldValue(6, 60),
                new FieldValue(7, (uint)70),
                new FieldValue(8, (long)80),
                new FieldValue(9, (ulong)90),
                new FieldValue(10, (float)10.2),
                new FieldValue(11, (double)30.4),
                new FieldValue(12, (decimal)50.6),
                new FieldValue(13, "0987654321098765432109876543210987654321"),
            };
            var updateResult = dataFile.Update(insertResult.StartDataFileOffset, insertResult.EndDataFileOffset, fieldValueCollection);
            Assert.IsTrue(insertResult.StartDataFileOffset < updateResult.NewStartDataFileOffset);
            Assert.IsTrue(insertResult.EndDataFileOffset < updateResult.NewEndDataFileOffset);

            var readFieldsResult = new FieldValue[_fieldMetaCollection.Length];
            dataFile.ReadFields(updateResult.NewStartDataFileOffset, updateResult.NewEndDataFileOffset, readFieldsResult);
            Assert.AreEqual(true, readFieldsResult[0].Value);
            Assert.AreEqual((sbyte)10, readFieldsResult[1].Value);
            Assert.AreEqual((byte)20, readFieldsResult[2].Value);
            Assert.AreEqual('b', readFieldsResult[3].Value);
            Assert.AreEqual((short)40, readFieldsResult[4].Value);
            Assert.AreEqual((ushort)50, readFieldsResult[5].Value);
            Assert.AreEqual(60, readFieldsResult[6].Value);
            Assert.AreEqual((uint)70, readFieldsResult[7].Value);
            Assert.AreEqual((long)80, readFieldsResult[8].Value);
            Assert.AreEqual((ulong)90, readFieldsResult[9].Value);
            Assert.AreEqual((float)10.2, readFieldsResult[10].Value);
            Assert.AreEqual((double)30.4, readFieldsResult[11].Value);
            Assert.AreEqual((decimal)50.6, readFieldsResult[12].Value);
            Assert.AreEqual("0987654321098765432109876543210987654321", readFieldsResult[13].Value);
        }

        [Test]
        public void InsertNullString()
        {
            var dataFile = new DataFile("", _fieldMetaCollection);
            var fieldValueCollection = new FieldValue[]
            {
                new FieldValue(13, null)
            };
            var insertResult = dataFile.Insert(fieldValueCollection);
            var readFieldsResult = new FieldValue[_fieldMetaCollection.Length];
            dataFile.ReadFields(insertResult.StartDataFileOffset, insertResult.EndDataFileOffset, readFieldsResult);
            Assert.AreEqual(null, readFieldsResult[13].Value);
        }

        [Test]
        public void InsertNullInnerObject()
        {
            var fieldMetaCollection = new FieldMeta[]
            {
                new FieldMeta(0, typeof(InnerObject)),
            };
            var fieldValueCollection = new FieldValue[]
            {
                new FieldValue(0, null)
            };
            var dataFile = new DataFile("", fieldMetaCollection);
            var insertResult = dataFile.Insert(fieldValueCollection);
            var readFieldsResult = new FieldValue[1];
            dataFile.ReadFields(insertResult.StartDataFileOffset, insertResult.EndDataFileOffset, readFieldsResult);
            Assert.AreEqual(null, readFieldsResult[0].Value);
        }

        [Test]
        public void SkipString()
        {
            var fieldMetaCollection = new FieldMeta[]
            {
                new FieldMeta(0, typeof(string)),
                new FieldMeta(1, typeof(int)),
            };
            var fieldValueCollection = new FieldValue[]
            {
                new FieldValue(0, "йцукенгшщзхъфывапролджэячсмитьбю 1234567890"),
                new FieldValue(1, 10),
            };
            var dataFile = new DataFile("", fieldMetaCollection);
            var insertResult = dataFile.Insert(fieldValueCollection);
            var fieldNumbers = new HashSet<byte> { 1 };
            var readFieldsResult = new Dictionary<byte, FieldValue>();
            dataFile.ReadFields(insertResult.StartDataFileOffset, insertResult.EndDataFileOffset, fieldNumbers, readFieldsResult);
            Assert.AreEqual(10, readFieldsResult[1].Value);
        }

        [Test]
        public void SkipEmptyString()
        {
            var fieldMetaCollection = new FieldMeta[]
            {
                new FieldMeta(0, typeof(string)),
                new FieldMeta(1, typeof(int)),
            };
            var fieldValueCollection = new FieldValue[]
            {
                new FieldValue(0, ""),
                new FieldValue(1, 10),
            };
            var dataFile = new DataFile("", fieldMetaCollection);
            var insertResult = dataFile.Insert(fieldValueCollection);
            var fieldNumbers = new HashSet<byte> { 1 };
            var readFieldsResult = new Dictionary<byte, FieldValue>();
            dataFile.ReadFields(insertResult.StartDataFileOffset, insertResult.EndDataFileOffset, fieldNumbers, readFieldsResult);
            Assert.AreEqual(10, readFieldsResult[1].Value);
        }

        [Test]
        public void SkipNullString()
        {
            var fieldMetaCollection = new FieldMeta[]
            {
                new FieldMeta(0, typeof(string)),
                new FieldMeta(1, typeof(int)),
            };
            var fieldValueCollection = new FieldValue[]
            {
                new FieldValue(0, null),
                new FieldValue(1, 10),
            };
            var dataFile = new DataFile("", fieldMetaCollection);
            var insertResult = dataFile.Insert(fieldValueCollection);
            var fieldNumbers = new HashSet<byte> { 1 };
            var readFieldsResult = new Dictionary<byte, FieldValue>();
            dataFile.ReadFields(insertResult.StartDataFileOffset, insertResult.EndDataFileOffset, fieldNumbers, readFieldsResult);
            Assert.AreEqual(10, readFieldsResult[1].Value);
        }

        class TestEntity
        {
            public bool Bool { get; set; }
            public sbyte SByte { get; set; }
            public byte Byte { get; set; }
            public char Char { get; set; }
            public short Short { get; set; }
            public ushort UShort { get; set; }
            public int Int { get; set; }
            public uint UInt { get; set; }
            public long Long { get; set; }
            public ulong ULong { get; set; }
            public float Float { get; set; }
            public double Double { get; set; }
            public decimal Decimal { get; set; }
            public string String { get; set; }
        }

        class TestEntityWithInnerObject
        {
            public InnerObject Value { get; set; }
        }

        class InnerObject
        {
            public int Value { get; set; }
        }

        class TestEntityWithString
        {
            public string String { get; set; }
            public int Int { get; set; }
        }
    }
}
