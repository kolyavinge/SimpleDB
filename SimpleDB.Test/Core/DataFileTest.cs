﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using SimpleDB.Core;
using SimpleDB.Infrastructure;
using SimpleDB.Test.Tools;

namespace SimpleDB.Test.Core;

class DataFileTest
{
    private MemoryFileSystem _fileSystem;
    private Memory _memory;
    private FieldMeta[] _fieldMetaCollection;
    private HashSet<byte> _fieldNumbers;

    [SetUp]
    public void Setup()
    {
        _fileSystem = new MemoryFileSystem();
        _memory = Memory.Instance;
        _fieldMetaCollection = new FieldMeta[]
        {
            new FieldMeta(1,  "", typeof(bool)),
            new FieldMeta(2,  "", typeof(sbyte)),
            new FieldMeta(3,  "", typeof(byte)),
            new FieldMeta(4,  "", typeof(char)),
            new FieldMeta(5,  "", typeof(short)),
            new FieldMeta(6,  "", typeof(ushort)),
            new FieldMeta(7,  "", typeof(int)),
            new FieldMeta(8,  "", typeof(uint)),
            new FieldMeta(9,  "", typeof(long)),
            new FieldMeta(10, "", typeof(ulong)),
            new FieldMeta(11, "", typeof(float)),
            new FieldMeta(12, "", typeof(double)),
            new FieldMeta(13, "", typeof(decimal)),
            new FieldMeta(14, "", typeof(DateTime)),
            new FieldMeta(15, "", typeof(string)),
            new FieldMeta(16, "", typeof(byte[]))
        };
        _fieldNumbers = _fieldMetaCollection.Select(x => x.Number).ToHashSet();
    }

    [Test]
    public void InsertAndReadFields()
    {
        var dataFile = new DataFile("", _fieldMetaCollection, _fileSystem, _memory);
        dataFile.BeginReadWrite();
        var fieldValueCollection = new FieldValue[]
        {
            new FieldValue(1, false),
            new FieldValue(2, (sbyte)1),
            new FieldValue(3, (byte)2),
            new FieldValue(4, 'a'),
            new FieldValue(5, (short)4),
            new FieldValue(6, (ushort)5),
            new FieldValue(7, 6),
            new FieldValue(8, (uint)7),
            new FieldValue(9, (long)8),
            new FieldValue(10, (ulong)9),
            new FieldValue(11, (float)1.2),
            new FieldValue(12, 3.4),
            new FieldValue(13, (decimal)5.6),
            new FieldValue(14, DateTime.Parse("2000-12-31")),
            new FieldValue(15, "1234567890"),
            new FieldValue(16, new byte[] { 1, 2, 3 })
        };
        var insertResult = dataFile.Insert(fieldValueCollection);

        var readFieldsResult = new FieldValueCollection();
        dataFile.ReadFields(0, insertResult.EndDataFileOffset, _fieldNumbers, readFieldsResult);
        Assert.AreEqual(16, readFieldsResult.Count);

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
        Assert.AreEqual(14, readFieldsResult[14].Number);
        Assert.AreEqual(15, readFieldsResult[15].Number);
        Assert.AreEqual(16, readFieldsResult[16].Number);

        Assert.AreEqual(false, readFieldsResult[1].Value);
        Assert.AreEqual((sbyte)1, readFieldsResult[2].Value);
        Assert.AreEqual((byte)2, readFieldsResult[3].Value);
        Assert.AreEqual('a', readFieldsResult[4].Value);
        Assert.AreEqual((short)4, readFieldsResult[5].Value);
        Assert.AreEqual((ushort)5, readFieldsResult[6].Value);
        Assert.AreEqual(6, readFieldsResult[7].Value);
        Assert.AreEqual((uint)7, readFieldsResult[8].Value);
        Assert.AreEqual((long)8, readFieldsResult[9].Value);
        Assert.AreEqual((ulong)9, readFieldsResult[10].Value);
        Assert.AreEqual((float)1.2, readFieldsResult[11].Value);
        Assert.AreEqual(3.4, readFieldsResult[12].Value);
        Assert.AreEqual((decimal)5.6, readFieldsResult[13].Value);
        Assert.AreEqual(DateTime.Parse("2000-12-31"), readFieldsResult[14].Value);
        Assert.AreEqual("1234567890", readFieldsResult[15].Value);
        Assert.AreEqual((byte)1, ((byte[])readFieldsResult[16].Value)[0]);
        Assert.AreEqual((byte)2, ((byte[])readFieldsResult[16].Value)[1]);
        Assert.AreEqual((byte)3, ((byte[])readFieldsResult[16].Value)[2]);
    }

    [Test]
    public void InsertAndReadFields_Object()
    {
        var fieldMetaCollection = new FieldMeta[]
        {
            new FieldMeta(1, "", typeof(TestEntity))
        };
        var dataFile = new DataFile("", fieldMetaCollection, _fileSystem, _memory);
        dataFile.BeginReadWrite();
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
            DateTime = DateTime.Parse("2000-12-31"),
            String = "1234567890"
        };
        var fieldValueCollection = new FieldValue[]
        {
            new FieldValue(1, obj)
        };
        var insertResult = dataFile.Insert(fieldValueCollection);

        var readFieldsResult = new FieldValueCollection();
        dataFile.ReadFields(0, insertResult.EndDataFileOffset, _fieldNumbers, readFieldsResult);
        Assert.AreEqual(1, readFieldsResult.Count);
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
        Assert.AreEqual(3.4, resultObject.Double);
        Assert.AreEqual((decimal)5.6, resultObject.Decimal);
        Assert.AreEqual(DateTime.Parse("2000-12-31"), resultObject.DateTime);
        Assert.AreEqual("1234567890", resultObject.String);
    }

    [Test]
    public void InsertManyEntities()
    {
        var dataFile = new DataFile("", _fieldMetaCollection, _fileSystem, _memory);
        dataFile.BeginReadWrite();
        var fieldValueCollection = new FieldValue[]
        {
            new FieldValue(1, false),
            new FieldValue(2, (sbyte)1),
            new FieldValue(3, (byte)2),
            new FieldValue(4, 'a'),
            new FieldValue(5, (short)4),
            new FieldValue(6, (ushort)5),
            new FieldValue(7, 6),
            new FieldValue(8, (uint)7),
            new FieldValue(9, (long)8),
            new FieldValue(10, (ulong)9),
            new FieldValue(11, (float)1.2),
            new FieldValue(12, 3.4),
            new FieldValue(13, (decimal)5.6),
            new FieldValue(14, DateTime.Parse("2000-12-31")),
            new FieldValue(15, "1234567890"),
            new FieldValue(16, new byte[] { 1, 2, 3 })
        };
        var insertResult = dataFile.Insert(fieldValueCollection);
        dataFile.Insert(fieldValueCollection);
        dataFile.Insert(fieldValueCollection);

        var readFieldsResult = new FieldValueCollection();
        dataFile.ReadFields(0, insertResult.EndDataFileOffset, _fieldNumbers, readFieldsResult);
        Assert.AreEqual(16, readFieldsResult.Count);
    }

    [Test]
    public void UpdateLengthEqual()
    {
        var dataFile = new DataFile("", _fieldMetaCollection, _fileSystem, _memory);
        dataFile.BeginReadWrite();
        var fieldValueCollection = new FieldValue[]
        {
            new FieldValue(1, false),
            new FieldValue(2, (sbyte)1),
            new FieldValue(3, (byte)2),
            new FieldValue(4, 'a'),
            new FieldValue(5, (short)4),
            new FieldValue(6, (ushort)5),
            new FieldValue(7, 6),
            new FieldValue(8, (uint)7),
            new FieldValue(9, (long)8),
            new FieldValue(10, (ulong)9),
            new FieldValue(11, (float)1.2),
            new FieldValue(12, 3.4),
            new FieldValue(13, (decimal)5.6),
            new FieldValue(14, DateTime.Parse("2000-12-31")),
            new FieldValue(15, "1234567890"),
            new FieldValue(16, new byte[] { 1, 2, 3 })
        };
        var insertResult = dataFile.Insert(fieldValueCollection);

        fieldValueCollection = new FieldValue[]
        {
            new FieldValue(1, true),
            new FieldValue(2, (sbyte)10),
            new FieldValue(3, (byte)20),
            new FieldValue(4, 'b'),
            new FieldValue(5, (short)40),
            new FieldValue(6, (ushort)50),
            new FieldValue(7, 60),
            new FieldValue(8, (uint)70),
            new FieldValue(9, (long)80),
            new FieldValue(10, (ulong)90),
            new FieldValue(11, (float)10.2),
            new FieldValue(12, 30.4),
            new FieldValue(13, (decimal)50.6),
            new FieldValue(14, DateTime.Parse("2000-12-31")),
            new FieldValue(15, "0987654321"),
            new FieldValue(16, new byte[] { 1, 2, 3 })
        };
        var updateResult = dataFile.Update(insertResult.StartDataFileOffset, insertResult.EndDataFileOffset, fieldValueCollection);
        Assert.AreEqual(insertResult.StartDataFileOffset, updateResult.NewStartDataFileOffset);
        Assert.AreEqual(insertResult.EndDataFileOffset, updateResult.NewEndDataFileOffset);

        var readFieldsResult = new FieldValueCollection();
        dataFile.ReadFields(updateResult.NewStartDataFileOffset, updateResult.NewEndDataFileOffset, _fieldNumbers, readFieldsResult);
        Assert.AreEqual(true, readFieldsResult[1].Value);
        Assert.AreEqual((sbyte)10, readFieldsResult[2].Value);
        Assert.AreEqual((byte)20, readFieldsResult[3].Value);
        Assert.AreEqual('b', readFieldsResult[4].Value);
        Assert.AreEqual((short)40, readFieldsResult[5].Value);
        Assert.AreEqual((ushort)50, readFieldsResult[6].Value);
        Assert.AreEqual(60, readFieldsResult[7].Value);
        Assert.AreEqual((uint)70, readFieldsResult[8].Value);
        Assert.AreEqual((long)80, readFieldsResult[9].Value);
        Assert.AreEqual((ulong)90, readFieldsResult[10].Value);
        Assert.AreEqual((float)10.2, readFieldsResult[11].Value);
        Assert.AreEqual(30.4, readFieldsResult[12].Value);
        Assert.AreEqual((decimal)50.6, readFieldsResult[13].Value);
        Assert.AreEqual(DateTime.Parse("2000-12-31"), readFieldsResult[14].Value);
        Assert.AreEqual("0987654321", readFieldsResult[15].Value);
    }

    [Test]
    public void UpdateLengthLess()
    {
        var dataFile = new DataFile("", _fieldMetaCollection, _fileSystem, _memory);
        dataFile.BeginReadWrite();
        var fieldValueCollection = new FieldValue[]
        {
            new FieldValue(1, false),
            new FieldValue(2, (sbyte)1),
            new FieldValue(3, (byte)2),
            new FieldValue(4, 'a'),
            new FieldValue(5, (short)4),
            new FieldValue(6, (ushort)5),
            new FieldValue(7, 6),
            new FieldValue(8, (uint)7),
            new FieldValue(9, (long)8),
            new FieldValue(10, (ulong)9),
            new FieldValue(11, (float)1.2),
            new FieldValue(12, 3.4),
            new FieldValue(13, (decimal)5.6),
            new FieldValue(14, DateTime.Parse("2000-12-31")),
            new FieldValue(15, "1234567890"),
        };
        var insertResult = dataFile.Insert(fieldValueCollection);

        fieldValueCollection = new FieldValue[]
        {
            new FieldValue(1, true),
            new FieldValue(2, (sbyte)10),
            new FieldValue(3, (byte)20),
            new FieldValue(4, 'b'),
            new FieldValue(5, (short)40),
            new FieldValue(6, (ushort)50),
            new FieldValue(7, 60),
            new FieldValue(8, (uint)70),
            new FieldValue(9, (long)80),
            new FieldValue(10, (ulong)90),
            new FieldValue(11, (float)10.2),
            new FieldValue(12, 30.4),
            new FieldValue(13, (decimal)50.6),
            new FieldValue(14, DateTime.Parse("2000-01-10")),
            new FieldValue(15, "0"),
        };
        var updateResult = dataFile.Update(insertResult.StartDataFileOffset, insertResult.EndDataFileOffset, fieldValueCollection);
        Assert.AreEqual(insertResult.StartDataFileOffset, updateResult.NewStartDataFileOffset);
        Assert.IsTrue(insertResult.EndDataFileOffset > updateResult.NewEndDataFileOffset);

        var readFieldsResult = new FieldValueCollection();
        dataFile.ReadFields(updateResult.NewStartDataFileOffset, updateResult.NewEndDataFileOffset, _fieldNumbers, readFieldsResult);
        Assert.AreEqual(true, readFieldsResult[1].Value);
        Assert.AreEqual((sbyte)10, readFieldsResult[2].Value);
        Assert.AreEqual((byte)20, readFieldsResult[3].Value);
        Assert.AreEqual('b', readFieldsResult[4].Value);
        Assert.AreEqual((short)40, readFieldsResult[5].Value);
        Assert.AreEqual((ushort)50, readFieldsResult[6].Value);
        Assert.AreEqual(60, readFieldsResult[7].Value);
        Assert.AreEqual((uint)70, readFieldsResult[8].Value);
        Assert.AreEqual((long)80, readFieldsResult[9].Value);
        Assert.AreEqual((ulong)90, readFieldsResult[10].Value);
        Assert.AreEqual((float)10.2, readFieldsResult[11].Value);
        Assert.AreEqual(30.4, readFieldsResult[12].Value);
        Assert.AreEqual((decimal)50.6, readFieldsResult[13].Value);
        Assert.AreEqual(DateTime.Parse("2000-01-10"), readFieldsResult[14].Value);
        Assert.AreEqual("0", readFieldsResult[15].Value);
    }

    [Test]
    public void UpdateLengthChange()
    {
        var dataFile = new DataFile("", _fieldMetaCollection, _fileSystem, _memory);
        dataFile.BeginReadWrite();
        var fieldValueCollection = new FieldValue[]
        {
            new FieldValue(1, false),
            new FieldValue(2, (sbyte)1),
            new FieldValue(3, (byte)2),
            new FieldValue(4, 'a'),
            new FieldValue(5, (short)4),
            new FieldValue(6, (ushort)5),
            new FieldValue(7, 6),
            new FieldValue(8, (uint)7),
            new FieldValue(9, (long)8),
            new FieldValue(10, (ulong)9),
            new FieldValue(11, (float)1.2),
            new FieldValue(12, 3.4),
            new FieldValue(13, (decimal)5.6),
            new FieldValue(14, DateTime.Parse("2000-12-31")),
            new FieldValue(15, "1234567890"),
        };
        var insertResult = dataFile.Insert(fieldValueCollection);

        fieldValueCollection = new FieldValue[]
        {
            new FieldValue(1, true),
            new FieldValue(2, (sbyte)10),
            new FieldValue(3, (byte)20),
            new FieldValue(4, 'b'),
            new FieldValue(5, (short)40),
            new FieldValue(6, (ushort)50),
            new FieldValue(7, 60),
            new FieldValue(8, (uint)70),
            new FieldValue(9, (long)80),
            new FieldValue(10, (ulong)90),
            new FieldValue(11, (float)10.2),
            new FieldValue(12, 30.4),
            new FieldValue(13, (decimal)50.6),
            new FieldValue(14, DateTime.Parse("2000-01-10")),
            new FieldValue(15, "0987654321098765432109876543210987654321"),
        };
        var updateResult = dataFile.Update(insertResult.StartDataFileOffset, insertResult.EndDataFileOffset, fieldValueCollection);
        Assert.IsTrue(insertResult.StartDataFileOffset < updateResult.NewStartDataFileOffset);
        Assert.IsTrue(insertResult.EndDataFileOffset < updateResult.NewEndDataFileOffset);

        var readFieldsResult = new FieldValueCollection();
        dataFile.ReadFields(updateResult.NewStartDataFileOffset, updateResult.NewEndDataFileOffset, _fieldNumbers, readFieldsResult);
        Assert.AreEqual(true, readFieldsResult[1].Value);
        Assert.AreEqual((sbyte)10, readFieldsResult[2].Value);
        Assert.AreEqual((byte)20, readFieldsResult[3].Value);
        Assert.AreEqual('b', readFieldsResult[4].Value);
        Assert.AreEqual((short)40, readFieldsResult[5].Value);
        Assert.AreEqual((ushort)50, readFieldsResult[6].Value);
        Assert.AreEqual(60, readFieldsResult[7].Value);
        Assert.AreEqual((uint)70, readFieldsResult[8].Value);
        Assert.AreEqual((long)80, readFieldsResult[9].Value);
        Assert.AreEqual((ulong)90, readFieldsResult[10].Value);
        Assert.AreEqual((float)10.2, readFieldsResult[11].Value);
        Assert.AreEqual(30.4, readFieldsResult[12].Value);
        Assert.AreEqual((decimal)50.6, readFieldsResult[13].Value);
        Assert.AreEqual(DateTime.Parse("2000-01-10"), readFieldsResult[14].Value);
        Assert.AreEqual("0987654321098765432109876543210987654321", readFieldsResult[15].Value);
    }

    [Test]
    public void UpdateManual()
    {
        var dataFile = new DataFile("", _fieldMetaCollection, _fileSystem, _memory);
        dataFile.BeginReadWrite();
        var fieldValueCollection = new FieldValue[]
        {
            new FieldValue(1, false),
            new FieldValue(2, (sbyte)1),
            new FieldValue(3, (byte)2),
            new FieldValue(4, 'a'),
            new FieldValue(5, (short)4),
            new FieldValue(6, (ushort)5),
            new FieldValue(7, 6),
            new FieldValue(8, (uint)7),
            new FieldValue(9, (long)8),
            new FieldValue(10, (ulong)9),
            new FieldValue(11, (float)1.2),
            new FieldValue(12, 3.4),
            new FieldValue(13, (decimal)5.6),
            new FieldValue(14, DateTime.Parse("2000-12-31")),
            new FieldValue(15, "1234567890"),
        };
        var insertResult = dataFile.Insert(fieldValueCollection);
        fieldValueCollection = new FieldValue[]
        {
            new FieldValue(1, true),
            new FieldValue(2, (sbyte)10),
            new FieldValue(3, (byte)20),
            new FieldValue(4, 'b'),
            new FieldValue(5, (short)40),
            new FieldValue(6, (ushort)50),
            new FieldValue(7, 60),
            new FieldValue(8, (uint)70),
            new FieldValue(9, (long)80),
            new FieldValue(10, (ulong)90),
            new FieldValue(11, (float)10.2),
            new FieldValue(12, 30.4),
            new FieldValue(13, (decimal)50.6),
            new FieldValue(14, DateTime.Parse("2000-01-10")),
            new FieldValue(15, "0987654321"),
        };
        dataFile.UpdateManual(insertResult.StartDataFileOffset, insertResult.EndDataFileOffset, fieldValueCollection);
        var readFieldsResult = new FieldValueCollection();
        dataFile.ReadFields(insertResult.StartDataFileOffset, insertResult.EndDataFileOffset, _fieldNumbers, readFieldsResult);
        Assert.AreEqual(true, readFieldsResult[1].Value);
        Assert.AreEqual((sbyte)10, readFieldsResult[2].Value);
        Assert.AreEqual((byte)20, readFieldsResult[3].Value);
        Assert.AreEqual('b', readFieldsResult[4].Value);
        Assert.AreEqual((short)40, readFieldsResult[5].Value);
        Assert.AreEqual((ushort)50, readFieldsResult[6].Value);
        Assert.AreEqual(60, readFieldsResult[7].Value);
        Assert.AreEqual((uint)70, readFieldsResult[8].Value);
        Assert.AreEqual((long)80, readFieldsResult[9].Value);
        Assert.AreEqual((ulong)90, readFieldsResult[10].Value);
        Assert.AreEqual((float)10.2, readFieldsResult[11].Value);
        Assert.AreEqual(30.4, readFieldsResult[12].Value);
        Assert.AreEqual((decimal)50.6, readFieldsResult[13].Value);
        Assert.AreEqual(DateTime.Parse("2000-01-10"), readFieldsResult[14].Value);
        Assert.AreEqual("0987654321", readFieldsResult[15].Value);
    }

    [Test]
    public void UpdateManual_Skip()
    {
        var dataFile = new DataFile("", _fieldMetaCollection, _fileSystem, _memory);
        dataFile.BeginReadWrite();
        var fieldValueCollection = new FieldValue[]
        {
            new FieldValue(1, false),
            new FieldValue(2, (sbyte)1),
            new FieldValue(3, (byte)2),
            new FieldValue(4, 'a'),
            new FieldValue(5, (short)4),
            new FieldValue(6, (ushort)5),
            new FieldValue(7, 6),
            new FieldValue(8, (uint)7),
            new FieldValue(9, (long)8),
            new FieldValue(10, (ulong)9),
            new FieldValue(11, (float)1.2),
            new FieldValue(12, 3.4),
            new FieldValue(13, (decimal)5.6),
            new FieldValue(14, DateTime.Parse("2000-12-31")),
            new FieldValue(15, "1234567890"),
        };
        var insertResult = dataFile.Insert(fieldValueCollection);
        fieldValueCollection = new FieldValue[]
        {
            new FieldValue(1, true),
            new FieldValue(3, (byte)20),
            new FieldValue(5, (short)40),
            new FieldValue(7, 60),
            new FieldValue(9, (long)80),
            new FieldValue(11, (float)10.2),
            new FieldValue(13, (decimal)50.6),
            new FieldValue(15, "0987654321"), // длина нового значения в байтах должна равняться старому, иначе ф-я работать не будет
        };
        dataFile.UpdateManual(insertResult.StartDataFileOffset, insertResult.EndDataFileOffset, fieldValueCollection);
        var readFieldsResult = new FieldValueCollection();
        dataFile.ReadFields(insertResult.StartDataFileOffset, insertResult.EndDataFileOffset, _fieldNumbers, readFieldsResult);
        Assert.AreEqual(true, readFieldsResult[1].Value);
        Assert.AreEqual((sbyte)1, readFieldsResult[2].Value);
        Assert.AreEqual((byte)20, readFieldsResult[3].Value);
        Assert.AreEqual('a', readFieldsResult[4].Value);
        Assert.AreEqual((short)40, readFieldsResult[5].Value);
        Assert.AreEqual((ushort)5, readFieldsResult[6].Value);
        Assert.AreEqual(60, readFieldsResult[7].Value);
        Assert.AreEqual((uint)7, readFieldsResult[8].Value);
        Assert.AreEqual((long)80, readFieldsResult[9].Value);
        Assert.AreEqual((ulong)9, readFieldsResult[10].Value);
        Assert.AreEqual((float)10.2, readFieldsResult[11].Value);
        Assert.AreEqual(3.4, readFieldsResult[12].Value);
        Assert.AreEqual((decimal)50.6, readFieldsResult[13].Value);
        Assert.AreEqual(DateTime.Parse("2000-12-31"), readFieldsResult[14].Value);
        Assert.AreEqual("0987654321", readFieldsResult[15].Value);
    }

    [Test]
    public void UpdateManual_StringAsByteArray()
    {
        var dataFile = new DataFile("", _fieldMetaCollection, _fileSystem, _memory);
        dataFile.BeginReadWrite();
        var fieldValueCollection = new FieldValue[]
        {
            new FieldValue(15, "01234")
        };
        var insertResult = dataFile.Insert(fieldValueCollection);
        fieldValueCollection = new FieldValue[]
        {
            new FieldValue(15, new byte[] { 48, 49, 50, 51, 52 })
        };
        dataFile.UpdateManual(insertResult.StartDataFileOffset, insertResult.EndDataFileOffset, fieldValueCollection);
        var readFieldsResult = new FieldValueCollection();
        dataFile.ReadFields(insertResult.StartDataFileOffset, insertResult.EndDataFileOffset, _fieldNumbers, readFieldsResult);
        Assert.AreEqual("01234", readFieldsResult[15].Value);
    }

    [Test]
    public void UpdateManual_ObjectAsByteArray()
    {
        var fieldMetaCollection = new FieldMeta[]
        {
            new FieldMeta(1, "", typeof(InnerObject))
        };
        var dataFile = new DataFile("", fieldMetaCollection, _fileSystem, _memory);
        dataFile.BeginReadWrite();
        var fieldValueCollection = new FieldValue[]
        {
            new FieldValue(1, new InnerObject { Value = 123 })
        };
        var insertResult = dataFile.Insert(fieldValueCollection);
        fieldValueCollection = new FieldValue[]
        {
            new FieldValue(1, Encoding.UTF8.GetBytes(JsonSerialization.ToJson(new InnerObject { Value = 321 })))
        };
        dataFile.UpdateManual(insertResult.StartDataFileOffset, insertResult.EndDataFileOffset, fieldValueCollection);
        var readFieldsResult = new FieldValueCollection();
        dataFile.ReadFields(insertResult.StartDataFileOffset, insertResult.EndDataFileOffset, _fieldNumbers, readFieldsResult);
        Assert.AreEqual(321, ((InnerObject)readFieldsResult[1].Value).Value);
    }

    [Test]
    public void InsertNullString()
    {
        var dataFile = new DataFile("", _fieldMetaCollection, _fileSystem, _memory);
        dataFile.BeginReadWrite();
        var fieldValueCollection = new FieldValue[]
        {
            new FieldValue(15, null)
        };
        var insertResult = dataFile.Insert(fieldValueCollection);
        var readFieldsResult = new FieldValueCollection();
        dataFile.ReadFields(insertResult.StartDataFileOffset, insertResult.EndDataFileOffset, _fieldNumbers, readFieldsResult);
        Assert.AreEqual(null, readFieldsResult[15].Value);
    }

    [Test]
    public void InsertNullInnerObject()
    {
        var fieldMetaCollection = new FieldMeta[]
        {
            new FieldMeta(1, "", typeof(InnerObject)),
        };
        var fieldValueCollection = new FieldValue[]
        {
            new FieldValue(1, null)
        };
        var dataFile = new DataFile("", fieldMetaCollection, _fileSystem, _memory);
        dataFile.BeginReadWrite();
        var insertResult = dataFile.Insert(fieldValueCollection);
        var readFieldsResult = new FieldValueCollection();
        dataFile.ReadFields(insertResult.StartDataFileOffset, insertResult.EndDataFileOffset, _fieldNumbers, readFieldsResult);
        Assert.AreEqual(null, readFieldsResult[1].Value);
    }

    [Test]
    public void InsertObjectWithoutType()
    {
        var fieldMetaCollection = new FieldMeta[]
        {
            new FieldMeta(1, "", null)
        };
        var fieldValueCollection = new FieldValue[]
        {
            new FieldValue(1, new InnerObject { Value = 123 })
        };
        var dataFile = new DataFile("", fieldMetaCollection, _fileSystem, _memory);
        dataFile.BeginReadWrite();
        var insertResult = dataFile.Insert(fieldValueCollection);
        var readFieldsResult = new FieldValueCollection();
        dataFile.ReadFields(insertResult.StartDataFileOffset, insertResult.EndDataFileOffset, _fieldNumbers, readFieldsResult);
        Assert.AreEqual(typeof(ObjectContainer), readFieldsResult[1].Value.GetType());
        Assert.AreEqual("{\"Value\":123}", readFieldsResult[1].Value.ToString());
    }

    [Test]
    public void SkipString()
    {
        var fieldMetaCollection = new FieldMeta[]
        {
            new FieldMeta(1, "", typeof(string)),
            new FieldMeta(2, "", typeof(int)),
        };
        var fieldValueCollection = new FieldValue[]
        {
            new FieldValue(1, "йцукенгшщзхъфывапролджэячсмитьбю 1234567890"),
            new FieldValue(2, 10),
        };
        var dataFile = new DataFile("", fieldMetaCollection, _fileSystem, _memory);
        dataFile.BeginRead();
        var insertResult = dataFile.Insert(fieldValueCollection);
        var fieldNumbers = new HashSet<byte> { 2 };
        var readFieldsResult = new FieldValueCollection();
        dataFile.ReadFields(insertResult.StartDataFileOffset, insertResult.EndDataFileOffset, fieldNumbers, readFieldsResult);
        Assert.AreEqual(10, readFieldsResult[2].Value);
    }

    [Test]
    public void SkipEmptyString()
    {
        var fieldMetaCollection = new FieldMeta[]
        {
            new FieldMeta(1, "", typeof(string)),
            new FieldMeta(2, "", typeof(int)),
        };
        var fieldValueCollection = new FieldValue[]
        {
            new FieldValue(1, ""),
            new FieldValue(2, 10),
        };
        var dataFile = new DataFile("", fieldMetaCollection, _fileSystem, _memory);
        dataFile.BeginRead();
        var insertResult = dataFile.Insert(fieldValueCollection);
        var fieldNumbers = new HashSet<byte> { 2 };
        var readFieldsResult = new FieldValueCollection();
        dataFile.ReadFields(insertResult.StartDataFileOffset, insertResult.EndDataFileOffset, fieldNumbers, readFieldsResult);
        Assert.AreEqual(10, readFieldsResult[2].Value);
    }

    [Test]
    public void SkipNullString()
    {
        var fieldMetaCollection = new FieldMeta[]
        {
            new FieldMeta(1, "", typeof(string)),
            new FieldMeta(2, "", typeof(int)),
        };
        var fieldValueCollection = new FieldValue[]
        {
            new FieldValue(1, null),
            new FieldValue(2, 10),
        };
        var dataFile = new DataFile("", fieldMetaCollection, _fileSystem, _memory);
        dataFile.BeginRead();
        var insertResult = dataFile.Insert(fieldValueCollection);
        var fieldNumbers = new HashSet<byte> { 2 };
        var readFieldsResult = new FieldValueCollection();
        dataFile.ReadFields(insertResult.StartDataFileOffset, insertResult.EndDataFileOffset, fieldNumbers, readFieldsResult);
        Assert.AreEqual(10, readFieldsResult[2].Value);
    }

    [Test]
    public void ReadFieldsLength()
    {
        var fieldValueCollection = new FieldValue[]
        {
            new FieldValue(1, true),
            new FieldValue(2, (sbyte)10),
            new FieldValue(3, (byte)20),
            new FieldValue(4, 'b'),
            new FieldValue(5, (short)40),
            new FieldValue(6, (ushort)50),
            new FieldValue(7, 60),
            new FieldValue(8, (uint)70),
            new FieldValue(9, (long)80),
            new FieldValue(10, (ulong)90),
            new FieldValue(11, (float)10.2),
            new FieldValue(12, 30.4),
            new FieldValue(13, (decimal)50.6),
            new FieldValue(14, DateTime.Parse("2000-01-10")),
            new FieldValue(15, "0987654321098765432109876543210987654321"),
        };
        var dataFile = new DataFile("", _fieldMetaCollection, _fileSystem, _memory);
        dataFile.BeginReadWrite();
        var insertResult = dataFile.Insert(fieldValueCollection);
        var result = new Dictionary<byte, int>();
        dataFile.ReadFieldsLength(insertResult.StartDataFileOffset, insertResult.EndDataFileOffset, _fieldNumbers, result);
        Assert.AreEqual(1, result[1]);
        Assert.AreEqual(1, result[2]);
        Assert.AreEqual(1, result[3]);
        Assert.AreEqual(2, result[4]);
        Assert.AreEqual(2, result[5]);
        Assert.AreEqual(2, result[6]);
        Assert.AreEqual(4, result[7]);
        Assert.AreEqual(4, result[8]);
        Assert.AreEqual(8, result[9]);
        Assert.AreEqual(8, result[10]);
        Assert.AreEqual(4, result[11]);
        Assert.AreEqual(8, result[12]);
        Assert.AreEqual(16, result[13]);
        Assert.AreEqual(8, result[14]);
        Assert.AreEqual(44, result[15]);
    }

    [Test]
    public void ReadFieldsLength_Skip()
    {
        var fieldValueCollection = new FieldValue[]
        {
            new FieldValue(1, true),
            new FieldValue(2, (sbyte)10),
            new FieldValue(3, (byte)20),
            new FieldValue(4, 'b'),
            new FieldValue(5, (short)40),
            new FieldValue(6, (ushort)50),
            new FieldValue(7, 60),
            new FieldValue(8, (uint)70),
            new FieldValue(9, (long)80),
            new FieldValue(10, (ulong)90),
            new FieldValue(11, (float)10.2),
            new FieldValue(12, 30.4),
            new FieldValue(13, (decimal)50.6),
            new FieldValue(14, DateTime.Parse("2000-01-10")),
            new FieldValue(15, "0987654321098765432109876543210987654321"),
        };
        var dataFile = new DataFile("", _fieldMetaCollection, _fileSystem, _memory);
        dataFile.BeginReadWrite();
        var insertResult = dataFile.Insert(fieldValueCollection);
        var result = new Dictionary<byte, int>();
        dataFile.ReadFieldsLength(insertResult.StartDataFileOffset, insertResult.EndDataFileOffset, new HashSet<byte> { 1, 3, 5, 7, 9, 11, 13, 15 }, result);
        Assert.AreEqual(1, result[1]);
        Assert.AreEqual(1, result[3]);
        Assert.AreEqual(2, result[5]);
        Assert.AreEqual(4, result[7]);
        Assert.AreEqual(8, result[9]);
        Assert.AreEqual(4, result[11]);
        Assert.AreEqual(16, result[13]);
        Assert.AreEqual(44, result[15]);
    }

    [Test]
    public void Compress_String()
    {
        var fieldMetaCollection = new FieldMeta[]
        {
            new FieldMeta(1, "", typeof(string)) { Settings = new FieldSettings { Compressed = true } }
        };
        var fieldValueCollection = new FieldValue[]
        {
            new FieldValue(1, new string('*', 100)),
        };
        var dataFile = new DataFile("", fieldMetaCollection, _fileSystem, _memory);
        dataFile.BeginReadWrite();
        var insertResult = dataFile.Insert(fieldValueCollection);
        var readFieldsResult = new FieldValueCollection();
        dataFile.ReadFields(insertResult.StartDataFileOffset, insertResult.EndDataFileOffset, new HashSet<byte> { 1 }, readFieldsResult);
        Assert.AreEqual(new string('*', 100), readFieldsResult[1].Value);
    }

    [Test]
    public void Compress_Object()
    {
        var fieldMetaCollection = new FieldMeta[]
        {
            new FieldMeta(1, "", typeof(InnerObject)) { Settings = new FieldSettings { Compressed = true } }
        };
        var fieldValueCollection = new FieldValue[]
        {
            new FieldValue(1, new InnerObject { Value = 123 }),
        };
        var dataFile = new DataFile("", fieldMetaCollection, _fileSystem, _memory);
        dataFile.BeginReadWrite();
        var insertResult = dataFile.Insert(fieldValueCollection);
        var readFieldsResult = new FieldValueCollection();
        dataFile.ReadFields(insertResult.StartDataFileOffset, insertResult.EndDataFileOffset, new HashSet<byte> { 1 }, readFieldsResult);
        var result = (InnerObject)readFieldsResult[1].Value;
        Assert.AreEqual(123, result.Value);
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
        public DateTime DateTime { get; set; }
        public string String { get; set; }
    }

    class InnerObject
    {
        public int Value { get; set; }
    }
}
