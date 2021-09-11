using System;
using System.Collections.Generic;
using NUnit.Framework;
using SimpleDB.Core;
using SimpleDB.Test.Tools;

namespace SimpleDB.Test.Core
{
    class IndexTest
    {
        private MemoryFileStream _stream;
        private Index<int> _index;

        class TestEntity { }

        class Inner
        {
            public string Value;
        }

        [SetUp]
        public void Setup()
        {
            _stream = new MemoryFileStream();
            var meta = new IndexMeta
            {
                EntityType = typeof(TestEntity),
                Name = "test index",
                IndexedFieldNumber = 0,
                IncludedFieldNumbers = new byte[] { 1, 2 }
            };
            _index = new Index<int>(meta);
        }

        [Test]
        public void SerializeDeserialize_PrimitiveTypes()
        {
            _index.Insert(new IndexValue
            {
                IndexedFieldValue = 10,
                Items = new List<IndexItem>
                {
                    new IndexItem { PrimaryKeyValue = 1, IncludedFields = new object[] { 101, 201 } }
                }
            });
            _index.Insert(new IndexValue
            {
                IndexedFieldValue = 20,
                Items = new List<IndexItem>
                {
                    new IndexItem { PrimaryKeyValue = 2, IncludedFields = new object[] { 102, 202 } }
                }
            });
            _index.Insert(new IndexValue
            {
                IndexedFieldValue = 30,
                Items = new List<IndexItem>
                {
                    new IndexItem { PrimaryKeyValue = 3, IncludedFields = new object[] { 103, 203 } },
                    new IndexItem { PrimaryKeyValue = 4, IncludedFields = new object[] { 104, 204 } },
                    new IndexItem { PrimaryKeyValue = 5, IncludedFields = new object[] { 105, 205 } }
                }
            });

            _index.Serialize(_stream);
            _stream.Seek(0, System.IO.SeekOrigin.Begin);
            var result = Index<int>.Deserialize(_stream, typeof(int), new Dictionary<byte, Type> { { 1, typeof(int) }, { 2, typeof(int) } });

            Assert.AreEqual(typeof(TestEntity), result.Meta.EntityType);
            Assert.AreEqual("test index", result.Meta.Name);
            Assert.AreEqual(0, result.Meta.IndexedFieldNumber);
            Assert.AreEqual(2, result.Meta.IncludedFieldNumbers.Length);
            Assert.AreEqual(1, result.Meta.IncludedFieldNumbers[0]);
            Assert.AreEqual(2, result.Meta.IncludedFieldNumbers[1]);

            var indexValue = result.Get(10);
            Assert.AreEqual(10, indexValue.IndexedFieldValue);
            Assert.AreEqual(1, indexValue.Items.Count);
            Assert.AreEqual(1, indexValue.Items[0].PrimaryKeyValue);
            Assert.AreEqual(2, indexValue.Items[0].IncludedFields.Length);
            Assert.AreEqual(101, indexValue.Items[0].IncludedFields[0]);
            Assert.AreEqual(201, indexValue.Items[0].IncludedFields[1]);

            indexValue = result.Get(20);
            Assert.AreEqual(20, indexValue.IndexedFieldValue);
            Assert.AreEqual(1, indexValue.Items.Count);
            Assert.AreEqual(2, indexValue.Items[0].PrimaryKeyValue);
            Assert.AreEqual(2, indexValue.Items[0].IncludedFields.Length);
            Assert.AreEqual(102, indexValue.Items[0].IncludedFields[0]);
            Assert.AreEqual(202, indexValue.Items[0].IncludedFields[1]);

            indexValue = result.Get(30);
            Assert.AreEqual(30, indexValue.IndexedFieldValue);
            Assert.AreEqual(3, indexValue.Items.Count);
            Assert.AreEqual(3, indexValue.Items[0].PrimaryKeyValue);
            Assert.AreEqual(2, indexValue.Items[0].IncludedFields.Length);
            Assert.AreEqual(103, indexValue.Items[0].IncludedFields[0]);
            Assert.AreEqual(203, indexValue.Items[0].IncludedFields[1]);
            Assert.AreEqual(4, indexValue.Items[1].PrimaryKeyValue);
            Assert.AreEqual(2, indexValue.Items[1].IncludedFields.Length);
            Assert.AreEqual(104, indexValue.Items[1].IncludedFields[0]);
            Assert.AreEqual(204, indexValue.Items[1].IncludedFields[1]);
            Assert.AreEqual(5, indexValue.Items[2].PrimaryKeyValue);
            Assert.AreEqual(2, indexValue.Items[2].IncludedFields.Length);
            Assert.AreEqual(105, indexValue.Items[2].IncludedFields[0]);
            Assert.AreEqual(205, indexValue.Items[2].IncludedFields[1]);
        }

        [Test]
        public void SerializeDeserialize_AllTypes()
        {
            _index.Meta.IncludedFieldNumbers = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 };
            _index.Insert(new IndexValue
            {
                IndexedFieldValue = 10,
                Items = new List<IndexItem>
                {
                    new IndexItem
                    {
                        PrimaryKeyValue = 1,
                        IncludedFields = new object[]
                        {
                            false,
                            (sbyte)1,
                            (byte)2,
                            'a',
                            (short)4,
                            (ushort)5,
                            6,
                            (uint)7,
                            (long)8,
                            (ulong)9,
                            (float)1.2,
                            3.4,
                            (decimal)5.6,
                            DateTime.Parse("2000-12-31"),
                            "1234567890",
                            new Inner { Value = "123" }
                        }
                    }
                }
            });

            _index.Serialize(_stream);
            _stream.Seek(0, System.IO.SeekOrigin.Begin);
            var result = Index<int>.Deserialize(_stream, typeof(int), new Dictionary<byte, Type>
            {
                 { 1, typeof(bool) },
                 { 2, typeof(sbyte) },
                 { 3, typeof(byte) },
                 { 4, typeof(char) },
                 { 5, typeof(short) },
                 { 6, typeof(ushort) },
                 { 7, typeof(int) },
                 { 8, typeof(uint) },
                 { 9, typeof(long) },
                 { 10, typeof(ulong) },
                 { 11, typeof(float) },
                 { 12, typeof(double) },
                 { 13, typeof(decimal) },
                 { 14, typeof(DateTime) },
                 { 15, typeof(string) },
                 { 16, typeof(Inner) },
            });

            Assert.AreEqual(typeof(TestEntity), result.Meta.EntityType);
            Assert.AreEqual("test index", result.Meta.Name);
            Assert.AreEqual(0, result.Meta.IndexedFieldNumber);
            Assert.AreEqual(16, result.Meta.IncludedFieldNumbers.Length);

            var indexValue = result.Get(10);
            Assert.AreEqual(10, indexValue.IndexedFieldValue);
            Assert.AreEqual(1, indexValue.Items.Count);
            Assert.AreEqual(1, indexValue.Items[0].PrimaryKeyValue);
            Assert.AreEqual(16, indexValue.Items[0].IncludedFields.Length);
            Assert.AreEqual(false, indexValue.Items[0].IncludedFields[0]);
            Assert.AreEqual((sbyte)1, indexValue.Items[0].IncludedFields[1]);
            Assert.AreEqual((byte)2, indexValue.Items[0].IncludedFields[2]);
            Assert.AreEqual('a', indexValue.Items[0].IncludedFields[3]);
            Assert.AreEqual((short)4, indexValue.Items[0].IncludedFields[4]);
            Assert.AreEqual((ushort)5, indexValue.Items[0].IncludedFields[5]);
            Assert.AreEqual(6, indexValue.Items[0].IncludedFields[6]);
            Assert.AreEqual((uint)7, indexValue.Items[0].IncludedFields[7]);
            Assert.AreEqual((long)8, indexValue.Items[0].IncludedFields[8]);
            Assert.AreEqual((ulong)9, indexValue.Items[0].IncludedFields[9]);
            Assert.AreEqual((float)1.2, indexValue.Items[0].IncludedFields[10]);
            Assert.AreEqual(3.4, indexValue.Items[0].IncludedFields[11]);
            Assert.AreEqual((decimal)5.6, indexValue.Items[0].IncludedFields[12]);
            Assert.AreEqual(DateTime.Parse("2000-12-31"), indexValue.Items[0].IncludedFields[13]);
            Assert.AreEqual("1234567890", indexValue.Items[0].IncludedFields[14]);
            Assert.AreEqual("123", ((Inner)indexValue.Items[0].IncludedFields[15]).Value);
        }
    }
}
