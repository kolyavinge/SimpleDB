using System;
using System.Collections.Generic;
using System.Linq;
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
                IndexedFieldType = typeof(int),
                Name = "test index",
                IndexedFieldNumber = 0,
                IncludedFieldNumbers = new byte[] { 1, 2 }
            };
            _index = new Index<int>(meta);
            for (int i = 0; i < 100; i++)
            {
                _index.Insert(new IndexValue { IndexedFieldValue = i });
            }
        }

        [Test]
        public void SerializeDeserialize_PrimitiveTypes()
        {
            _index.Clear();
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
            Assert.AreEqual(typeof(int), result.Meta.IndexedFieldType);
            Assert.AreEqual("test index", result.Meta.Name);
            Assert.AreEqual(0, result.Meta.IndexedFieldNumber);
            Assert.AreEqual(2, result.Meta.IncludedFieldNumbers.Length);
            Assert.AreEqual(1, result.Meta.IncludedFieldNumbers[0]);
            Assert.AreEqual(2, result.Meta.IncludedFieldNumbers[1]);

            var indexValue = result.GetEquals(10);
            Assert.AreEqual(10, indexValue.IndexedFieldValue);
            Assert.AreEqual(1, indexValue.Items.Count);
            Assert.AreEqual(1, indexValue.Items[0].PrimaryKeyValue);
            Assert.AreEqual(2, indexValue.Items[0].IncludedFields.Length);
            Assert.AreEqual(101, indexValue.Items[0].IncludedFields[0]);
            Assert.AreEqual(201, indexValue.Items[0].IncludedFields[1]);

            indexValue = result.GetEquals(20);
            Assert.AreEqual(20, indexValue.IndexedFieldValue);
            Assert.AreEqual(1, indexValue.Items.Count);
            Assert.AreEqual(2, indexValue.Items[0].PrimaryKeyValue);
            Assert.AreEqual(2, indexValue.Items[0].IncludedFields.Length);
            Assert.AreEqual(102, indexValue.Items[0].IncludedFields[0]);
            Assert.AreEqual(202, indexValue.Items[0].IncludedFields[1]);

            indexValue = result.GetEquals(30);
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
            _index.Clear();
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
            Assert.AreEqual(typeof(int), result.Meta.IndexedFieldType);
            Assert.AreEqual("test index", result.Meta.Name);
            Assert.AreEqual(0, result.Meta.IndexedFieldNumber);
            Assert.AreEqual(16, result.Meta.IncludedFieldNumbers.Length);

            var indexValue = result.GetEquals(10);
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

        [Test]
        public void GetEquals()
        {
            var result = _index.GetEquals(55);
            Assert.AreEqual(55, result.IndexedFieldValue);
        }

        [Test]
        public void GetNotEquals()
        {
            var result = _index.GetNotEquals(55).Select(x => x.IndexedFieldValue).ToHashSet();
            Assert.AreEqual(99, result.Count);
            foreach (var x in Enumerable.Range(0, 55).Union(Enumerable.Range(56, 43)))
            {
                Assert.IsTrue(result.Contains(x));
            }
        }

        [Test]
        public void GetLess()
        {
            var result = _index.GetLess(55).Select(x => x.IndexedFieldValue).ToHashSet();
            Assert.AreEqual(55, result.Count);
            foreach (var x in Enumerable.Range(0, 55))
            {
                Assert.IsTrue(result.Contains(x));
            }
        }

        [Test]
        public void GetGreat()
        {
            var result = _index.GetGreat(55).Select(x => x.IndexedFieldValue).ToHashSet();
            Assert.AreEqual(44, result.Count);
            foreach (var x in Enumerable.Range(56, 43))
            {
                Assert.IsTrue(result.Contains(x));
            }
        }

        [Test]
        public void GetLessOrEquals()
        {
            var result = _index.GetLessOrEquals(55).Select(x => x.IndexedFieldValue).ToHashSet();
            Assert.AreEqual(56, result.Count);
            foreach (var x in Enumerable.Range(0, 56))
            {
                Assert.IsTrue(result.Contains(x));
            }
        }

        [Test]
        public void GetGreatOrEquals()
        {
            var result = _index.GetGreatOrEquals(55).Select(x => x.IndexedFieldValue).ToHashSet();
            Assert.AreEqual(45, result.Count);
            foreach (var x in Enumerable.Range(55, 43))
            {
                Assert.IsTrue(result.Contains(x));
            }
        }

        [Test]
        public void GetLike()
        {
            var result = _index.GetLike("5").Select(x => x.IndexedFieldValue).ToHashSet();
            Assert.AreEqual(19, result.Count);
            Assert.IsTrue(result.Contains(5));
            Assert.IsTrue(result.Contains(15));
            Assert.IsTrue(result.Contains(25));
            Assert.IsTrue(result.Contains(35));
            Assert.IsTrue(result.Contains(45));
            Assert.IsTrue(result.Contains(50));
            Assert.IsTrue(result.Contains(51));
            Assert.IsTrue(result.Contains(52));
            Assert.IsTrue(result.Contains(53));
            Assert.IsTrue(result.Contains(54));
            Assert.IsTrue(result.Contains(55));
            Assert.IsTrue(result.Contains(56));
            Assert.IsTrue(result.Contains(57));
            Assert.IsTrue(result.Contains(58));
            Assert.IsTrue(result.Contains(59));
            Assert.IsTrue(result.Contains(65));
            Assert.IsTrue(result.Contains(75));
            Assert.IsTrue(result.Contains(85));
            Assert.IsTrue(result.Contains(95));
        }

        [Test]
        public void GetNotLike()
        {
            var result = _index.GetNotLike("5").Select(x => x.IndexedFieldValue).ToHashSet();
            Assert.AreEqual(81, result.Count);
            Assert.IsTrue(!result.Contains(5));
            Assert.IsTrue(!result.Contains(15));
            Assert.IsTrue(!result.Contains(25));
            Assert.IsTrue(!result.Contains(35));
            Assert.IsTrue(!result.Contains(45));
            Assert.IsTrue(!result.Contains(50));
            Assert.IsTrue(!result.Contains(51));
            Assert.IsTrue(!result.Contains(52));
            Assert.IsTrue(!result.Contains(53));
            Assert.IsTrue(!result.Contains(54));
            Assert.IsTrue(!result.Contains(55));
            Assert.IsTrue(!result.Contains(56));
            Assert.IsTrue(!result.Contains(57));
            Assert.IsTrue(!result.Contains(58));
            Assert.IsTrue(!result.Contains(59));
            Assert.IsTrue(!result.Contains(65));
            Assert.IsTrue(!result.Contains(75));
            Assert.IsTrue(!result.Contains(85));
            Assert.IsTrue(!result.Contains(95));
        }

        [Test]
        public void GetIn()
        {
            var result = _index.GetIn(new[] { 21, 55, 68, 81 }).Select(x => x.IndexedFieldValue).ToHashSet();
            Assert.AreEqual(4, result.Count);
            Assert.IsTrue(result.Contains(21));
            Assert.IsTrue(result.Contains(55));
            Assert.IsTrue(result.Contains(68));
            Assert.IsTrue(result.Contains(81));
        }

        [Test]
        public void GetNotIn()
        {
            var result = _index.GetNotIn(new[] { 21, 55, 68, 81 }).Select(x => x.IndexedFieldValue).ToHashSet();
            Assert.AreEqual(96, result.Count);
            Assert.IsTrue(!result.Contains(21));
            Assert.IsTrue(!result.Contains(55));
            Assert.IsTrue(!result.Contains(68));
            Assert.IsTrue(!result.Contains(81));
        }
    }
}
