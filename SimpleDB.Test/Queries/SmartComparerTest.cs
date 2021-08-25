using NUnit.Framework;
using SimpleDB.Queries;

namespace SimpleDB.Test.Queries
{
    class SmartComparerTest
    {
        [Test]
        public void AreEquals()
        {
            var xarray = new object[]
            {
                (byte)2,
                (sbyte)2,
                (short)2,
                (ushort)2,
                (int)2,
                (uint)2,
                (long)2,
                (ulong)2,
                (float)2,
                (double)2,
                (decimal)2
            };

            var yarray = new object[]
            {
                (byte)2,
                (sbyte)2,
                (short)2,
                (ushort)2,
                (int)2,
                (uint)2,
                (long)2,
                (ulong)2,
                (float)2,
                (double)2,
                (decimal)2
            };

            foreach (var x in xarray)
            {
                foreach (var y in yarray)
                {
                    Assert.True(SmartComparer.AreEquals(x, y), "{0}, {1}", x.GetType(), y.GetType());
                }
            }
        }

        [Test]
        public void AreEquals_Negatives()
        {
            var xarray = new object[]
            {
                (byte)2,
                (ushort)2,
                (uint)2,
                (ulong)2,
            };

            var yarray = new object[]
            {
                (sbyte)-2,
                (short)-2,
                (int)-2,
                (long)-2,
            };

            foreach (var x in xarray)
            {
                foreach (var y in yarray)
                {
                    Assert.False(SmartComparer.AreEquals(x, y), "{0}, {1}", x.GetType(), y.GetType());
                }
            }
        }

        [Test]
        public void Compare_Less()
        {
            var xarray = new object[]
            {
                (byte)2,
                (sbyte)2,
                (short)2,
                (ushort)2,
                (int)2,
                (uint)2,
                (long)2,
                (ulong)2,
                (float)2,
                (double)2,
                (decimal)2
            };

            var yarray = new object[]
            {
                (byte)3,
                (sbyte)3,
                (short)3,
                (ushort)3,
                (int)3,
                (uint)3,
                (long)3,
                (ulong)3,
                (float)3,
                (double)3,
                (decimal)3
            };

            foreach (var x in xarray)
            {
                foreach (var y in yarray)
                {
                    Assert.True(SmartComparer.Compare(x, y) < 0, "{0}, {1}", x.GetType(), y.GetType());
                }
            }
        }

        [Test]
        public void Compare_Great()
        {
            var xarray = new object[]
            {
                (byte)3,
                (sbyte)3,
                (short)3,
                (ushort)3,
                (int)3,
                (uint)3,
                (long)3,
                (ulong)3,
                (float)3,
                (double)3,
                (decimal)3
            };

            var yarray = new object[]
            {
                (byte)2,
                (sbyte)2,
                (short)2,
                (ushort)2,
                (int)2,
                (uint)2,
                (long)2,
                (ulong)2,
                (float)2,
                (double)2,
                (decimal)2
            };

            foreach (var x in xarray)
            {
                foreach (var y in yarray)
                {
                    Assert.True(SmartComparer.Compare(x, y) > 0, "{0}, {1}", x.GetType(), y.GetType());
                }
            }
        }

        [Test]
        public void Equals_IntNull()
        {
            Assert.False(SmartComparer.AreEquals(123, null));
            Assert.False(SmartComparer.AreEquals(null, 123));
        }

        [Test]
        public void Equals_Null()
        {
            Assert.True(SmartComparer.AreEquals(null, null));
        }
    }
}
