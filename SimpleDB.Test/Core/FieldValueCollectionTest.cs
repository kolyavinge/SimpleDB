using NUnit.Framework;
using SimpleDB.Core;

namespace SimpleDB.Test.Core
{
    class FieldValueCollectionTest
    {
        [Test]
        public void Equals_Empty()
        {
            var x = new FieldValueCollection();
            var y = new FieldValueCollection();
            Assert.True(x.Equals(y));
            Assert.True(y.Equals(x));
        }

        [Test]
        public void GetHashCode_Empty()
        {
            var x = new FieldValueCollection();
            var y = new FieldValueCollection();
            Assert.True(x.GetHashCode() == y.GetHashCode());
        }

        [Test]
        public void Equals_EmptyAndFull()
        {
            var x = new FieldValueCollection();
            var y = new FieldValueCollection(new[] { new FieldValue(0, 123) });
            Assert.False(x.Equals(y));
            Assert.False(y.Equals(x));
        }

        [Test]
        public void GetHashCode_EmptyAndFull()
        {
            var x = new FieldValueCollection();
            var y = new FieldValueCollection(new[] { new FieldValue(0, 123) });
            Assert.True(x.GetHashCode() != y.GetHashCode());
        }

        [Test]
        public void Equals_1Same()
        {
            var x = new FieldValueCollection(new[] { new FieldValue(0, 123) });
            var y = new FieldValueCollection(new[] { new FieldValue(0, 123) });
            Assert.True(x.Equals(y));
            Assert.True(y.Equals(x));
        }

        [Test]
        public void GetHashCode_1Same()
        {
            var x = new FieldValueCollection(new[] { new FieldValue(0, 123) });
            var y = new FieldValueCollection(new[] { new FieldValue(0, 123) });
            Assert.True(x.GetHashCode() == y.GetHashCode());
        }

        [Test]
        public void Equals_1Diff()
        {
            var x = new FieldValueCollection(new[] { new FieldValue(0, 123) });
            var y = new FieldValueCollection(new[] { new FieldValue(0, 456) });
            Assert.False(x.Equals(y));
            Assert.False(y.Equals(x));
        }

        [Test]
        public void GetHashCode_1Diff()
        {
            var x = new FieldValueCollection(new[] { new FieldValue(0, 123) });
            var y = new FieldValueCollection(new[] { new FieldValue(0, 456) });
            Assert.True(x.GetHashCode() != y.GetHashCode());
        }

        [Test]
        public void Equals_2()
        {
            var x = new FieldValueCollection(new[] { new FieldValue(0, 123), new FieldValue(1, 456) });
            var y = new FieldValueCollection(new[] { new FieldValue(0, 123), new FieldValue(1, 456) });
            Assert.True(x.Equals(y));
            Assert.True(y.Equals(x));
        }

        [Test]
        public void GetHashCode_2()
        {
            var x = new FieldValueCollection(new[] { new FieldValue(0, 123), new FieldValue(1, 456) });
            var y = new FieldValueCollection(new[] { new FieldValue(0, 123), new FieldValue(1, 456) });
            Assert.True(x.GetHashCode() == y.GetHashCode());
        }

        [Test]
        public void Equals_2Invert()
        {
            var x = new FieldValueCollection(new[] { new FieldValue(1, 456), new FieldValue(0, 123) });
            var y = new FieldValueCollection(new[] { new FieldValue(0, 123), new FieldValue(1, 456) });
            Assert.True(x.Equals(y));
            Assert.True(y.Equals(x));
        }

        [Test]
        public void GetHashCode_2Invert()
        {
            var x = new FieldValueCollection(new[] { new FieldValue(1, 456), new FieldValue(0, 123) });
            var y = new FieldValueCollection(new[] { new FieldValue(0, 123), new FieldValue(1, 456) });
            Assert.True(x.GetHashCode() == y.GetHashCode());
        }

        [Test]
        public void Equals_Add()
        {
            var x = new FieldValueCollection(new[] { new FieldValue(0, 123) });
            var y = new FieldValueCollection(new[] { new FieldValue(0, 123), new FieldValue(1, 456) });
            Assert.False(x.Equals(y));
            Assert.False(y.Equals(x));
            x.Add(1, new FieldValue(1, 456));
            Assert.True(x.Equals(y));
            Assert.True(y.Equals(x));
        }
    }
}
