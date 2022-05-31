using NUnit.Framework;
using SimpleDB.Infrastructure;

namespace SimpleDB.Test.Infrastructure;

class JsonSerializationTest
{
    [Test]
    public void JsonSerialization_NoLineBreaks()
    {
        var json = JsonSerialization.ToJson(new TestClass { IntField = 123, StringField = "123", Inner = new Inner { IntField = 987 } });
        Assert.AreEqual("{\"IntField\":123,\"StringField\":\"123\",\"Inner\":{\"IntField\":987}}", json);
    }

    class TestClass
    {
        public int IntField { get; set; }
        public string StringField { get; set; }
        public Inner Inner { get; set; }
    }

    class Inner
    {
        public int IntField { get; set; }
    }
}
