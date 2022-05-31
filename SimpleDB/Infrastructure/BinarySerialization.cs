using System.IO;
using System.Runtime.Serialization.Formatters.Binary;

namespace SimpleDB.Infrastructure;

internal class BinarySerialization
{
    public static byte[] ToBinary(object obj)
    {
        var memoryStream = new MemoryStream();
        var formatter = new BinaryFormatter();
        formatter.Serialize(memoryStream, obj);

        return memoryStream.ToArray();
    }

    public static T FromBinary<T>(byte[] bytes)
    {
        var memoryStream = new MemoryStream(bytes);
        var formatter = new BinaryFormatter();
        var obj = formatter.Deserialize(memoryStream);

        return (T)obj;
    }
}
