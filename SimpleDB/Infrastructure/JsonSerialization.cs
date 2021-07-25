using System;
using Newtonsoft.Json;

namespace SimpleDB.Infrastructure
{
    internal class JsonSerialization
    {
        public static string ToJson(object obj)
        {
            return JsonConvert.SerializeObject(obj);
        }

        public static object FromJson(Type type, string json)
        {
            return JsonConvert.DeserializeObject(json, type);
        }
    }
}
