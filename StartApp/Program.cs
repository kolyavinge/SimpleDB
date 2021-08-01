using System;
using System.IO;
using SimpleDB;

namespace StartApp
{
    class Program
    {
        static void Main(string[] args)
        {
            var workingDirectory = @"D:\Projects\SimpleDB\StartApp\bin\Debug\netcoreapp3.1\Database";
            foreach (var file in Directory.GetFiles(workingDirectory))
            {
                File.Delete(file);
            }

            // build engine

            var builder = DBEngineBuilder.Make();
            builder.WorkingDirectory(workingDirectory);
            builder.Map<Person>()
                .Name("person")
                .PrimaryKey(x => x.Id)
                .Field(0, x => x.Name)
                .Field(1, x => x.Surname)
                .Field(2, x => x.Middlename)
                .Field(3, x => x.AdditionalInfo);

            var engine = builder.BuildEngine();

            // insert

            Console.WriteLine("========== Insert ==========");
            var collection = engine.GetCollection<Person>();
            var sw = System.Diagnostics.Stopwatch.StartNew();
            var count = 10000;
            for (int i = 0; i < count; i++)
            {
                collection.Insert(new Person { Id = i, Name = "Name " + i, Surname = "Surname " + i, Middlename = "Middlename " + i, AdditionalInfo = new PersonAdditionalInfo { Value = i } });
            }
            sw.Stop();
            Console.WriteLine(sw.Elapsed);

            // get

            Console.WriteLine("========== Get ==========");
            sw = System.Diagnostics.Stopwatch.StartNew();
            for (int i = 0; i < count; i++)
            {
                var person = collection.Get(i);
            }
            sw.Stop();
            Console.WriteLine(sw.Elapsed);

            Console.WriteLine(collection.Get(0));
            Console.WriteLine(collection.Get(1));
            Console.WriteLine(collection.Get(count - 2));
            Console.WriteLine(collection.Get(count - 1));

            // update

            Console.WriteLine("========== Update ==========");
            sw = System.Diagnostics.Stopwatch.StartNew();
            for (int i = 0; i < count; i++)
            {
                collection.Update(new Person { Id = i, Name = "New Name " + i, Surname = "New Surname " + i, Middlename = "New Middlename " + i, AdditionalInfo = new PersonAdditionalInfo { Value = -i } });
            }
            sw.Stop();
            Console.WriteLine(sw.Elapsed);

            Console.WriteLine(collection.Get(0));
            Console.WriteLine(collection.Get(1));
            Console.WriteLine(collection.Get(count - 2));
            Console.WriteLine(collection.Get(count - 1));

            // delete

            Console.WriteLine("========== Delete ==========");
            collection.Delete(0);
            Console.WriteLine(collection.Get(0) == null ? "collection.Get(0): null" : "collection.Get(0): !!! not null !!!");
            Console.WriteLine("collection.Get(1): " + collection.Get(1));

            // linq query

            Console.WriteLine("========== Linq query ==========");

            var queryResult = collection.Query()
                .Select(x => new { x.Id, x.Name, x.AdditionalInfo })
                .Where(x => x.Name == "New Name 10" && x.Surname == "New Surname 10")
                .OrderBy(x => x.Name, SortDirection.Asc)
                .OrderBy(x => x.Surname, SortDirection.Desc)
                .Skip(1)
                .Limit(10)
                .ToList();

            foreach (var item in queryResult)
            {
                Console.WriteLine(item);
            }

            Console.ReadKey();
        }
    }

    class Person
    {
        public int Id { get; set; }

        public string Name { get; set; }

        public string Surname { get; set; }

        public string Middlename { get; set; }

        public PersonAdditionalInfo AdditionalInfo { get; set; }

        public override string ToString()
        {
            return $"{Id}: {Name} {Surname} {Middlename} {AdditionalInfo.Value}";
        }
    }

    public class PersonAdditionalInfo
    {
        public int Value { get; set; }
    }
}
