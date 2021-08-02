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
            var modeCreate = false;

            if (modeCreate)
            {
                foreach (var file in Directory.GetFiles(workingDirectory)) File.Delete(file);
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
            var collection = engine.GetCollection<Person>();

            var count = 10000;
            System.Diagnostics.Stopwatch sw = null;

            if (modeCreate)
            {
                // insert
                Console.WriteLine("========== Insert ==========");
                sw = System.Diagnostics.Stopwatch.StartNew();
                for (int i = 0; i < count; i++)
                {
                    collection.Insert(new Person { Id = i, Name = "Name " + i, Surname = "Surname " + i, Middlename = "Middlename " + i, AdditionalInfo = new PersonAdditionalInfo { Value = i } });
                }
                sw.Stop();
                Console.WriteLine(sw.Elapsed);
            }

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
                if (collection.Exist(i))
                {
                    collection.Update(new Person { Id = i, Name = "New Name " + i, Surname = "New Surname " + i, Middlename = "New Middlename " + i, AdditionalInfo = new PersonAdditionalInfo { Value = -i } });
                }
                else
                {
                    Console.WriteLine(String.Format("Id {0} not exists", i));
                }
            }
            sw.Stop();
            Console.WriteLine(sw.Elapsed);

            Console.WriteLine(collection.Get(0));
            Console.WriteLine(collection.Get(1));
            Console.WriteLine(collection.Get(count - 2));
            Console.WriteLine(collection.Get(count - 1));

            // delete
            Console.WriteLine("========== Delete ==========");
            if (collection.Exist(0))
            {
                collection.Delete(0);
            }
            else
            {
                Console.WriteLine(String.Format("Id {0} not exists", 0));
            }
            Console.WriteLine(collection.Get(0) == null ? "collection.Get(0): null" : "collection.Get(0): !!! not null !!!");
            Console.WriteLine("collection.Get(1): " + collection.Get(1));

            // linq query
            Console.WriteLine("========== Linq query ==========");

            var queryResult = collection.Query()
                .Select(x => new { x.Id, x.Name, x.AdditionalInfo })
                .Where(x => x.Name == "New Name 10" && x.Surname == "New Surname 10")
                .ToList();
            foreach (var item in queryResult) Console.WriteLine(item);
            Console.WriteLine("- - - - - - - - -");

            queryResult = collection.Query()
                .Select(x => new { x.Id, x.Name })
                .Skip(10)
                .Limit(10)
                .ToList();
            foreach (var item in queryResult) Console.WriteLine(item);
            Console.WriteLine("- - - - - - - - -");

            queryResult = collection.Query()
                .Select(x => new { x.Id, x.Name })
                .Where(x => x.Name.Contains("111"))
                .OrderBy(x => x.Id, SortDirection.Desc)
                .ToList();
            foreach (var item in queryResult) Console.WriteLine(item);
            Console.WriteLine("- - - - - - - - -");

            engine.Dispose();

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
            return String.Format("{0}:\t{1}\t{2}\t{3}\t{4}", Id, Name, Surname, Middlename, AdditionalInfo != null ? AdditionalInfo.Value.ToString() : "null");
        }
    }

    public class PersonAdditionalInfo
    {
        public int Value { get; set; }
    }
}
