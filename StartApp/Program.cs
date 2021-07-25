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
            var builder = DBEngineBuilder.Make();
            builder.WorkingDirectory(workingDirectory);
            builder.Map<Person>()
                .Name("person")
                .PrimaryKey(x => x.Id)
                .Field(0, x => x.Name)
                .Field(1, x => x.Surname)
                .Field(2, x => x.Middlename);

            var engine = builder.BuildEngine();

            var collection = engine.GetCollection<Person>();
            var sw = System.Diagnostics.Stopwatch.StartNew();
            var count = 10000;
            for (int i = 0; i < count; i++)
            {
                collection.Insert(new Person
                {
                    Id = i,
                    Name = "Name " + i,
                    Surname = "Surname" + i,
                    Middlename = "Middlename " + i
                });
            }
            sw.Stop();
            Console.WriteLine(sw.Elapsed);
            sw = System.Diagnostics.Stopwatch.StartNew();
            for (int i = 0; i < count; i++)
            {
                var person = collection.GetById(i);
            }
            sw.Stop();
            Console.WriteLine(sw.Elapsed);

            Console.WriteLine(collection.GetById(0));
            Console.WriteLine(collection.GetById(1));
            Console.WriteLine(collection.GetById(count - 2));
            Console.WriteLine(collection.GetById(count - 1));

            Console.ReadKey();
        }
    }

    class Person
    {
        public int Id { get; set; }

        public string Name { get; set; }

        public string Surname { get; set; }

        public string Middlename { get; set; }

        public override string ToString()
        {
            return $"{Id}: {Name} {Surname} {Middlename}";
        }
    }
}
