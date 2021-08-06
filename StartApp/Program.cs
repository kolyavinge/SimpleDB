using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using SimpleDB;

namespace StartApp
{
    class Program
    {
        static void Main(string[] args)
        {
            var doInsert = 0;
            var doGet = 1;
            var doUpdate = 1;
            var doDelete = 1;
            var doQuery = 1;
            var doGetAsync = 0;

            var workingDirectory = @"D:\Projects\SimpleDB\StartApp\bin\Debug\netcoreapp3.1\Database";
            if (doInsert == 1)
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
                .Field(3, x => x.BirthDay)
                .Field(4, x => x.AdditionalInfo)
                .MakeFunction(() => new Person())
                .PrimaryKeySetFunction((primaryKeyValue, entity) => entity.Id = (int)primaryKeyValue)
                .FieldSetFunction((fieldNumber, fieldValue, entity) =>
                {
                    if (fieldNumber == 0) entity.Name = (string)fieldValue;
                    else if (fieldNumber == 1) entity.Surname = (string)fieldValue;
                    else if (fieldNumber == 2) entity.Middlename = (string)fieldValue;
                    else if (fieldNumber == 3) entity.BirthDay = (DateTime)fieldValue;
                    else if (fieldNumber == 4) entity.AdditionalInfo = (PersonAdditionalInfo)fieldValue;
                });

            var engine = builder.BuildEngine();
            var collection = engine.GetCollection<Person>();

            var count = 100000;
            System.Diagnostics.Stopwatch sw = null;

            if (doInsert == 1)
            {
                // insert
                Console.WriteLine("========== Insert ==========");
                var personList = Enumerable.Range(0, count).Select(i => new Person
                {
                    Id = i,
                    Name = "Name " + i,
                    Surname = "Surname " + i,
                    Middlename = "Middlename " + i,
                    BirthDay = DateTime.Today.AddYears(-10).AddDays(i),
                    AdditionalInfo = new PersonAdditionalInfo { Value = i }
                }).ToList();
                sw = System.Diagnostics.Stopwatch.StartNew();
                collection.Insert(personList);
                sw.Stop();
                Console.WriteLine(sw.Elapsed);
            }

            if (doGet == 1)
            {
                // get
                Console.WriteLine("========== Get ==========");
                var personIdList = Enumerable.Range(0, count).Cast<object>().ToList();
                sw = System.Diagnostics.Stopwatch.StartNew();
                collection.Get(personIdList).ToList();
                sw.Stop();
                Console.WriteLine(sw.Elapsed);
                var result = collection.Get(new object[] { 0, 1, count - 2, count - 1 }).ToList();
                Console.WriteLine(result[0]);
                Console.WriteLine(result[1]);
                Console.WriteLine(result[2]);
                Console.WriteLine(result[3]);
            }

            if (doUpdate == 1)
            {
                // update
                Console.WriteLine("========== Update ==========");
                var personUpdateList = Enumerable.Range(0, count).Select(i => new Person
                {
                    Id = i,
                    Name = "Новое имя " + i,
                    Surname = "Новая фамилия " + i,
                    Middlename = "Новое отчество " + i,
                    BirthDay = DateTime.Today.AddDays(i),
                    AdditionalInfo = new PersonAdditionalInfo { Value = -i }
                }).ToList();
                sw = System.Diagnostics.Stopwatch.StartNew();
                collection.Update(personUpdateList);
                sw.Stop();
                Console.WriteLine(sw.Elapsed);
            }

            if (doGet == 1)
            {
                var result = collection.Get(new object[] { 0, 1, count - 2, count - 1 }).ToList();
                Console.WriteLine(result[0]);
                Console.WriteLine(result[1]);
                Console.WriteLine(result[2]);
                Console.WriteLine(result[3]);
            }

            if (doDelete == 1)
            {
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
            }

            if (doQuery == 1)
            {
                // linq query
                Console.WriteLine("========== Linq query ==========");

                sw = System.Diagnostics.Stopwatch.StartNew();
                var queryResult = collection.Query()
                    .Select(x => new { x.Id, x.Name, x.BirthDay, x.AdditionalInfo })
                    .Where(x => x.Name.Contains("Name"))
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
                    .Where(x => x.Name.Contains("1111"))
                    .OrderBy(x => x.Id, SortDirection.Desc)
                    .ToList();
                foreach (var item in queryResult) Console.WriteLine(item);
                Console.WriteLine("- - - - - - - - -");

                var queryResultCount = collection.Query()
                    .Select()
                    .Where(x => x.Surname == "Новая фамилия 10")
                    .Count();
                Console.WriteLine("count: " + queryResultCount);
                Console.WriteLine("- - - - - - - - -");

                sw.Stop();
                Console.WriteLine(sw.Elapsed);
            }

            if (doGetAsync == 1)
            {
                Parallel.For(0, 10, i => collection.Get(i));
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

        public DateTime BirthDay { get; set; }

        public PersonAdditionalInfo AdditionalInfo { get; set; }

        public override string ToString()
        {
            return String.Format("{0}:\t{1}\t{2}\t{3}\t{4:yyyy-MM-dd}\t{5}", Id, Name, Surname, Middlename, BirthDay, AdditionalInfo != null ? AdditionalInfo.Value.ToString() : "null");
        }
    }

    public class PersonAdditionalInfo
    {
        public int Value { get; set; }
    }
}
