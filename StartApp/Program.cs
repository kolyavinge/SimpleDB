﻿using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using SimpleDB;
using SimpleDB.Maintenance;

namespace StartApp
{
    class Program
    {
        static void Main()
        {
            var doInsert = 1;
            var doGet = 1;
            var doUpdate = 1;
            var doDelete = 1;
            var doQuery = 1;
            var doMerge = 1;
            var doGetAsync = 0;
            var doStatistics = 0;
            var doDefragmentation = 0;

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
                .Field(2, x => x.Middlename, new FieldSettings { Compressed = true })
                .Field(3, x => x.BirthDay)
                .Field(4, x => x.AdditionalInfo, new FieldSettings { Compressed = true })
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

            builder.Index<Person>()
                .Name("name")
                .For(x => x.Name)
                .Include(x => x.Surname)
                .Include(x => x.Middlename);

            Console.WriteLine("========== Build engine ==========");
            var sw = System.Diagnostics.Stopwatch.StartNew();
            var engine = builder.BuildEngine();
            sw.Stop();
            Console.WriteLine(sw.Elapsed);

            var collection = engine.GetCollection<Person>();

            var count = 100000;

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
                    Bytes = new byte[] { 1, 2, 3 },
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

                Console.WriteLine("========== Get indexed ==========");
                sw = System.Diagnostics.Stopwatch.StartNew();
                result = collection.Query()
                    .Select(x => new { x.Name, x.Surname, x.Middlename })
                    .Where(x => x.Name == "Name 123")
                    .ToList();
                if (result.Any())
                {
                    Console.WriteLine(result.First().Name);
                }
                else
                {
                    Console.WriteLine("empty result");
                }
                sw.Stop();
                Console.WriteLine(sw.Elapsed);
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
                    Bytes = new byte[] { 1, 2, 3 },
                    AdditionalInfo = new PersonAdditionalInfo { Value = -i }
                }).ToList();
                sw = System.Diagnostics.Stopwatch.StartNew();
                collection.Update(personUpdateList);
                sw.Stop();
                Console.WriteLine(sw.Elapsed);
            }

            if (doGet == 1)
            {
                Console.WriteLine("========== Get ==========");
                var result = collection.Get(new object[] { 0, 1, count - 2, count - 1 }).ToList();
                Console.WriteLine(result[0]);
                Console.WriteLine(result[1]);
                Console.WriteLine(result[2]);
                Console.WriteLine(result[3]);
                Console.WriteLine("========== Get indexed updated ==========");
                sw = System.Diagnostics.Stopwatch.StartNew();
                result = collection.Query()
                    .Select(x => new { x.Name, x.Surname, x.Middlename })
                    .Where(x => x.Name == "Новое имя 123")
                    .ToList();
                if (result.Any())
                {
                    Console.WriteLine(result.First().Name);
                }
                else
                {
                    Console.WriteLine("empty result");
                }
                sw.Stop();
                Console.WriteLine(sw.Elapsed);
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
                Console.WriteLine("========== Select ==========");

                sw = System.Diagnostics.Stopwatch.StartNew();
                var selectQueryResult = collection.Query()
                    .Select(x => new { x.Id, x.Name, x.BirthDay, x.AdditionalInfo })
                    .Where(x => x.Name.Contains("Name"))
                    .ToList();
                foreach (var item in selectQueryResult.Take(10)) Console.WriteLine(item);
                Console.WriteLine("- - - - - - - - -");

                selectQueryResult = collection.Query()
                    .Skip(10)
                    .Limit(10)
                    .ToList();
                foreach (var item in selectQueryResult) Console.WriteLine(item);
                Console.WriteLine("- - - - - - - - -");

                selectQueryResult = collection.Query()
                    .Where(x => x.Name.Contains("1111"))
                    .OrderBy(x => x.Id, SortDirection.Desc)
                    .ToList();
                foreach (var item in selectQueryResult) Console.WriteLine(item);
                Console.WriteLine("- - - - - - - - -");

                selectQueryResult = collection.Query()
                    .OrderBy(x => x.Name, SortDirection.Desc)
                    .ToList();
                foreach (var item in selectQueryResult.Take(10)) Console.WriteLine(item);
                Console.WriteLine("- - - - - - - - -");

                var queryResultCount = collection.Query()
                    .Select()
                    .Where(x => x.Surname == "Новая фамилия 10")
                    .Count();
                Console.WriteLine("count: " + queryResultCount);

                queryResultCount = collection.Query().Count();
                Console.WriteLine("count: " + queryResultCount);
                Console.WriteLine("- - - - - - - - -");

                sw.Stop();
                Console.WriteLine(sw.Elapsed);

                Console.WriteLine("========== Select ==========");

                sw = System.Diagnostics.Stopwatch.StartNew();
                var updateQueryResult = collection.Query()
                    .Update(x => new Person { Name = "Linq new name" }, x => x.Id == 100 || x.Id == 101);
                Console.WriteLine(collection.Get(100));
                Console.WriteLine(collection.Get(101));
                sw.Stop();
                Console.WriteLine(sw.Elapsed);

                Console.WriteLine("========== Delete ==========");

                sw = System.Diagnostics.Stopwatch.StartNew();
                Console.WriteLine(collection.Exist(102));
                var deelteQueryResult = collection.Query().Delete(x => x.Id == 102);
                Console.WriteLine(collection.Exist(102));
                sw.Stop();
                Console.WriteLine(sw.Elapsed);
            }

            if (doMerge == 1)
            {
                Console.WriteLine("========== Merge ==========");
                var newEntities = Enumerable.Range(1, count)
                    .Select(i => new Person { Id = -i, Name = "Merged Name " + i.ToString(), Surname = "Merged Surname " + i.ToString() })
                    .ToList();
                sw = System.Diagnostics.Stopwatch.StartNew();
                var mergeResult = collection.Query().Merge(
                    x => new { x.Name, x.Surname },
                    newEntities);
                sw.Stop();
                Console.WriteLine(sw.Elapsed);
                Console.WriteLine("MergeResult: " + mergeResult.NewItems.Count);
                Console.WriteLine("Exist -1: " + collection.Exist(-1));
                Console.WriteLine("Exist -2: " + collection.Exist(-2));
                Console.WriteLine("Exist -3: " + collection.Exist(-3));
            }

            if (doGetAsync == 1)
            {
                Parallel.For(0, 10, i => collection.Get(i));
            }

            if (doStatistics == 1)
            {
                Console.WriteLine("========== Statistics ==========");
                var statistics = StatisticsFactory.MakeStatistics(workingDirectory);
                sw = System.Diagnostics.Stopwatch.StartNew();
                foreach (var stat in statistics.GetPrimaryKeyFileStatistics())
                {
                    var str = String.Format("{0}: fragment {1} of {2} ({3:F0}%)", stat.FileName, stat.FragmentationSizeInBytes, stat.TotalFileSizeInBytes, stat.FragmentationPercent);
                    Console.WriteLine(str);
                }
                foreach (var stat in statistics.GetDataFileStatistics())
                {
                    var str = String.Format("{0}: fragment {1} of {2} ({3:F0}%)", stat.FileName, stat.FragmentationSizeInBytes, stat.TotalFileSizeInBytes, stat.FragmentationPercent);
                    Console.WriteLine(str);
                }
                sw.Stop();
                Console.WriteLine(sw.Elapsed);
            }

            if (doDefragmentation == 1)
            {
                Console.WriteLine("========== Defragmentation ==========");
                sw = System.Diagnostics.Stopwatch.StartNew();
                var defragmentator = DefragmentatorFactory.MakeDefragmentator(workingDirectory);
                defragmentator.DefragmentDataFile("person.data");
                sw.Stop();
                Console.WriteLine(sw.Elapsed);
            }

            Console.WriteLine("done");
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

        public byte[] Bytes { get; set; }

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
