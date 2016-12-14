using System;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Newtonsoft.Json;
using RandomNameGeneratorLibrary;

namespace WestWorld
{
    public class Program
    {
        private static IDocumentClient _client;
        private const int HostNumber = 100000;
        private const int ConcurrentCount = 100;

        public static void Main(string[] args)
        {
            const string endPoint = "https://westworld.documents.azure.com:443/";
            const string key = "1YTKAuKuZOx4I8tkdJGmLNBRnr3kBu3PYQuqQw24HsZnu5rbUnJL33LKW39NNAMGOCLwWrVtR9tVCb44w4slDw==";
            _client = new DocumentClient(new Uri(endPoint), key);

            const string databaseName = "WestWorld";
            string collectionName = "HostList";
            if (args.Length > 0)
            {
                collectionName = args[0];
            }
            CreateDatabaseIfNotExists(databaseName).Wait();
            CreateDocumentCollectionIfNotExists(databaseName, collectionName).Wait();
            

            string operationName = null;
            if (args.Length > 1)
            {
                operationName = args[1];
            }

            if (string.IsNullOrEmpty(operationName) ||
                string.Equals("Create", operationName, StringComparison.CurrentCultureIgnoreCase))
            {
                CreateHostList(databaseName, collectionName);
            }
            else if (string.Equals("Ping", operationName, StringComparison.CurrentCultureIgnoreCase))
            {
                GetRandomHostFromHostList(databaseName, collectionName);
            }
            else if (string.Equals("Pressure", operationName, StringComparison.CurrentCultureIgnoreCase))
            {
                PressureTest(databaseName, collectionName);
            }
        }

        private static void PressureTest(string databaseName, string collectionName)
        {
            const int concurrentNumStart = 10;
            const int concurrentNumEnd = 100;
            const int concurrentStep = 10;
            const int stepDurationMinute = 5;

            for (int i = concurrentNumStart; i <= concurrentNumEnd; i += concurrentStep)
            {
                var autoEvent = new AutoResetEvent(false);
                var concurrentNum = i;
                using (new Timer(state =>
                {
                    Console.WriteLine($"Hello {concurrentNum} {System.DateTime.Now}");
                }, autoEvent, 0, Timeout.Infinite))
                {
                    autoEvent.WaitOne(stepDurationMinute * 60 * 1000);
                }
            }

            Console.ReadKey();
        }

        private static void GetRandomHostFromHostList(string databaseName, string collectionName)
        {
            Random seed = new Random();
            long index = 0;

            while (true)
            {
                int hostId;
                long timeElpased;
                hostId = seed.Next(0, HostNumber);
                Console.Write($"{index}");
                timeElpased = GetTimeElapsedOfGetHost(databaseName, collectionName, hostId.ToString()).Result;
                Console.Write($", {DateTime.Now:h:mm:ss tt}");
                Console.WriteLine($", {timeElpased}");
                index++;
                System.Threading.Thread.Sleep(1000);
            }
        }

        private async static Task<long> GetTimeElapsedOfGetHost(string databaseName, string collectionName, string hostId)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();
            Document doc = await _client.ReadDocumentAsync(UriFactory.CreateDocumentUri(databaseName, collectionName, hostId));
            Console.Write($", {hostId}, {doc.GetPropertyValue<string>("Name")}");
            sw.Stop();
            return sw.ElapsedMilliseconds;
        }

        private static void CreateHostList(string databaseName, string collectionName)
        {
            Task[] tasks = new Task[ConcurrentCount];
            var personGenerator = new PersonNameGenerator();
            for (int i = 0; i < HostNumber; i++)
            {
                if (tasks.All(t => t != null && !t.IsCompleted && !t.IsFaulted))
                {
                    Task.WhenAny(tasks).Wait();
                }
                for (int j = 0; j < ConcurrentCount; j++)
                {
                    if (tasks[j] == null || tasks[j].IsCompleted || tasks[j].IsFaulted)
                    {
                        tasks[j] = CreateHostDocumentIfNotExists(databaseName, collectionName, new Host
                        {
                            Id = i.ToString(),
                            Name = personGenerator.GenerateRandomFirstName()
                        });
                        break;
                    }
                }
            }
            Task.WhenAll(tasks.Where(t => t != null)).Wait();
        }

        private static void WriteToConsoleAndPromptToContinue(string format, params object[] args)
        {
            Console.WriteLine(format, args);
            //Console.WriteLine("Press any key to continue ...");
            //Console.ReadKey();
        }

        private static async Task CreateDatabaseIfNotExists(string databaseName)
        {
            // Check to verify a database with the id=FamilyDB does not exist
            try
            {
                await _client.ReadDatabaseAsync(UriFactory.CreateDatabaseUri(databaseName));
                WriteToConsoleAndPromptToContinue("Found {0}", databaseName);
            }
            catch (DocumentClientException de)
            {
                // If the database does not exist, create a new database
                if (de.StatusCode == HttpStatusCode.NotFound)
                {
                    await _client.CreateDatabaseAsync(new Database { Id = databaseName });
                    WriteToConsoleAndPromptToContinue("Created {0}", databaseName);
                }
                else
                {
                    throw;
                }
            }
        }

        private static async Task CreateDocumentCollectionIfNotExists(string databaseName, string collectionName)
        {
            try
            {
                await _client.ReadDocumentCollectionAsync(UriFactory.CreateDocumentCollectionUri(databaseName, collectionName));
                WriteToConsoleAndPromptToContinue("Found {0}", collectionName);
            }
            catch (DocumentClientException de)
            {
                // If the document collection does not exist, create a new collection
                if (de.StatusCode == HttpStatusCode.NotFound)
                {
                    DocumentCollection collectionInfo = new DocumentCollection();
                    collectionInfo.Id = collectionName;

                    // Configure collections for maximum query flexibility including string range queries.
                    collectionInfo.IndexingPolicy = new IndexingPolicy(new RangeIndex(DataType.String) { Precision = -1 });

                    // Here we create a collection with 400 RU/s.
                    await _client.CreateDocumentCollectionAsync(
                        UriFactory.CreateDatabaseUri(databaseName),
                        collectionInfo,
                        new RequestOptions { OfferThroughput = 400 });

                    WriteToConsoleAndPromptToContinue("Created {0}", collectionName);
                }
                else
                {
                    throw;
                }
            }
        }

        private static async Task CreateHostDocumentIfNotExists(string databaseName, string collectionName, Host host)
        {
            bool flag = false;
            while (true)
            {
                try
                {
                    if (flag)
                    {
                        Console.WriteLine($"Re-get {host.Id}");
                    }
                    await _client.ReadDocumentAsync(UriFactory.CreateDocumentUri(databaseName, collectionName, host.Id));
                    break;
                }
                catch (DocumentClientException de)
                {
                    if (de.StatusCode == HttpStatusCode.NotFound)
                    {
                        while (true)
                        {
                            try
                            {
                                await
                                    _client.CreateDocumentAsync(
                                        UriFactory.CreateDocumentCollectionUri(databaseName, collectionName), host);
                                Console.WriteLine($"Create {host.Name} (Id: {host.Id})");
                                return;
                            }
                            catch (DocumentClientException dce)
                            {
                                Console.WriteLine($"{dce.StatusCode} Request too large, {host.Name} (Id: {host.Id})");
                            }
                        }
                    }
                    else
                    {
                        flag = true;
                        Console.WriteLine($"{de.StatusCode} Read Request too large, {host.Name} (Id: {host.Id})");
                    }
                }
            }
        }

        public class Host
        {
            [JsonProperty(PropertyName = "id")]
            public string Id { get; set; }
            public string Name { get; set; }

            public override string ToString()
            {
                return JsonConvert.SerializeObject(this);
            }
        }
    }
}
