using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Newtonsoft.Json;
using RandomNameGeneratorLibrary;

namespace WestWorld
{
    public class Program
    {
        private static IDocumentClient client;
        public static void Main(string[] args)
        {
            const string endPoint = "https://westworld.documents.azure.com:443/";
            const string key = "1YTKAuKuZOx4I8tkdJGmLNBRnr3kBu3PYQuqQw24HsZnu5rbUnJL33LKW39NNAMGOCLwWrVtR9tVCb44w4slDw==";
            client = new DocumentClient(new Uri(endPoint), key);

            const string databaseName = "WestWorld";
            string collectionName = "HostList";
            if (args.Length > 0)
            {
                collectionName = args[0];
            }
            CreateDatabaseIfNotExists(databaseName).Wait();
            CreateDocumentCollectionIfNotExists(databaseName, collectionName).Wait();
            const int concurrentCount = 100;
            Task[] tasks = new Task[concurrentCount];
            var personGenerator = new PersonNameGenerator();
            for (int i = 99990; i < 100000; i++)
            {
                if (tasks.All(t => t != null && !t.IsCompleted && !t.IsFaulted))
                {
                    Task.WhenAny(tasks).Wait();
                }
                for (int j = 0; j < concurrentCount; j++)
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
            Console.WriteLine("Press any key to continue ...");
            Console.ReadKey();
        }

        private static async Task CreateDatabaseIfNotExists(string databaseName)
        {
            // Check to verify a database with the id=FamilyDB does not exist
            try
            {
                await client.ReadDatabaseAsync(UriFactory.CreateDatabaseUri(databaseName));
                WriteToConsoleAndPromptToContinue("Found {0}", databaseName);
            }
            catch (DocumentClientException de)
            {
                // If the database does not exist, create a new database
                if (de.StatusCode == HttpStatusCode.NotFound)
                {
                    await client.CreateDatabaseAsync(new Database { Id = databaseName });
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
                await client.ReadDocumentCollectionAsync(UriFactory.CreateDocumentCollectionUri(databaseName, collectionName));
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
                    await client.CreateDocumentCollectionAsync(
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
                    await client.ReadDocumentAsync(UriFactory.CreateDocumentUri(databaseName, collectionName, host.Id));
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
                                    client.CreateDocumentAsync(
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
