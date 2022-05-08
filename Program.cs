using System;
using System.Threading.Tasks;
using System.Configuration;
using System.Collections.Generic;
using System.Net;
using Microsoft.Azure.Cosmos;
using CosmosDbSQL.Models;

namespace CosmosDbSQL
{
    public class Program
    {
        private string EndpointUrl = Environment.GetEnvironmentVariable("EndpointUrl");
        private string PrimaryKey = Environment.GetEnvironmentVariable("PrimaryKey");

        private CosmosClient cosmosClient;
        private Database database;
        private Container container;

        private string databaseId = "FamilyDatabase";
        private string containerId = "FamilyContainer";


        public static async Task Main(string[] args)
        {
            try
            {
                Console.WriteLine("Beginning operations... \n");
                Program p = new Program();
                await p.GetstartedDemoAsync();
            }
            catch (CosmosException ex)
            {
                Exception baseException = ex.GetBaseException();
                Console.WriteLine("{0} error occured: {1}", ex.StatusCode, ex);
            }
            catch (Exception e)
            {
                Console.WriteLine($"Error: {e}");
            }
            finally
            {
                Console.WriteLine("End of demo, press and ket to exit.");
                Console.ReadKey();
            }
        }

        private async Task GetstartedDemoAsync()
        {
            this.cosmosClient = new CosmosClient(EndpointUrl, PrimaryKey);
            await this.CreateDatabaseAsync();
            await this.CreateContainerAsync();
            await this.AddItemsToContainerAsync();
            await this.QueryItemsAsyn();
        }

        //Create Database
        private async Task CreateDatabaseAsync()
        {
            this.database = await this.cosmosClient.CreateDatabaseIfNotExistsAsync(databaseId);
            Console.WriteLine($"Created Database: {this.database.Id} \n");
        }

        //Cerate Container
        private async Task CreateContainerAsync()
        {
            this.container = await this.database.CreateContainerIfNotExistsAsync(containerId, "/LastName");
            Console.WriteLine($"Created Container: {this.container.Id} \n");
        }

        //Create an item
        private async Task AddItemsToContainerAsync()
        {
            Family andersenFamily = new Family
            {
                Id = "Andersen.1",
                LastName = "Andersen",
                Parents = new Parent[]
                {
                    new Parent{FirstName = "Thomas"},
                    new Parent{FirstName = "Mary Kay"}
                },
                Children = new Child[]
                {
                    new Child
                    {
                        FisrtName = "Henriette Thaulow",
                        Gender = "female",
                        Grade = 5,
                        Pets = new Pet[]
                        {
                            new Pet{GivenName = "Fluffy"}
                        }
                    }
                },
                Address = new Address { State = "WA", Country = "King", City = "Seattle" },
                IsRegistered = false
            };

            try
            {
                ItemResponse<Family> andersenFamilyResponse = await this.container.CreateItemAsync<Family>
                    (andersenFamily, new PartitionKey(andersenFamily.LastName));
                Console.WriteLine("Created item in database with id: {0} Operation consumed {1} RUs.\n",
                    andersenFamilyResponse.Resource.Id, andersenFamilyResponse.RequestCharge);
            }
            catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.Conflict)
            {
                Console.WriteLine("Item in database with id: {0} already exists\n", andersenFamily.Id);
            }
        }

        //Query the items
        private async Task QueryItemsAsyn()
        {
            var sqlQueryText = "SELECT * FROM c WHERE c.LastName = 'Andersen'";
            Console.WriteLine($"Running query: {sqlQueryText}");

            QueryDefinition queryDefinition = new QueryDefinition(sqlQueryText);
            FeedIterator<Family> queryResultSetIterator = this.container.GetItemQueryIterator<Family>(queryDefinition);

            List<Family> families = new List<Family>();

            while (queryResultSetIterator.HasMoreResults)
            {
                FeedResponse<Family> currentResultSet = await queryResultSetIterator.ReadNextAsync();
                foreach (Family family in currentResultSet)
                {
                    families.Add(family);
                    Console.WriteLine($"\tRead: {family}\n");
                }
            }
        }

        //Delete the database
        private async Task DeleteDatabaseAndCleanupAsyn()
        {
            DatabaseResponse databaseResourceResponse = await this.database.DeleteAsync();

            Console.WriteLine("Deleted Database: {0}\n", this.databaseId);

            this.cosmosClient.Dispose();
        }
    }
}
