using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Couchbase.Configuration.Client;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Orleans.Runtime;
using Orleans.Storage;

namespace Orleans.Persistence.CouchBase
{
    public class CouchBaseStorage : IGrainStorage
    {
        private readonly CouchBaseDataManager _dbManager;
        private readonly ILogger<CouchBaseStorage> _logger;

        public CouchBaseStorage(ILogger<CouchBaseStorage> logger,
            IOptions<CouchBaseGrainStorageOptions> options,
            string name)
        {
            _logger = logger;
            var bucketName = !string.IsNullOrWhiteSpace(name)
                ? name + "_" + options.Value.BucketName
                : options.Value.BucketName;
            _dbManager = new CouchBaseDataManager(bucketName, new ClientConfiguration
            {
                Servers = new List<Uri> {new Uri(options.Value.Address)}
            }, options.Value.Username, options.Value.Password);
        }

        public async Task ReadStateAsync(string grainType, GrainReference grainReference, IGrainState grainState)
        {
            var grainTypeName = grainType.Split('.').Last();

            //Reads the state and returns a tuple with (data,ETag) structure.
            var entityDataAndEtag = await _dbManager.Read(grainTypeName, grainReference.ToKeyString());
            //If no data exists Item1 will be null
            //If we can not find any data we don't touch the state object.
            if (entityDataAndEtag.Item1 != null)
            {
                ConvertFromStorageFormat(grainState, entityDataAndEtag.Item1);
                grainState.ETag = entityDataAndEtag.Item2;
            }
        }

        public async Task WriteStateAsync(string grainType, GrainReference grainReference, IGrainState grainState)
        {
            var grainTypeName = grainType.Split('.').Last();
            //Serialize the data
            var entityData = ConvertToStorageFormat(grainState);
            //Get the ETag to send to the DB
            var eTag = grainState.ETag;
            var returnedEtag = await _dbManager.Write(grainTypeName, grainReference.ToKeyString(), entityData, eTag);
            //Set the new ETag on the state object.
            grainState.ETag = returnedEtag;
        }

        public async Task ClearStateAsync(string grainType, GrainReference grainReference, IGrainState grainState)
        {
            var grainTypeName = grainType.Split('.').Last();
            //When deleting we at least read the grain state at least once so we should have the ETag
            await _dbManager.Delete(grainTypeName, grainReference.ToKeyString(), grainState.ETag);
        }

        public static IGrainStorage Create(IServiceProvider services, string name)
        {
            var optionsSnapshot = services.GetRequiredService<IOptionsSnapshot<CouchBaseGrainStorageOptions>>();
            return ActivatorUtilities.CreateInstance<CouchBaseStorage>(services,
                Options.Create(optionsSnapshot.Get(name)), name);
        }

        /// <summary>
        ///     Serializes from a grain instance to a JSON document.
        /// </summary>
        /// <param name="grainState">Grain state to be converted into JSON storage format.</param>
        /// <remarks>
        ///     See:
        ///     JSON.NET's website
        ///     for more on the JSON serializer.
        /// </remarks>
        protected static string ConvertToStorageFormat(IGrainState grainState)
        {
            var jo = JObject.FromObject(grainState.State);
            jo.Add("type", grainState.State.GetType().ToString());
            return jo.ToString();
        }

        /// <summary>
        ///     Constructs a grain state instance by deserializing a JSON document.
        /// </summary>
        /// <param name="grainState">Grain state to be populated for storage.</param>
        /// <param name="entityData">JSON storage format representaiton of the grain state.</param>
        protected static void ConvertFromStorageFormat(IGrainState grainState, string entityData)
        {
            JsonConvert.PopulateObject(entityData, grainState.State);
        }
    }
}