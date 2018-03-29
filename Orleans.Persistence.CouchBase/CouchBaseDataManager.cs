using System;
using System.Threading.Tasks;
using Couchbase;
using Couchbase.Authentication;
using Couchbase.Configuration.Client;
using Couchbase.Core;
using Couchbase.IO;
using Orleans.Storage;

namespace Orleans.Persistence.CouchBase
{
    /// <summary>
    ///     Interfaces with CouchBase on behalf of the provider.
    /// </summary>
    internal class CouchBaseDataManager
    {
        /// <summary>
        ///     Name of the bucket that it works with.
        /// </summary>
        protected readonly string BucketName;

        /// <summary>
        ///     The cached bucket reference
        /// </summary>
        protected IBucket Bucket;

        /// <summary>
        ///     Constructor
        /// </summary>
        /// <param name="bucketName">Name of the bucket that this manager should operate on.</param>
        /// <param name="clientConfig">Configuration object for the database client</param>
        public CouchBaseDataManager(string bucketName, ClientConfiguration clientConfig, string username, string password)
        {
            //Bucket name should not be empty
            //Keep in mind that you should create the buckets before being able to use them either
            //using the commandline tool or the web console
            if (string.IsNullOrWhiteSpace(bucketName))
                throw new ArgumentException("bucketName can not be null or empty");
            //config should not be null either
            if (clientConfig == null)
                throw new ArgumentException("You should suply a configuration to connect to CouchBase");

            BucketName = bucketName;
            ClusterHelper.Initialize(clientConfig, new PasswordAuthenticator(username, password));
            //cache the bucket.
            Bucket = ClusterHelper.GetBucket(BucketName);
        }

        /// <summary>
        ///     Deletes a document representing a grain state object.
        /// </summary>
        /// <param name="collectionName">The type of the grain state object.</param>
        /// <param name="key">The grain id string.</param>
        /// <returns>Completion promise for this operation.</returns>
        public async Task Delete(string collectionName, string key, string eTag)
        {
            var docId = GetDocumentID(collectionName, key);
            var result = await Bucket.RemoveAsync(docId, ulong.Parse(eTag));
            if (!result.Success)
                throw new InconsistentStateException(result.Message, eTag, result.Cas.ToString());
        }

        /// <summary>
        ///     Reads a document representing a grain state object.
        /// </summary>
        /// <param name="collectionName">The type of the grain state object.</param>
        /// <param name="key">The grain id string.</param>
        /// <returns>Completion promise for this operation.</returns>
        public async Task<Tuple<string, string>> Read(string collectionName, string key)
        {
            var docId = GetDocumentID(collectionName, key);

            //If there is a value we read it and consider the CAS as ETag as well and return
            //both as a tuple
            var result = await Bucket.GetAsync<string>(docId);
            if (result.Success)
                return Tuple.Create(result.Value, result.Cas.ToString());
            if (!result.Success && result.Status == ResponseStatus.KeyNotFound) //not found
                return Tuple.Create<string, string>(null, "");
            throw result.Exception;
        }

        /// <summary>
        ///     Writes a document representing a grain state object.
        /// </summary>
        /// <param name="collectionName">The type of the grain state object.</param>
        /// <param name="key">The grain id string.</param>
        /// <param name="entityData">The grain state data to be stored./</param>
        /// <returns>Completion promise for this operation.</returns>
        public async Task<string> Write(string collectionName, string key, string entityData, string eTag)
        {
            var docId = GetDocumentID(collectionName, key);
            if (ulong.TryParse(eTag, out var realETag))
            {
                var r = await Bucket.UpsertAsync(docId, entityData, realETag);
                if (!r.Success) throw new InconsistentStateException(r.Status.ToString(), eTag, r.Cas.ToString());

                return r.Cas.ToString();
            }
            else
            {
                var r = await Bucket.InsertAsync(docId, entityData);

                //check if key exist and we don't have the CAS
                if (!r.Success && r.Status == ResponseStatus.KeyExists)
                    throw new InconsistentStateException(r.Status.ToString(), eTag, r.Cas.ToString());
                if (!r.Success)
                    throw new Exception(r.Status.ToString());
                return r.Cas.ToString();
            }
        }

        public void Dispose()
        {
            Bucket.Dispose();
            Bucket = null;
            //Closes the DB connection
            ClusterHelper.Close();
        }


        /// <summary>
        ///     Creates a document ID based on the type name and key of the grain
        /// </summary>
        /// <param name="collectionName"></param>
        /// <param name="key"></param>
        /// <returns></returns>
        /// <remarks>
        ///     Each ID should be at most 250 bytes and it should not cause an issue unless you have
        ///     an appetite for very long class names.
        ///     The id will be of form TypeName_Key where TypeName doesn't include any namespace
        ///     or version info.
        /// </remarks>
        private string GetDocumentID(string collectionName, string key)
        {
            return collectionName + "_" + key;
        }
    }
}