namespace Orleans.Persistence.CouchBase
{
    /// <summary>
    ///     All options needed to use couchbase
    /// </summary>
    public class CouchBaseGrainStorageOptions
    {
        /// <summary>
        ///     Couchbase server address with port
        /// </summary>
        public string Address { get; set; } = "http://localhost:8092";

        /// <summary>
        ///     Couchbase userbase
        /// </summary>
        public string Username { get; set; } = "admin";

        /// <summary>
        ///     Couchbase password
        /// </summary>
        public string Password { get; set; } = "password";

        /// <summary>
        ///     Default bucket name which grain data will be saved in
        /// </summary>
        public string BucketName { get; set; } = "Grains";

        /// <summary>
        ///     Get or set a value indicating whether serialized json should be indtented or not
        /// </summary>
        public bool IndentJson { get; set; }
    }
}