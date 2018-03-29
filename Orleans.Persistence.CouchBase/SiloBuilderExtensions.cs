using System;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Providers;
using Orleans.Runtime;
using Orleans.Storage;

namespace Orleans.Persistence.CouchBase
{
    public static class SiloBuilderExtensions
    {
        /// <summary>
        /// Configure silo to use CouchBase grain storage as the default grain storage.
        /// </summary>
        public static ISiloHostBuilder AddCouchBaseGrainStorageAsDefault(this ISiloHostBuilder builder, Action<CouchBaseGrainStorageOptions> configureOptions)
        {
            return builder.AddCouchBaseGrainStorage(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME, configureOptions);
        }

        /// <summary>
        /// Configure silo to use  CouchBase grain storagee for grain storage.
        /// </summary>
        public static ISiloHostBuilder AddCouchBaseGrainStorage(this ISiloHostBuilder builder, string name, Action<CouchBaseGrainStorageOptions> configureOptions)
        {
            return builder.ConfigureServices(services => services.AddCouchBaseGrainStorage(name, configureOptions));
        }

        /// <summary>
        /// Configure silo to use  CouchBase grain storage as the default grain storage.
        /// </summary>
        public static ISiloHostBuilder AddCouchBaseGrainStorageAsDefault(this ISiloHostBuilder builder, Action<OptionsBuilder<CouchBaseGrainStorageOptions>> configureOptions = null)
        {
            return builder.AddCouchBaseGrainStorage(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME, configureOptions);
        }

        /// <summary>
        /// Configure silo to use CouchBase grain storage for grain storage.
        /// </summary>
        public static ISiloHostBuilder AddCouchBaseGrainStorage(this ISiloHostBuilder builder, string name, Action<OptionsBuilder<CouchBaseGrainStorageOptions>> configureOptions = null)
        {
            return builder.ConfigureServices(services => services.AddCouchBaseGrainStorage(name, configureOptions));
        }

        /// <summary>
        /// Configure silo to use  CouchBase grain storage as the default grain storage.
        /// </summary>
        public static IServiceCollection AddCouchBaseGrainStorage(this IServiceCollection services, Action<CouchBaseGrainStorageOptions> configureOptions)
        {
            return services.AddCouchBaseGrainStorage(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME, ob => ob.Configure(configureOptions));
        }

        /// <summary>
        /// Configure silo to use CouchBase grain storage for grain storage.
        /// </summary>
        public static IServiceCollection AddCouchBaseGrainStorage(this IServiceCollection services, string name, Action<CouchBaseGrainStorageOptions> configureOptions)
        {
            return services.AddCouchBaseGrainStorage(name, ob => ob.Configure(configureOptions));
        }

        /// <summary>
        /// Configure silo to use CouchBase grain storage as the default grain storage.
        /// </summary>
        public static IServiceCollection AddCouchBaseGrainStorageAsDefault(this IServiceCollection services, Action<OptionsBuilder<CouchBaseGrainStorageOptions>> configureOptions = null)
        {
            return services.AddCouchBaseGrainStorage(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME, configureOptions);
        }

        /// <summary>
        ///     Configure silo to use CouchBase grain storage for grain storage.
        /// </summary>
        public static IServiceCollection AddCouchBaseGrainStorage(this IServiceCollection services, string name,
            Action<OptionsBuilder<CouchBaseGrainStorageOptions>> configureOptions = null)
        {
            configureOptions?.Invoke(services.AddOptions<CouchBaseGrainStorageOptions>(name));
            services.ConfigureNamedOptionForLogging<CouchBaseGrainStorageOptions>(name);
            services.TryAddSingleton(sp =>
                sp.GetServiceByName<IGrainStorage>(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME));
            return services.AddSingletonNamedService(name, CouchBaseStorage.Create);
        }
    }
}