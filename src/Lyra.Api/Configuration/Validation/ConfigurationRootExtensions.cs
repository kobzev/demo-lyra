namespace Lyra.Api.Configuration.Validation
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Microsoft.Extensions.Configuration;

    public static class ConfigurationRootExtensions
    {
        public static IDictionary<string, IConfigurationProvider[]> GetProvidersByKey(this IConfigurationRoot root)
        {
            var keys = new Dictionary<string, IConfigurationProvider[]>(StringComparer.OrdinalIgnoreCase);
            var configurationProviders = root.Providers.Reverse();

            void RecurseChildren(IEnumerable<IConfigurationSection> children)
            {
                foreach (var child in children)
                {
                    var providers = configurationProviders
                        .Where(provider => provider.TryGet(child.Path, out var _))
                        .ToArray();

                    if (providers.Length > 0)
                    {
                        if (keys.ContainsKey(child.Path))
                        {
                            keys[child.Path] = keys[child.Path]
                                .Union(providers)
                                .ToArray();
                        }
                        else
                        {
                            keys.Add(child.Path, providers);
                        }

                    }

                    RecurseChildren(child.GetChildren());
                }
            }

            var configurationSections = root.GetChildren();

            RecurseChildren(configurationSections);

            return keys;
        }
    }
}
