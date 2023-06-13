namespace Lyra.Api.Configuration.Validation
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.ComponentModel.DataAnnotations;
    using System.Linq;
    using System.Reflection;
    using System.Security.Cryptography;
    using System.Text;
    using Microsoft.Extensions.Configuration;
    using Microsoft.Extensions.Configuration.EnvironmentVariables;

    public static class ConfigurationValidator
    {

        public static void PrintProperties<T>(
            IConfiguration configRoot,
            T subject,
            StringBuilder builder,
            string prefix = null)
        {

            // Verify if config is an iconfiguration root. 
            // aspnet core startup registers the configuration as an IConfiguration, 
            // not as the IConfigurationRoot as it actually is. So, the choice is 
            // to cast it everywhere when using it or to cast it here. I'm casting 
            // it here. 
            if (!(configRoot is IConfigurationRoot))
            {
                throw new ArgumentException("Config has to be an IConfigurationRoot", nameof(configRoot));
            }

            var type = subject.GetType();
            var properties = GetProperties(type, subject).OrderBy(x => x.Name);

            var providers = ((IConfigurationRoot) configRoot).GetProvidersByKey();

            foreach (var property in properties)
            {
                var propertyName = prefix + property.Name;
                var definedIn = GetProvidersThatDefineThisProperty(providers, propertyName);

                var printableValue = property.Value?.ToString();
                if (string.IsNullOrEmpty(printableValue))
                {
                    printableValue = "<missing>";
                }
                else if (property.Redacted)
                {
                    printableValue = $"<redacted #{GetHash(property)}>";
                }
                else printableValue = "\"" + printableValue + "\"";

                builder.AppendLine($" - {propertyName} = {printableValue}{definedIn}");
            }
        }

        /// <summary>
        /// Calculates a deterministic number from the input to allow a visual
        /// inspection of the result to see if the value has changed
        /// </summary>
        /// <param name="input"></param>
        /// <returns></returns>
        private static string GetHash(SettingsProperty input)
        {
            return Convert.ToBase64String(SHA256.Create().ComputeHash(Encoding.UTF8.GetBytes(input.Value.ToString())));
        }

        private static string GetProvidersThatDefineThisProperty(IDictionary<string, IConfigurationProvider[]> providers, string property)
        {
            string definedIn = "";
            if (providers.TryGetValue(property, out var values))
            {
                var providerName = string.Join(", ", values.Select(GetProviderName));
                definedIn = $" - from ({providerName})";
            }

            return definedIn;
        }

        private static string GetProviderName(IConfigurationProvider provider)
        {
            var info = provider.GetType().Name.Replace("ConfigurationProvider", "");

            switch (provider)
            {
                case FileConfigurationProvider p:
                    info += " (Path=" + p.Source.Path + ")";
                    break;
                case EnvironmentVariablesConfigurationProvider p:
                    // Nasty, would be better if prefix was a property
                    var field = typeof(EnvironmentVariablesConfigurationProvider).GetField("_prefix",
                        BindingFlags.NonPublic | BindingFlags.GetField | BindingFlags.Instance);
                    var prefix = field.GetValue(p);
                    info += " (Prefix='" + prefix + "')";
                    break;
            }

            return info;
        }

        public static List<SettingsProperty> GetProperties(Type type, object subject, string prefix = null)
        {
            var properties = type.GetProperties();
            var propertyNames = new List<SettingsProperty>();
            foreach (var property in properties)
            {
                object propertyValue = subject == null
                    ? null
                    : property.GetValue(subject);

                // Check if this property can be safely ignored
                if (property.GetCustomAttribute<NotConfiguredAttribute>() != null)
                {
                    continue;
                }

                bool redacted = property.GetCustomAttribute<RedactedAttribute>() != null;

                if (property.PropertyType == typeof(string) || property.PropertyType.IsValueType)
                {
                    propertyNames.Add(new SettingsProperty(prefix + property.Name, propertyValue, redacted, property));
                }
                else
                {
                    foreach (var subProperty in GetProperties(property.PropertyType, propertyValue, prefix + property.Name + ":"))
                    {
                        propertyNames.Add(subProperty);
                    }
                }
            }

            return propertyNames;
        }

        public static bool TryValidateObject(object obj, ICollection<ValidationResult> results, IDictionary<object, object> validationContextItems = null) =>
            Validator.TryValidateObject(obj, new ValidationContext(obj, null, validationContextItems), results, true);

        public static bool TryValidateObjectRecursive<T>(T obj, List<ValidationResult> results, IDictionary<object, object> validationContextItems = null) =>
            TryValidateObjectRecursive(obj, results, new HashSet<object>(), validationContextItems);

        public static bool TryValidate<T>(T subject, out IReadOnlyCollection<ConfigurationError> errorsResult)
        {
            var validationResult = new List<ValidationResult>();
            var isValid = TryValidateObjectRecursive(subject, validationResult);

            var errors = new List<ConfigurationError>();

            foreach (var error in validationResult)
            {
                errors.Add(new ConfigurationError()
                {
                    ErrorMessage = error.ErrorMessage,
                    Property = error.MemberNames?.FirstOrDefault(),
                });
            }

            errorsResult = errors;
            return isValid;
        }

        private static bool TryValidateObjectRecursive<T>(
            T obj,
            List<ValidationResult> results,
            ISet<object> validatedObjects,
            IDictionary<object, object> validationContextItems = null)
        {
            if (validatedObjects.Contains(obj))
            {
                return true;
            }

            validatedObjects.Add(obj);
            var result = TryValidateObject(obj, results, validationContextItems);

            var properties = obj.GetType().GetProperties()
                .Where(prop => prop.CanRead && prop.GetIndexParameters().Length == 0)
                .OrderBy(x => x.Name)
                .ToList();

            foreach (var property in properties)
            {
                if (property.PropertyType == typeof(string) || property.PropertyType.IsValueType)
                {
                    continue;
                }

                var propertyInfo = obj.GetType().GetProperty(property.Name);
                var value = propertyInfo != null ? propertyInfo.GetValue(obj, null) : string.Empty;

                if (value == null)
                {
                    if (property.GetCustomAttribute<NotConfiguredAttribute>() == null)
                    {
                        results.Add(new ValidationResult($"{property.Name} is null", new string[] { property.Name }));
                    }

                    continue;
                }

                if (value is IEnumerable asEnumerable)
                {
                    foreach (var enumObj in asEnumerable)
                    {
                        if (enumObj != null)
                        {
                            var nestedResults = new List<ValidationResult>();
                            if (!TryValidateObjectRecursive(enumObj, nestedResults, validatedObjects, validationContextItems))
                            {
                                result = false;
                                foreach (var validationResult in nestedResults)
                                {
                                    var property1 = property;
                                    results.Add(new ValidationResult(validationResult.ErrorMessage, validationResult.MemberNames.Select(x => property1.Name + ':' + x)));
                                }
                            };
                        }
                        else
                        {
                            results.Add(new ValidationResult($"{property.Name} is null", new string[]{property.Name}));
                        }
                    }
                }
                else
                {
                    var nestedResults = new List<ValidationResult>();
                    if (!TryValidateObjectRecursive(value, nestedResults, validatedObjects, validationContextItems))
                    {
                        result = false;
                        foreach (var validationResult in nestedResults)
                        {
                            var property1 = property;
                            results.Add(new ValidationResult(validationResult.ErrorMessage, validationResult.MemberNames.Select(x => property1.Name + ':' + x)));
                        }
                    };
                }
            }

            return result;
        }
    }   
}
