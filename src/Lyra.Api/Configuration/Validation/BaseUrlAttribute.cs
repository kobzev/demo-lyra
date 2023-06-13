namespace Lyra.Api.Configuration.Validation
{
    using System;
    using System.ComponentModel.DataAnnotations;

    /// <summary>
    /// Url that's meant to be used as a base url for creating paths
    /// Should end with a "/"
    /// </summary>
    public class BaseUrlAttribute : ValidationAttribute
    {
        public override string FormatErrorMessage(string name)
        {
            return $"{name} is not a valid url or it doesn't end with a '/'";
        }

        public override bool IsValid(object value)
        {
            var stringValue = value?.ToString();
            if (string.IsNullOrEmpty(stringValue))
                return true;

            return Uri.TryCreate(stringValue, UriKind.Absolute, out var notUsed)
                && stringValue.EndsWith("/");

        }
    }
}
