namespace Lyra.Instruments.Configuration.Validation
{
    using System;
    using System.ComponentModel.DataAnnotations;

    /// <summary>
    /// Relative URLS meant to be used as a root path.
    /// Should start with a "/"
    /// </summary>
    public class RootPathAttribute : ValidationAttribute
    {
        public override string FormatErrorMessage(string name)
        {
            return $"{name} is not a valid root path as it doesn't start with a '/'";
        }

        public override bool IsValid(object value)
        {
            var stringValue = value?.ToString();
            if (string.IsNullOrEmpty(stringValue))
                return false;

            return Uri.TryCreate(stringValue, UriKind.Relative, out var notUsed)
                && stringValue.StartsWith("/");

        }
    }
}
