namespace Lyra.Instruments.Configuration.Validation
{
    using System.Reflection;

    public class SettingsProperty
    {
        public readonly string Name;
        public readonly object Value;
        public readonly bool Redacted;
        public readonly PropertyInfo PropertyInfo;

        public SettingsProperty(string name, object value, bool redacted, PropertyInfo propertyInfo)
        {
            this.Name = name;
            this.Value = value;
            this.Redacted = redacted;
            this.PropertyInfo = propertyInfo;
        }
    }
}
