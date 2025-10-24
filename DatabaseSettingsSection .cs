using System.Configuration;

namespace DBCompare
{
    public class DatabaseSettingsSection : ConfigurationSection
    {
        [ConfigurationProperty("connections")]
        public ConnectionCollection Connections
        {
            get { return (ConnectionCollection)this["connections"]; }
        }

        [ConfigurationProperty("excludeTables")]
        public ExcludeTableCollection ExcludeTables
        {
            get { return (ExcludeTableCollection)this["excludeTables"]; }
        }
    }

    [ConfigurationCollection(typeof(ConnectionElement), AddItemName = "add")]
    public class ConnectionCollection : ConfigurationElementCollection
    {
        protected override ConfigurationElement CreateNewElement() => new ConnectionElement();
        protected override object GetElementKey(ConfigurationElement element) => ((ConnectionElement)element).Name;
        public new ConnectionElement this[string name] => (ConnectionElement)BaseGet(name);
    }

    public class ConnectionElement : ConfigurationElement
    {
        [ConfigurationProperty("name", IsRequired = true)]
        public string Name => (string)this["name"];

        [ConfigurationProperty("connectionString", IsRequired = true)]
        public string ConnectionString => (string)this["connectionString"];
    }

    [ConfigurationCollection(typeof(ExcludeTableElement), AddItemName = "add")]
    public class ExcludeTableCollection : ConfigurationElementCollection
    {
        protected override ConfigurationElement CreateNewElement() => new ExcludeTableElement();
        protected override object GetElementKey(ConfigurationElement element) => ((ExcludeTableElement)element).Name;
        public new ExcludeTableElement this[string name] => (ExcludeTableElement)BaseGet(name);
    }

    public class ExcludeTableElement : ConfigurationElement
    {
        [ConfigurationProperty("name", IsRequired = true)]
        public string Name => (string)this["name"];
    }
}
