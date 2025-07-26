using System.Text;

namespace Ormamu;

internal static class CompositionUtilities
{
    internal static StringBuilder AppendProperties(
        this StringBuilder sb,
        PropertyMapping[] properties,
        AppendType type,
        char propertyWrapper = '\0',
        bool skipKey = false)
    {
        bool firstSeparatorSkip = true;
        foreach (PropertyMapping property in properties)
        {
            if (property.IsDbGenerated || (property.IsKey && skipKey)) continue;

            switch (type)
            {
                case AppendType.Assembly:
                    sb.AppendWithSeparator('@', skipSeparator: firstSeparatorSkip);
                    sb.Append(property.AssemblyName);
                    break;

                case AppendType.Equality:
                    sb.AppendWithSeparatorAndWrapper(property.DbName, propertyWrapper, ',', firstSeparatorSkip);
                    sb.Append("=@");
                    sb.Append(property.AssemblyName);
                    break;

                default:
                    sb.AppendWithSeparatorAndWrapper(property.DbName, propertyWrapper, ',', firstSeparatorSkip);
                    break;
            }
            if(firstSeparatorSkip) firstSeparatorSkip = false;
        }
        return sb;
    }

    internal static StringBuilder AppendWithSeparator(
        this StringBuilder primary,
        string addition,
        char separator = ',',
        bool skipSeparator = false)
    {
        if (primary.Length > 0 && !skipSeparator) primary.Append(separator);
        primary.Append(addition);
        return primary;
    }

    internal static void AppendWithSeparator(
        this StringBuilder primary,
        string addition,
        string separator = ",",
        bool skipSeparator = false)
    {
        if (primary.Length > 0 && !skipSeparator) primary.Append(separator);
        primary.Append(addition);
    }

    private static void AppendWithSeparator(
        this StringBuilder primary,
        char addition,
        char separator = ',',
        bool skipSeparator = false)
    {
        if (primary.Length > 0 && !skipSeparator) primary.Append(separator);
        primary.Append(addition);
    }

    internal static void AppendWithSeparatorAndWrapper(
        this StringBuilder sb,
        string addition,
        char propertyWrapper = '\0',
        char separator = ',',
        bool skipSeparator = false)
    {
        if (sb.Length > 0 && !skipSeparator)
            sb.Append(separator);

        if (propertyWrapper != '\0')
        {
            sb.AppendWithWrapper(addition, propertyWrapper);
        }
        else
        {
            sb.Append(addition);
        }
    }

    internal static StringBuilder AppendWithWrapper(
        this StringBuilder sb,
        string addition,
        char propertyWrapper = '\0')
    {

        if (propertyWrapper != '\0')
        {
            sb.Append(propertyWrapper).Append(addition).Append(propertyWrapper);
        }
        else
        {
            sb.Append(addition);
        }
        return sb;
    }

    internal static void AppendEquality(
        this StringBuilder sb,
        PropertyMapping key,
        bool paramFromAssembly = false,
        char propertyWrapper = '\0',
        int index = -1)
    {
        if (propertyWrapper != '\0')
        {
            sb.Append(propertyWrapper).Append(key.DbName).Append(propertyWrapper);
        }
        else
        {
            sb.Append(key.DbName);
        }
        
        sb.Append("=@")
            .Append(paramFromAssembly ? key.AssemblyName : "KeyValue");
        
        if(index != -1) sb.Append(index);
    }
}

internal enum AppendType {
    Db,
    Assembly,
    Equality
}