using System.Text;

namespace Ormamu;

/// <summary>
/// A collection of commonly-used name converters methods to use in a <see cref="OrmamuOptions"/> definition. 
/// </summary>
public static class NameConverters
{
    /// <summary>
    /// Snake case conversion method
    /// </summary>
    public static string ToSnakeCase(string input)
    {
        if (string.IsNullOrEmpty(input))
            return input;

        StringBuilder builder = new();
        for (int i = 0; i < input.Length; i++)
        {
            var c = input[i];
            if (char.IsUpper(c))
            {
                if (i > 0) builder.Append('_');
                builder.Append(char.ToLowerInvariant(c));
            }
            else
            {
                builder.Append(c);
            }
        }
        return builder.ToString();
    }
    
    /// <summary>
    /// Kebab case conversion method
    /// </summary>
    public static string ToKebabCase(string input)
    {
        if (string.IsNullOrEmpty(input))
            return input;

        StringBuilder builder = new();
        for (int i = 0; i < input.Length; i++)
        {
            var c = input[i];
            if (char.IsUpper(c))
            {
                if (i > 0) builder.Append('-');
                builder.Append(char.ToLowerInvariant(c));
            }
            else
            {
                builder.Append(c);
            }
        }
        return builder.ToString();
    }
    
    /// <summary>
    /// Upper case conversion method
    /// </summary>
    public static string ToUpperCase(string input)
        => input.ToUpperInvariant();
    
    /// <summary>
    /// Lower case conversion method
    /// </summary>
    public static string ToLowerCase(string input)
        => input.ToLowerInvariant();
}