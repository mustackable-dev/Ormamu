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
        StringBuilder sb = new();
        for (int i = 0; i < input.Length; i++)
        {
            if (char.IsUpper(input[i]))
            {
                if(i>0)
                    sb.Append('_');
                
                sb.Append(char.ToLowerInvariant(input[i]));
                continue;
            }
            sb.Append(input[i]);
            
        }
        
        return sb.ToString();
    }
    
    /// <summary>
    /// Kebab case conversion method
    /// </summary>
    public static string ToKebabCase(string input)
    {
        StringBuilder sb = new();
        for (int i = 0; i < input.Length; i++)
        {
            if (char.IsUpper(input[i]))
            {
                if(i>0)
                    sb.Append('-');
                
                sb.Append(char.ToLowerInvariant(input[i]));
                continue;
            }
            sb.Append(input[i]);
            
        }
        
        return sb.ToString();
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