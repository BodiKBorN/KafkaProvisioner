using System;
using System.Linq;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;
namespace Tech.Infrastructure.Utilities;

public static class HashHelper
{
    /// <summary>
    /// This approach provides a flexible and dynamic way to compute hash codes based on object properties, leveraging the power of reflection in C#.
    /// Ensure that the hash code is stable and consistent across different instances and executions
    /// </summary>
    /// <param name="obj"></param>
    /// <typeparam name="T"></typeparam>
    /// <returns>HashCode</returns>
    public static int GetStableHashCode<T>(T obj)
    {
        using var sha256 = SHA256.Create();
        var properties = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance);
        var propertyValues = properties
            .Select(p => p.GetValue(obj, null))
            .Select(v => v?.ToString() ?? string.Empty);
        
        var hashInput = string.Join("-", propertyValues);
        var bytes = Encoding.UTF8.GetBytes(hashInput);
        var hashBytes = sha256.ComputeHash(bytes);
        
        return BitConverter.ToInt32(hashBytes, 0);
    }
}