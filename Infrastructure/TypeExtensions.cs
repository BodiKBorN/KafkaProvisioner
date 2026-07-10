using System;
using System.Collections.Generic;
using System.Linq;

namespace Infrastructure
{
    public static class TypeExtensions
    {
        public static T NotNull<T>(this T source, string argumentName = null)
        {
            if (source == null) throw new ArgumentNullException(argumentName);
            return source;
        }

        public static bool IsNullOrEmpty<T>(this IEnumerable<T> source)
        {
            return source == null || !source.Any();
        }
        
        public static bool IsSame(this string item, string value)
        {
            return item != null && item.Equals(value, StringComparison.OrdinalIgnoreCase);
        }

        public static bool HasProperty(this Type t, string propertyName)
        {
            return !string.IsNullOrWhiteSpace(propertyName) && t.GetProperty(propertyName) != null;
        }
    }
}