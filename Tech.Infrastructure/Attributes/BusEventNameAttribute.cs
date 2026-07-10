using System;

namespace Tech.Domain.Attributes;

public class BusEventNameAttribute : Attribute
{
    public BusEventNameAttribute(string name)
    {
        Name = name;
    }

    public string Name { get; set; }
}