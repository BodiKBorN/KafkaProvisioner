using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Infrastructure;

public static class ServiceLocator
{
    private static IServiceProvider _serviceProvider;
    public static void RegisterDefaultServiceLocator(this IServiceProvider serviceProvider)
    {
        _serviceProvider = serviceProvider;
    }

    public static object GetInstance(Type serviceType)
    {
        return _serviceProvider?.GetService(serviceType);
    }
    
    public static TService GetInstance<TService>()
    {
        return _serviceProvider == null ? default : _serviceProvider.GetService<TService>();
    }

    public static IEnumerable<TService> GetInstances<TService>()
    {        
        return _serviceProvider == null ? Enumerable.Empty<TService>() : _serviceProvider.GetServices<TService>();
    }
}
