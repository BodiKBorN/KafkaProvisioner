using System;
using System.Diagnostics;
using Tech.Social.Infrastructure.Logging;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Serilog;
using Serilog.Events;

namespace Tech.Social.Infrastructure;

public abstract class CorePlatformHostBuilder
{
    public void BuildHost<TStartup>(string[] args, LogEventLevel logLevel = LoggerHelper.DefaultLevel) where TStartup : class
    {
        Activity.DefaultIdFormat = ActivityIdFormat.W3C;
        var env = Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT");

        try
        {
            var host = CreateHostBuilder<TStartup>(args).Build();
            // host.Services.RegisterDefaultServiceLocator();

            var configuration = GetConfigurationRoot(env);
            // Log.Logger = CreateLogger(configuration, logLevel);

            host.Run();
        }
        catch (Exception ex)
        {
            Log.Error(ex, "An error occurred while starting the application");
            throw;
        }
        finally
        {
            Log.CloseAndFlush();
        }
    }

    protected abstract IHostBuilder CreateHostBuilder<TStartup>(string[] args)
        where TStartup : class;

    // private static Logger CreateLogger(IConfiguration configuration, LogEventLevel logEventLevel)
    // {
    //     return new LoggerConfiguration()
    //         .Filter.ByExcluding(
    //             logEvent =>
    //                 logEvent.Properties.TryGetValue(SerilogConstants.EventProperties.SkipLog, out var value)
    //                 && (value as ScalarValue)?.Value is true
    //         )
    //         .ReadFrom.Configuration(configuration)
    //         .ConfigureLogAreas(logEventLevel)
    //         .Enrich.FromLogContext()
    //         .Enrich.With<ActivityEnricher>()
    //         .WriteTo.Console(new ElasticsearchJsonFormatter(renderMessage: false))
    //         .AddMetricLogging()
    //         .CreateLogger();
    // }

    public static IConfigurationRoot GetConfigurationRoot(string env = null)
    {
        Debug.WriteLine($"ASPNETCORE_ENVIRONMENT: {Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT")}");
        return new ConfigurationBuilder()
            .AddJsonFile("appsettings.json", false, true)
            .AddJsonFile($"appsettings.{env ?? Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT")}.json", true, true)
            .AddEnvironmentVariables()
            .Build();
    }
}