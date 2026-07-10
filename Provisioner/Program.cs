using System.Reflection;
using Confluent.Kafka;
using Microsoft.Extensions.Options;
using Tech.Common;
using Tech.Deployment.KafkaSetup.Services;
using Infrastructure.Options;
using Tech.Kafka.Clients.Admin;
using Tech.Social.Infrastructure;

var builder = Host.CreateApplicationBuilder(args);

var configuration = CorePlatformHostBuilder.GetConfigurationRoot();

// Add services to the container.
var services = builder.Services;

services.Configure<KafkaHostOptions>(configuration.GetSection(nameof(KafkaHostOptions)));
services.AddSingleton<TopicsScanner>();
services.AddScoped<IAdminClient>(x => new AdminClientBuilder(new AdminClientConfig
{
    BootstrapServers = x.GetRequiredService<IOptions<KafkaHostOptions>>().Value.Host
}).Build());
services.AddSingleton<IKafkaAdminClient>(s => new KafkaAdminClient(s.GetRequiredService<IOptions<KafkaHostOptions>>().Value.Host));

services.AddSingleton<ITopicConfigService, TopicConfigService>();


var app = builder.Build();

try
{
    // Console.WriteLine($"{Assembly.GetExecutingAssembly().GetName().Name}: version {File.ReadAllText(Constants.VersionFilePath)}");
    Console.WriteLine($"{Assembly.GetExecutingAssembly().GetName().Name}: version 1");
    
    using var scope = app.Services.CreateScope();
    var topicsScanner = scope.ServiceProvider.GetRequiredService<TopicsScanner>();
    var topicConfigService = scope.ServiceProvider.GetRequiredService<ITopicConfigService>();

    var allTopics = topicsScanner.GetTopics();
    
    await topicConfigService.SetupTopicsAsync(allTopics.Where(x=> x.Name.Contains("CorePlatform.RafEvents")).ToList());

    Environment.ExitCode = 0; // success
}
catch (Exception ex)
{
    using var scope = app.Services.CreateScope();
    var logger = app.Services.GetRequiredService<ILogger<Program>>();
    logger.LogError(ex, "Kafka setup failed.");
    Environment.ExitCode = 1; // failure
}