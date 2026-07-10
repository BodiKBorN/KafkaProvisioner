using App.Metrics;
using Infrastructure.Utilities;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Threading;
using System.Threading.Tasks;
using Kafka.Utilities;

namespace Infrastructure.Pipelines.Assign;

public abstract class ResilientHostedService(ILogger logger, IMetrics metrics) : IHostedService
{
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        await TaskMethods.CreateLongRunning(() => StartWithRetry(cancellationToken), cancellationToken);
        logger.LogInformation("Hosted service of type <{}> started", GetType().Name);
    }
    
    private async Task StartWithRetry(CancellationToken stoppingToken)
    {
        try
        {
            await StartImplementationAsync(stoppingToken);
        }
        catch (Exception ex)
        {
            logger.LogErrorWithMetrics(metrics, ex, "{@Error}");
            if (stoppingToken.IsCancellationRequested) return;

            await Task.Delay(TimeSpan.FromSeconds(30), stoppingToken);
            await StartWithRetry(stoppingToken);
        }
    }

    protected abstract Task StartImplementationAsync(CancellationToken cancellationToken);

    public abstract Task StopAsync(CancellationToken cancellationToken);
}