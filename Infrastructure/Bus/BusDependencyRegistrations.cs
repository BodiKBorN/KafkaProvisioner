using Infrastructure.Bus.Abstractions;
using Infrastructure.Bus.Produce;
using Infrastructure.Bus.Produce.ProducerInvokers;
using Infrastructure.Pipelines;
using Microsoft.Extensions.DependencyInjection;

namespace Infrastructure.Bus;

internal static class BusDependencyRegistrations
{
    internal static IServiceCollection RegisterInfrastructureBus(this IServiceCollection services)
    {
        services.AddScoped<IMessageDeserialize, MessageDeserialize>();

        services.AddScoped<IRegisteredEvents, RegisteredEvents>();
        services.AddScoped<IRegisteredEventHandlers, RegisteredEventHandlers>();

        services.AddSingleton<IEventPublisher, EventPublisher>()
            .Decorate<IEventPublisher, TransactionBufferedEventPublisherDecorator>();
        
        services.AddSingleton<NonKeyedProducerInvokerRegistry>();
        services.AddSingleton(typeof(NonKeyedProducerInvoker<>));
        services.AddSingleton<ProducerInvokerRegistry>();
        services.AddSingleton(typeof(ProducerInvoker<>));
        services.AddSingleton<ErrorProducerInvokerRegistry>();
        services.AddSingleton(typeof(ErrorProducerInvoker<>));
        services.AddSingleton<ITopicResolver, TopicResolver>();

        services.AddSingleton<IPlatformKafkaProducer, PlatformKafkaProducer>();

        services.AddSingleton<IEventTopicRouter, EventTopicRouter>();
        services.AddSingleton<IEventKeyMapper, EventKeyMapper>();

        return services;
    }
}