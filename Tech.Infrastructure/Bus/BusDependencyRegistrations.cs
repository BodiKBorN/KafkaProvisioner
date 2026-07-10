using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using App.Metrics;
using Confluent.Kafka;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Tech.Infrastructure.Options;
using Tech.Kafka.Clients;
using Tech.Kafka.Clients.Admin;
using Tech.Kafka.Clients.Producer;
using Tech.Kafka.Models;
using Tech.Social.Infrastructure.Bus.Abstractions;
using Tech.Social.Infrastructure.Bus.Produce;
using Tech.Social.Infrastructure.Bus.ProducerInvokers;
using Tech.Social.Infrastructure.Pipelines;

namespace Tech.Social.Infrastructure.Bus;

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