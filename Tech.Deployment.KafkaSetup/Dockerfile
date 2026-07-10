# Runtime image for .NET console apps
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS base
WORKDIR /app

# Kafka CLI installer stage
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS kafka-cli
# Install Java + curl (Kafka requires Java)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    default-jre \
    && rm -rf /var/lib/apt/lists/*
# Kafka versions (easy to override)
ENV KAFKA_VERSION=3.7.0
ENV SCALA_VERSION=2.13
# Download and extract Kafka
RUN curl -fSL "https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz" -o /tmp/kafka.tgz \
    && mkdir -p /opt/kafka \
    && tar -xzf /tmp/kafka.tgz --strip-components=1 -C /opt/kafka \
    && rm /tmp/kafka.tgz

# Build image
FROM base AS build
ARG BUILD_CONFIGURATION=Release
WORKDIR /src

COPY ["Tech.Deployment.KafkaSetup/Tech.Deployment.KafkaSetup.csproj", "Tech.Deployment.KafkaSetup/"]
COPY ["Tech.Base/Tech.Infrastructure/Tech.Infrastructure.csproj", "Tech.Infrastructure/"]
COPY NuGet.Config ./
RUN dotnet restore "Tech.Deployment.KafkaSetup/Tech.Deployment.KafkaSetup.csproj"

COPY . .
WORKDIR "/src/Tech.Deployment.KafkaSetup"
RUN dotnet build "Tech.Deployment.KafkaSetup.csproj" -c $BUILD_CONFIGURATION -o /app/build

# Publish
FROM build AS publish
ARG BUILD_CONFIGURATION=Release
RUN dotnet publish "Tech.Deployment.KafkaSetup.csproj" -c $BUILD_CONFIGURATION -o /app/publish /p:UseAppHost=false

# Final image
FROM kafka-cli AS final
WORKDIR /app
# Copy published .NET output
COPY --from=publish /app/publish .
# Copy Kafka into the runtime image
COPY --from=kafka-cli /opt/kafka /opt/kafka
# Add Kafka CLI binaries to PATH
ENV PATH="/opt/kafka/bin:${PATH}"

CMD ["dotnet", "Tech.Deployment.KafkaSetup.dll"]

ENTRYPOINT ["dotnet", "Tech.Deployment.KafkaSetup.dll"]