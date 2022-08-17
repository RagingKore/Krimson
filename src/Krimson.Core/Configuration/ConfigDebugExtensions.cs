using Confluent.Kafka;

namespace Krimson; 

public static class ConfigDebugExtensions {
    public static ConsumerConfig EnableDebug(this ConsumerConfig config, bool enable = true, string? contexts = "consumer,cgrp,topic,fetch") {
        if (enable)
            config.Debug = contexts ?? "consumer,cgrp,topic,fetch";

        return config;
    }

    public static ProducerConfig EnableDebug(this ProducerConfig config, bool enable = true, string? contexts = "broker,topic,msg") {
        if (enable)
            config.Debug = contexts ?? "broker,topic,msg";

        return config;
    }

    public static bool IsDebugEnabled(this ClientConfig config) => !string.IsNullOrEmpty(config.Debug);
}