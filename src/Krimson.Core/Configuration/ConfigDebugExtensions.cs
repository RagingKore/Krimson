using Confluent.Kafka;

namespace Krimson {
    public static class ConfigDebugExtensions {
        public static ConsumerConfig EnableDebug(this ConsumerConfig config, bool enable = true, string? contexts = "consumer,cgrp,topic,fetch") {
            config.Debug = enable ? contexts ?? "consumer,cgrp,topic,fetch" : "";
            return config;
        }

        public static ProducerConfig EnableDebug(this ProducerConfig config, bool enable = true, string? contexts = "broker,topic,msg") {
            config.Debug = enable ? contexts ?? "broker,topic,msg" : "";
            return config;
        }

        public static bool IsDebugEnabled(this ConsumerConfig config) => !string.IsNullOrEmpty(config.Debug);
        public static bool IsDebugEnabled(this ProducerConfig config) => !string.IsNullOrEmpty(config.Debug);
    }
}