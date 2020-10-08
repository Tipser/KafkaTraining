using System;
using System.Threading.Tasks;
using Confluent.Kafka;
using KafkaConsumer;

public class Program
{
    public static void Main(string[] args)
    {
        var key = "";
        var secret = "";
        var topic = "mytopic";
        var bootstrapServers = "pkc-4r297.europe-west1.gcp.confluent.cloud:9092";

        var consumerConfig = new ConsumerConfig
        {
            SaslMechanism = SaslMechanism.Plain,
            SecurityProtocol = SecurityProtocol.SaslSsl,
            SaslUsername = key,
            SaslPassword = secret,
            BootstrapServers = bootstrapServers,
            GroupId = "csharp-consumer",
            EnableAutoCommit = false,
            StatisticsIntervalMs = 5000,
            SessionTimeoutMs = 6000,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnablePartitionEof = false
        };

        var producerConfig = new ProducerConfig
        {
            SaslMechanism = SaslMechanism.Plain,
            SecurityProtocol = SecurityProtocol.SaslSsl,
            SaslUsername = key,
            SaslPassword = secret,
            BootstrapServers = bootstrapServers,
        };

        const int commitPeriod = 1000;
        _ = Task.Run(() => MyProducer.ProduceLoop(producerConfig, topic));
        _ = Task.Run(() => MyConsumer.ConsumeLoop(consumerConfig, topic, commitPeriod));
        Console.ReadLine();
    }
}