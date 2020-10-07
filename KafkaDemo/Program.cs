using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry.Serdes;
using KafkaConsumer;
using Newtonsoft.Json;

var key = "ZOK6LYNQ2STVRL6H";
var secret = "vD6gzHD0hxWT4OsCUrbeC4HhR9sD65Q8Q++7+z/MvZXtFW9XwF+GByknGn1QoyNX";

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

const int commitPeriod = 5;
// Note: Specifying json serializer configuration is optional.

_ = Task.Run(() => MyProducer.ProduceLoop(producerConfig, topic));
_ = Task.Run(() => MyConsumer.ConsumeLoop(consumerConfig, topic, commitPeriod));



Console.ReadLine();


