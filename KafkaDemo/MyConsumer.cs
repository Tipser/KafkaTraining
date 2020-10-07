using System;
using System.Text;
using Confluent.Kafka;
using Newtonsoft.Json;

namespace KafkaConsumer
{
    public static class MyConsumer
    {
        public static void ConsumeLoop(ConsumerConfig consumerConfig, string s, int i)
        {
            using var consumer = new ConsumerBuilder<Ignore, byte[]>(consumerConfig)
                .Build();

            consumer.Subscribe(s);

            while (true)
            {
                try
                {
                    var consumeResult = consumer.Consume();

                    if (consumeResult.IsPartitionEOF)
                    {
                        continue;
                    }

                    var bytes = consumeResult.Message.Value;
                    var json = Encoding.UTF8.GetString(bytes);
                    var msg = JsonConvert.DeserializeObject<MyMessage>(json);
                    
                    Console.WriteLine(msg.Message);

                    // Console.WriteLine(
                    //     $"Received message at {consumeResult.TopicPartitionOffset}: {consumeResult.Message.Value}");

                    if (consumeResult.Offset % i == 0)
                    {
                        consumer.Commit(consumeResult);
                    }
                }
                catch (KafkaException e)
                {
                    Console.WriteLine($"Commit error: {e.Error.Reason}" + e);
                }
                catch (OperationCanceledException)
                {
                    Console.WriteLine("Closing consumer.");
                    consumer.Close();
                }
            }
        }
    }
}