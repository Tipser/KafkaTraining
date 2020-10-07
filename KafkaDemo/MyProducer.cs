using System;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka;
using Newtonsoft.Json;

namespace KafkaConsumer
{
    public static class MyProducer
    {
        public static async Task ProduceLoop(ProducerConfig producerConfig, string s)
        {
            try
            {
                var producer = new ProducerBuilder<Null, byte[]>(producerConfig)
                    .Build();

                while (true)
                {
                    var msg = new MyMessage
                    {
                        Message = "Arne Anka!!" + Guid.NewGuid()
                    };

                    var json = JsonConvert.SerializeObject(msg);
                    var bytes = Encoding.UTF8.GetBytes(json);

                    try
                    {
                        producer.Produce(s, new Message<Null, byte[]>
                        {
                            Value = bytes
                        });
                    }
                    catch (ProduceException<Null,byte[]> x)
                    {
                        Console.WriteLine("Going to fast!");
                    }

                    await Task.Delay(1000);
                }
            }
            catch (Exception x)
            {
                Console.WriteLine(x);
            }
        }
    }
}