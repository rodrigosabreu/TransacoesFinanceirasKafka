using Confluent.Kafka;
using Newtonsoft.Json;

namespace Rodrigo.Transacoes.Produtor
{
    public class KafkaProducerTransacoes
    {
        private readonly string _bootstrapServers;
        private readonly string _topic;
        private readonly Random _random;
        private readonly string[] _transactionTypes = { "Débito", "Crédito", "PIX", "TEF" };


        public KafkaProducerTransacoes(string bootstrapServers, string topic)
        {
            _bootstrapServers = bootstrapServers;
            _topic = topic;
            _random = new Random();
        }

        public void StartProducingTransactions()
        {
            var config = new ProducerConfig { BootstrapServers = _bootstrapServers };

            using (var producer = new ProducerBuilder<Null, string>(config).Build())
            {
                try
                {
                    while (true)
                    {                        

                        var transaction = new Transaction
                        {
                            TransactionId = Guid.NewGuid().ToString(),
                            Amount = GenerateRandomAmount(),
                            CustomerId = GenerateRandomCustomerId(),
                            TransactionType = GetRandomTransactionType()
                        };

                        var json = JsonConvert.SerializeObject(transaction);
                        var message = new Message<Null, string> { Value = json };
                                                
                        var deliveryReport = producer.ProduceAsync(_topic, message).Result;

                        Console.WriteLine(json);

                        // Intervalo de 1 a 5 segundos entre as transações
                        int interval = _random.Next(1000, 1000);
                        Thread.Sleep(interval);
                    }
                }
                catch (ProduceException<Null, string> ex)
                {
                    Console.WriteLine($"Falha ao produzir mensagem: {ex.Error.Reason}");
                }
            }
        }

        private decimal GenerateRandomAmount()
        {
            // Gera um valor aleatório entre 1.00 e 1000.00
            return Math.Round((decimal)(_random.NextDouble() * 999 + 1), 2);
        }

        private string GenerateRandomCustomerId()
        {
            // Gera um ID de cliente aleatório de 6 dígitos
            return _random.Next(1, 4).ToString();
        }

        private string GetRandomTransactionType()
        {
            int index = _random.Next(0, _transactionTypes.Length);
            return _transactionTypes[index];
        }
    }

    public class Transaction
    {
        public string TransactionId { get; set; }
        public decimal Amount { get; set; }
        public string CustomerId { get; set; }
        public string TransactionType { get; set; }
    }
}
