using Confluent.Kafka;
using Newtonsoft.Json;

namespace Rodrigo.Transacoes.Produtor
{
    public class KafkaProducerTimeAtendimentoPrivate
    {
        private readonly string _bootstrapServers;
        private readonly string _topic;
        private readonly Random _random;

        private readonly string[] _idClientes = { "1_cli", "2_cli", "3_cli" };
        private readonly string[] _bankers = { "1_ban", "2_ban", null};
        private readonly string[] _gerentes = { "3_ger", "4_ger", null};
        private readonly string[] _especialistas = { "5_espec", "6_esp", null};

        public KafkaProducerTimeAtendimentoPrivate(string bootstrapServers, string topic)
        {
            _bootstrapServers = bootstrapServers;
            _topic = topic;
            _random = new Random();
        }

        public void StartProducingTimes()
        {
            var config = new ProducerConfig { BootstrapServers = _bootstrapServers };

            using (var producer = new ProducerBuilder<Null, string>(config).Build())
            {
                try
                {
                    while (true)
                    {                        

                        var time = new TimeAtendimentoPrivate
                        {
                            Cliente = GetRandomCliente(),
                            TimeAtendimento = new Time
                            {
                                Banker = GetRandomBanker(),
                                Gerente = GetRandomGerente(),
                                Especialista = GetRandomEspecialista()
                            }
                        };

                        var json = JsonConvert.SerializeObject(time);
                        var message = new Message<Null, string> { Value = json };
                                                
                        var deliveryReport = producer.ProduceAsync(_topic, message).Result;

                        Console.WriteLine(json);

                        // Intervalo de 1 a 5 segundos entre as transações
                        int interval = _random.Next(3000, 3000);
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

        private string GetRandomCliente()
        {
            int index = _random.Next(0, _idClientes.Length);
            return _idClientes[index];
        }

        private string GetRandomBanker()
        {
            int index = _random.Next(0, _bankers.Length);
            return _bankers[index];
        }

        private string GetRandomGerente()
        {
            int index = _random.Next(0, _gerentes.Length);
            return _gerentes[index];
        }

        private string GetRandomEspecialista()
        {
            int index = _random.Next(0, _especialistas.Length);
            return _especialistas[index];
        }
    }

    public class TimeAtendimentoPrivate
    {
        public string Cliente { get; set; }
        public Time TimeAtendimento { get; set; }
    }

    public class Time
    {
        public string Banker { get; set; }
        public string Gerente { get; set; }
        public string Especialista { get; set; }
    }
}
