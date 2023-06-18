using Confluent.Kafka;
using Microsoft.Extensions.Caching.Memory;
using Newtonsoft.Json;
using Rodrigo.Transacoes.Consumidor.Contexto;
using Rodrigo.Transacoes.Consumidor.Entidades;
using Rodrigo.Transacoes.Consumidor.Models;
using System.Diagnostics;

namespace Rodrigo.Transacoes.Consumidor
{
    public class KafkaConsumer
    {
        private readonly string _bootstrapServers;
        private readonly string _topic;
        private readonly EstruturaComercialContext _context;
        private IMemoryCache _cache;

        public KafkaConsumer(string bootstrapServers, string topic, EstruturaComercialContext context, IMemoryCache cache)
        {
            _bootstrapServers = bootstrapServers;
            _topic = topic;
            _context = context;
            _cache = cache;
        }

        public async Task StartConsumingTransactions()
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = _bootstrapServers,
                GroupId = "transaction-consumer-group",
                AutoOffsetReset = AutoOffsetReset.Earliest, // Consumir a partir do início do tópico
                EnableAutoCommit = false
            };

            using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
            {
                consumer.Subscribe(_topic);

                try
                {
                    while (true)
                    {
                        var consumeResult = consumer.Consume();
                        // Remove manualmente a mensagem do tópico
                        consumer.Commit(consumeResult);                       

                        if (consumeResult != null && consumeResult.Message.Value != null)
                        {
                            var transaction = JsonConvert.DeserializeObject<Transacao>(consumeResult.Message.Value);                            
                            //await ConsultarEspecialistadaTabela();
                            await ConsultarEspecialistadaMemoria(transaction);
                        }
                    }
                }
                catch (ConsumeException ex)
                {
                    Console.WriteLine($"Falha ao consumir mensagem: {ex.Error.Reason}");
                }
                finally
                {
                    consumer.Close();
                }
            }
        }

        private async Task ConsultarEspecialistadaTabela()
        {
            var teste2 = _context.estruturas
                            .Where(p => p.IdCliente == 1)
                            .Select(p => p.NomeEspecialista)
                            .FirstOrDefault();

            Console.WriteLine(teste2);
        }

        private async Task ConsultarEspecialistadaMemoria(Transacao t)
        {
            var stopwatch = Stopwatch.StartNew();
            var cacheKey = "DadosDaTabela";
            
            if (_cache.TryGetValue(cacheKey, out List<EstruturaComercial> dados))
            {
                int id;
                if (int.TryParse(t.CustomerId, out id))
                {                    
                    var teste = dados
                            .Where(p => p.IdCliente == id)
                            .Select(p => new { p.IdCliente, p.NomeEspecialista, p.EmailEspecialista })
                            .FirstOrDefault();

                    if(teste != null)
                        Console.WriteLine("Envia mensagem para SQS: " + teste);
                    else
                        Console.WriteLine("Cliente não é investitor: " + t.CustomerId);
                }
            }
            stopwatch.Stop(); // Parar a contagem do tempo
            Console.WriteLine($"Tempo de consulta no cache: {stopwatch.ElapsedMilliseconds}ms");


        }       
    }

 
}
