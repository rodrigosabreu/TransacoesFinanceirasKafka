using Microsoft.Extensions.Caching.Memory;
using Rodrigo.Transacoes.Consumidor.Contexto;

namespace Rodrigo.Transacoes.Consumidor
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private readonly EstruturaComercialContext _context;
        private readonly DataService _dataService;
        private readonly IMemoryCache _cache;

        public Worker(ILogger<Worker> logger, EstruturaComercialContext context, DataService dataService, IMemoryCache cache)
        {
            _logger = logger;
            _context = context;
            _dataService = dataService;
            _cache = cache;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            await Consumir(stoppingToken);           
        }        

        private async Task Consumir(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                _logger.LogInformation("Worker running at: {time}", DateTimeOffset.Now);


                _logger.LogInformation("Carregando dados da tabela: {time}", DateTimeOffset.Now);
                _dataService.CarregarDados();
                //_dataService.LerDados();
                //await Task.Delay(1000, stoppingToken);

                string bootstrapServers = "localhost:9094";
                string topic = "transacoes_financeiras";

                var consumer = new KafkaConsumer(bootstrapServers, topic, _context, _cache);
                consumer.StartConsumingTransactions();          

                Console.ReadLine();
            }
        }       
    }
}