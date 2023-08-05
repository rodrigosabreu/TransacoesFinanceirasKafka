namespace Rodrigo.Transacoes.Produtor;

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;        

    public Worker(ILogger<Worker> logger)
    {
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Worker start at: {time}", DateTimeOffset.Now);

        //await Task.WhenAll(ProduzirTransacoes(stoppingToken), ProduzirTime(stoppingToken));

        Task metodo1Task = Task.Run(() => ProduzirTransacoes(stoppingToken));
        Task metodo2Task = Task.Run(() => ProduzirTime(stoppingToken));

    }

    private void ProduzirTransacoes(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Produzindo Transacoes: {time}", DateTimeOffset.Now);
        string bootstrapServers = "localhost:9094";
        string topic = "transacoes_financeiras";

        KafkaProducerTransacoes producer = new KafkaProducerTransacoes(bootstrapServers, topic);

        producer.StartProducingTransactions();
        
    }

    private void ProduzirTime(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Produzindo Times: {time}", DateTimeOffset.Now);
        string bootstrapServers = "localhost:9094";
        string topic = "movimentacoes_time";

        KafkaProducerTimeAtendimentoPrivate producer = new KafkaProducerTimeAtendimentoPrivate(bootstrapServers, topic);

        producer.StartProducingTimes();        
    }

}