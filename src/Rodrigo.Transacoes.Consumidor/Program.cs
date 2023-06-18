using Microsoft.EntityFrameworkCore;
using Rodrigo.Transacoes.Consumidor;
using Rodrigo.Transacoes.Consumidor.Contexto;

public class Program
{
    public static async Task Main(string[] args)
    {

        IHost host = Host.CreateDefaultBuilder(args)
        .ConfigureServices(services =>
        {

            IConfiguration config = new ConfigurationBuilder()
               .SetBasePath(Directory.GetCurrentDirectory())
               .AddJsonFile("appsettings.json")
               .Build();

            string connectionString = config.GetConnectionString("DefaultConnection");

            var serverVersion = new MySqlServerVersion(new Version(8, 0, 31));                        
            
            services.AddDbContext<EstruturaComercialContext>(
                dbContextOptions => dbContextOptions
                    .UseMySql(connectionString, serverVersion), ServiceLifetime.Singleton
            );

            services.AddMemoryCache();

            services.AddSingleton<DataService>();

            services.AddHostedService<Worker>();
        })
        .Build();

        await host.RunAsync();
    }
}
