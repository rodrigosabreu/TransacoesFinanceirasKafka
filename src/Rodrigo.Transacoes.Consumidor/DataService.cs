using Microsoft.Extensions.Caching.Memory;
using Rodrigo.Transacoes.Consumidor.Contexto;
using Rodrigo.Transacoes.Consumidor.Entidades;

namespace Rodrigo.Transacoes.Consumidor
{
    public class DataService
    {
        private readonly IMemoryCache _cache;
        private readonly EstruturaComercialContext _context;

        public DataService(IMemoryCache cache, EstruturaComercialContext context)
        {
            _cache = cache;
            _context = context;
        }

        public void CarregarDados()
        {
            var cacheKey = "DadosDaTabela";

            if (!_cache.TryGetValue(cacheKey, out List<EstruturaComercial> dados))
            {
                dados = CarregarDadosDoBanco();

                // Armazene os dados no cache com uma opção de expiração
                var cacheOptions = new MemoryCacheEntryOptions()
                         .SetSlidingExpiration(TimeSpan.FromMinutes(10)); // Expirará após 10 minutos de inatividade

                _cache.Set(cacheKey, dados, cacheOptions);
            }
        }

        public void LerDados()
        {
            var cacheKey = "DadosDaTabela";
            if (_cache.TryGetValue(cacheKey, out List<EstruturaComercial> dados))
            {
                Console.WriteLine("Dados encontrados no cache:");                
            }
        }

        private List<EstruturaComercial>? CarregarDadosDoBanco()
        {
            return _context.estruturas.ToList();
            
        }
    }
}
