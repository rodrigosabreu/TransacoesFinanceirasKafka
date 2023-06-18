namespace Rodrigo.Transacoes.Consumidor.Models
{
    public class Transacao
    {
        public string? TransactionId { get; set; }
        public decimal Amount { get; set; }
        public string? CustomerId { get; set; }
        public string? TransactionType { get; set; }
    }
}
