using Microsoft.EntityFrameworkCore;
using Rodrigo.Transacoes.Consumidor.Entidades;

namespace Rodrigo.Transacoes.Consumidor.Contexto
{
    public class EstruturaComercialContext : DbContext
    {
        public DbSet<EstruturaComercial> estruturas { get; set; }

        public EstruturaComercialContext(DbContextOptions<EstruturaComercialContext> options) : base(options) {}
        //public DbSet<EstruturaComercial> EstruturaComercial { get; set; }       

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {            
            modelBuilder.Entity<EstruturaComercial>().ToTable("tbl_estruturacomercial");
            modelBuilder.Entity<EstruturaComercial>().HasKey(p => p.IdCliente);
            modelBuilder.Entity<EstruturaComercial>().Property(p => p.IdCliente).HasColumnName("id_cliente");
            modelBuilder.Entity<EstruturaComercial>().Property(p => p.NomeEspecialista).HasColumnName("nome_especialista");
            modelBuilder.Entity<EstruturaComercial>().Property(p => p.EmailEspecialista).HasColumnName("email_especialista");
        }
    }
}
