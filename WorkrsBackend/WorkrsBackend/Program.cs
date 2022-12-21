using Microsoft.Extensions.DependencyInjection;
using WorkrsBackend.Config;
using WorkrsBackend.DataHandling;
using WorkrsBackend.RabbitMQ;
using Serilog;

namespace WorkrsBackend
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var builder = WebApplication.CreateBuilder(args);
            Log.Logger = new LoggerConfiguration().MinimumLevel.Debug().WriteTo.Console().CreateLogger();
            builder.Host.UseSerilog();
            // Add services to the container.

            builder.Services.AddControllers();
            builder.Services.AddSingleton<IServerConfig, ServerConfig>();
            builder.Services.AddSingleton<IDataAccessHandler, DataAccessHandler>();
            builder.Services.AddSingleton<ISharedResourceHandler, SharedResourceHandler>();
            builder.Services.AddSingleton<IRabbitMQHandler, RabbitMQHandler>();
            builder.Services.AddHostedService<ServiceLogic>();

            // Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
            builder.Services.AddEndpointsApiExplorer();
            builder.Services.AddSwaggerGen();

            var app = builder.Build();
            // Configure the HTTP request pipeline.
            if (app.Environment.IsDevelopment())
            {
                app.UseSwagger();
                app.UseSwaggerUI();
            }

            app.UseHttpsRedirection();

            app.UseAuthorization();


            app.MapControllers();
            Log.Information("Starting app");
            app.Run();

        }
    }
}