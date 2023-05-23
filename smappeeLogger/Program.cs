

// See https://aka.ms/new-console-template for more information
using Serilog.Core;
using Serilog;

Console.WriteLine("Hello, World!");

#if DEBUG
ILogger _logger = Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Debug()
    .WriteTo.Console()
    .WriteTo.File("log-.txt", rollingInterval: RollingInterval.Day)
    .CreateLogger();
#else
ILogger _logger = Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Information()
    .WriteTo.Console()
    .WriteTo.File("log-.txt", rollingInterval: RollingInterval.Day)
    .CreateLogger();
#endif

smappeeLogger.Logger pmLogger = new smappeeLogger.Logger(_logger);

while(pmLogger.Update())
{
    Thread.Sleep(500);
}
System.Environment.Exit(1);
