// Inicialização de um worker — sobe um servidor gRPC na porta informada por argumento.
// Exemplo: dotnet run --project src/CompressionWorker -- 5001
// Cada instância rodando em uma porta (ou máquina) diferente é um worker independente.

using CompressionWorker.Services;

var porta = args.Length > 0 ? args[0] : "5001";

var builder = WebApplication.CreateBuilder(args);
builder.WebHost.UseUrls($"http://localhost:{porta}");

builder.Services.AddGrpc(options =>
{
    options.MaxReceiveMessageSize = 256 * 1024 * 1024;
    options.MaxSendMessageSize    = 256 * 1024 * 1024;
});

var app = builder.Build();

app.MapGrpcService<WorkerService>();
app.Run();
