// Inicialização do servidor orquestrador na porta 5000.
// É ele que recebe o arquivo do cliente e distribui o trabalho entre os workers.

using CompressionServer.Services;

var builder = WebApplication.CreateBuilder(args);
builder.WebHost.UseUrls("http://localhost:5000");

// Aumenta o limite de mensagem do gRPC (padrão 4MB) para caber arquivos grandes,
// tanto no recebimento do cliente quanto no envio do resultado de volta.
builder.Services.AddGrpc(options =>
{
    options.MaxReceiveMessageSize = 256 * 1024 * 1024;
    options.MaxSendMessageSize    = 256 * 1024 * 1024;
});

var app = builder.Build();

app.MapGrpcService<CompressionService>();
app.Run();
