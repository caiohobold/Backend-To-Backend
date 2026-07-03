// Cliente: lê um arquivo do disco, envia ao servidor via gRPC em pedaços de 1MB,
// aguarda o resultado e salva o arquivo comprimido.

using Grpc.Net.Client;
using Compression;
using Google.Protobuf;

string caminhoArquivo;

if (args.Length > 0)
{
    caminhoArquivo = args[0];
}
else
{
    Console.Write("Informe o caminho do arquivo: ");
    caminhoArquivo = Console.ReadLine()?.Trim() ?? string.Empty;
}

if (!File.Exists(caminhoArquivo))
{
    Console.Error.WriteLine($"Arquivo não encontrado: {caminhoArquivo}");
    return 1;
}

var infoArquivo = new FileInfo(caminhoArquivo);
Console.WriteLine($"Arquivo: {infoArquivo.Name}");
Console.WriteLine($"Tamanho: {infoArquivo.Length:N0} bytes");

// O limite padrão do gRPC é 4MB; aumentamos aqui também porque a resposta
// devolve o arquivo comprimido inteiro em uma única mensagem.
var opcoesCanal = new GrpcChannelOptions
{
    MaxReceiveMessageSize = 256 * 1024 * 1024,
    MaxSendMessageSize    = 256 * 1024 * 1024
};

using var canal = GrpcChannel.ForAddress("http://localhost:5000", opcoesCanal);
var cliente = new CompressionService.CompressionServiceClient(canal);

Console.WriteLine("Conectando ao servidor em http://localhost:5000...");

// Abre o stream de envio: a partir daqui é possível mandar vários pedaços
// do arquivo antes de receber qualquer resposta (client streaming).
using var chamada = cliente.CompressFile();

// Lê o arquivo em pedaços de 1MB para não carregar tudo na memória de uma vez.
const int tamanhoDoPedaco = 1024 * 1024;
var buffer = new byte[tamanhoDoPedaco];
using var arquivo = File.OpenRead(caminhoArquivo);

int bytesLidos;
int quantidadePedacos = 0;
while ((bytesLidos = await arquivo.ReadAsync(buffer)) > 0)
{
    await chamada.RequestStream.WriteAsync(new UploadRequest
    {
        Chunk = ByteString.CopyFrom(buffer, 0, bytesLidos)
    });
    quantidadePedacos++;
}

// Sinaliza ao servidor que o envio terminou; só então ele começa a distribuir
// o trabalho entre os workers.
await chamada.RequestStream.CompleteAsync();

Console.WriteLine($"{quantidadePedacos} pedaço(s) enviado(s). Aguardando o resultado da compressão...");

// A resposta só chega depois que todos os workers tiverem terminado.
var resposta = await chamada.ResponseAsync;

var caminhoSaida = Path.Combine(infoArquivo.DirectoryName!, infoArquivo.Name + ".gz");
await File.WriteAllBytesAsync(caminhoSaida, resposta.CompressedData.ToByteArray());

Console.WriteLine();
Console.WriteLine("=== Resultado da Compressão ===");
Console.WriteLine($"Tamanho original:    {resposta.OriginalSize:N0} bytes");
Console.WriteLine($"Tamanho comprimido:  {resposta.CompressedSize:N0} bytes");
Console.WriteLine($"Taxa de compressão:  {resposta.CompressionRatio:F2}%");
Console.WriteLine($"Workers utilizados:  {resposta.WorkersUsed}");
Console.WriteLine($"Tempo no servidor:   {resposta.ElapsedMilliseconds:F2} ms");
Console.WriteLine($"Arquivo de saída:    {caminhoSaida}");

return 0;
