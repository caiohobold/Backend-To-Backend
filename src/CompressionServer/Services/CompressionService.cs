// Implementação do servidor orquestrador — o coração do projeto.
// Este serviço é o "backend-to-backend" propriamente dito: ele é servidor para o
// cliente (recebe o arquivo) e, ao mesmo tempo, cliente para os workers
// (envia pedaços via gRPC e coleta os resultados).

using Grpc.Core;
using Grpc.Net.Client;
using Compression;
using Worker;
using System.Diagnostics;
using Google.Protobuf;

namespace CompressionServer.Services;

public class CompressionService : Compression.CompressionService.CompressionServiceBase
{
    private readonly ILogger<CompressionService> _logger;

    // Endereços dos workers na rede. Aqui simulamos 3 workers na mesma máquina em
    // portas distintas, mas cada endereço poderia apontar para uma máquina diferente
    // (outra VM, outro computador na mesma rede local) sem mudar nada no código.
    private readonly List<string> _enderecosWorkers = new()
    {
        "http://localhost:5001",
        "http://localhost:5002",
        "http://localhost:5003",
    };

    public CompressionService(ILogger<CompressionService> logger)
    {
        _logger = logger;
    }

    // Método principal, chamado pelo cliente via gRPC com client streaming.
    public override async Task<CompressResponse> CompressFile(
        IAsyncStreamReader<UploadRequest> requestStream,
        ServerCallContext context)
    {
        var cronometro = Stopwatch.StartNew();
        var pedacosRecebidos = new List<byte[]>();

        await foreach (var request in requestStream.ReadAllAsync())
        {
            pedacosRecebidos.Add(request.Chunk.ToByteArray());
        }

        var tamanhoTotal = pedacosRecebidos.Sum(p => p.Length);
        var arquivoCompleto = new byte[tamanhoTotal];
        var offset = 0;
        foreach (var pedaco in pedacosRecebidos)
        {
            Buffer.BlockCopy(pedaco, 0, arquivoCompleto, offset, pedaco.Length);
            offset += pedaco.Length;
        }

        _logger.LogInformation("Arquivo recebido: {Tamanho} bytes. Dividindo em {N} partes.", tamanhoTotal, _enderecosWorkers.Count);

        // Divide o arquivo em N partes iguais, uma para cada worker.
        var partes = DividirEmPartes(arquivoCompleto, _enderecosWorkers.Count);

        _logger.LogInformation("Iniciando compressão paralela com {N} workers.", _enderecosWorkers.Count);

        // Dispara uma chamada gRPC para cada worker ao mesmo tempo e só continua quando
        // todas tiverem terminado. É aqui que acontece o paralelismo distribuído: cada
        // worker comprime sua parte de forma independente, em paralelo com os demais.
        var tarefas = partes.Select((parte, indice) => EnviarParaWorker(parte, indice, _enderecosWorkers[indice]));
        var respostas = await Task.WhenAll(tarefas);

        // As respostas podem chegar fora de ordem; reordena pelo índice original antes
        // de concatenar, para remontar o arquivo comprimido corretamente.
        var respostasOrdenadas = respostas.OrderBy(r => r.ChunkIndex).ToList();
        var partesComprimidas = respostasOrdenadas.Select(r => r.CompressedData.ToByteArray()).ToArray();
        var tamanhoComprimidoTotal = partesComprimidas.Sum(p => p.Length);
        var dadosComprimidos = new byte[tamanhoComprimidoTotal];
        var offsetEscrita = 0;
        foreach (var parte in partesComprimidas)
        {
            Buffer.BlockCopy(parte, 0, dadosComprimidos, offsetEscrita, parte.Length);
            offsetEscrita += parte.Length;
        }

        cronometro.Stop();
        var taxaCompressao = tamanhoTotal > 0 ? (double)tamanhoComprimidoTotal / tamanhoTotal * 100 : 0;

        _logger.LogInformation("Compressão concluída. Original: {Original} bytes, Comprimido: {Comprimido} bytes, Taxa: {Taxa:F2}%, Tempo: {Tempo}ms",
            tamanhoTotal, tamanhoComprimidoTotal, taxaCompressao, cronometro.Elapsed.TotalMilliseconds);

        return new CompressResponse
        {
            CompressedData      = ByteString.CopyFrom(dadosComprimidos),
            OriginalSize        = tamanhoTotal,
            CompressedSize      = tamanhoComprimidoTotal,
            CompressionRatio    = taxaCompressao,
            WorkersUsed         = _enderecosWorkers.Count,
            ElapsedMilliseconds = cronometro.Elapsed.TotalMilliseconds
        };
    }

    // Abre um canal gRPC com um worker específico e envia sua parte para compressão.
    private async Task<ChunkResponse> EnviarParaWorker(byte[] dados, int indice, string endereco)
    {
        var canal = GrpcChannel.ForAddress(endereco);
        var cliente = new WorkerService.WorkerServiceClient(canal);

        _logger.LogInformation("Enviando parte {Indice} ({Tamanho} bytes) para o worker em {Endereco}", indice, dados.Length, endereco);

        var resposta = await cliente.CompressChunkAsync(new ChunkRequest
        {
            Data       = ByteString.CopyFrom(dados),
            ChunkIndex = indice
        });

        _logger.LogInformation("Worker em {Endereco} respondeu a parte {Indice}.", endereco, indice);
        return resposta;
    }

    // Divide o array de bytes em N partes de tamanho aproximadamente igual.
    private static List<byte[]> DividirEmPartes(byte[] dados, int quantidade)
    {
        var partes = new List<byte[]>();
        var tamanhoPorParte = (int)Math.Ceiling((double)dados.Length / quantidade);

        for (int i = 0; i < quantidade; i++)
        {
            var inicio = i * tamanhoPorParte;
            if (inicio >= dados.Length) break;
            var tamanho = Math.Min(tamanhoPorParte, dados.Length - inicio);
            var parte = new byte[tamanho];
            Buffer.BlockCopy(dados, inicio, parte, 0, tamanho);
            partes.Add(parte);
        }

        return partes;
    }
}
