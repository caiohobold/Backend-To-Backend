// Implementação do worker — aqui está a lógica real de compressão.
// Este serviço é chamado pelo servidor via gRPC. Ele não sabe quem o chamou nem por quê,
// nem faz ideia da existência dos outros workers: só recebe uma parte do arquivo,
// comprime com GZip e devolve o resultado. Essa independência é o que permite
// rodar cada worker num processo (ou máquina) diferente sem qualquer coordenação entre eles.

using Grpc.Core;
using Worker;
using System.IO.Compression;

namespace CompressionWorker.Services;

public class WorkerService : Worker.WorkerService.WorkerServiceBase
{
    private readonly ILogger<WorkerService> _logger;

    public WorkerService(ILogger<WorkerService> logger)
    {
        _logger = logger;
    }

    public override async Task<ChunkResponse> CompressChunk(ChunkRequest request, ServerCallContext context)
    {
        var dados = request.Data.ToByteArray();
        _logger.LogInformation("Parte {Indice}: recebidos {Tamanho} bytes", request.ChunkIndex, dados.Length);

        using var streamSaida = new MemoryStream();
        using (var gzip = new GZipStream(streamSaida, CompressionLevel.Optimal))
        {
            await gzip.WriteAsync(dados);
        }

        var comprimido = streamSaida.ToArray();
        _logger.LogInformation("Parte {Indice}: comprimida para {Tamanho} bytes", request.ChunkIndex, comprimido.Length);

        // Devolve o índice junto com os dados para o servidor remontar o arquivo
        // na ordem correta, já que as respostas dos workers podem chegar fora de ordem.
        return new ChunkResponse
        {
            CompressedData = Google.Protobuf.ByteString.CopyFrom(comprimido),
            ChunkIndex     = request.ChunkIndex,
            OriginalSize   = dados.Length,
            CompressedSize = comprimido.Length
        };
    }
}
