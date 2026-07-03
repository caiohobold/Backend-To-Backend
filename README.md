## Pré-requisitos

- .NET 8 SDK ([download](https://dotnet.microsoft.com/download/dotnet/8.0))

## Como Executar

### 1. Compilar

```bash
dotnet build
```

### 2. Iniciar os Workers (cada um em um terminal separado)

**Terminal 1 — Worker na porta 5001:**
```bash
dotnet run --project src/CompressionWorker -- 5001
```

**Terminal 2 — Worker na porta 5002:**
```bash
dotnet run --project src/CompressionWorker -- 5002
```

**Terminal 3 — Worker na porta 5003:**
```bash
dotnet run --project src/CompressionWorker -- 5003
```

### 3. Iniciar o Servidor Orquestrador (novo terminal)

```bash
dotnet run --project src/CompressionServer
```

### 4. Executar o Cliente com um arquivo

```bash
dotnet run --project src/CompressionClient -- /caminho/para/seu/arquivo.txt
```

O arquivo comprimido será salvo no mesmo diretório com a extensão `.gz`.

## Exemplo de Saída

```
File: documento.txt
Size: 10,485,760 bytes
Connecting to CompressionServer at http://localhost:5000...
Uploaded 10 chunk(s). Waiting for compression result...

=== Compression Results ===
Original size:    10,485,760 bytes
Compressed size:  2,345,123 bytes
Compression ratio: 22.37%
Workers used:     3
Server time:      312.45 ms
Output file:      /caminho/para/documento.txt.gz
```

## Computação Distribuída e Paralela

Este projeto demonstra os seguintes conceitos:

- **Paralelismo real**: Os três workers processam chunks do arquivo simultaneamente usando `Task.WhenAll`, reduzindo o tempo total de compressão.
- **Comunicação entre processos**: Cada componente roda em um processo separado e se comunica via gRPC sobre HTTP/2.
- **Client streaming**: O cliente envia o arquivo em múltiplos chunks sem precisar carregar tudo na memória de uma vez.
- **Orquestração**: O servidor central coordena o trabalho distribuído, divide tarefas e agrega resultados.

## Stack Tecnológica

| Tecnologia | Uso |
|---|---|
| .NET 8 | Runtime e SDK |
| gRPC / Protocol Buffers | Comunicação entre serviços |
| GZipStream | Compressão de dados |
| Task.WhenAll | Paralelismo assíncrono |
| ASP.NET Core | Hosting dos servidores gRPC |
