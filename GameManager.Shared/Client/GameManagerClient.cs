using GameManager.Shared.Api;
using GameManager.Shared.Ipc;
using System.Collections.Concurrent;
using System.IO.Pipes;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace GameManager.Shared.Client
{
    public sealed class GameManagerClient : IGameManager
    {
        private const string PipeName = "GameManager";

        private readonly NamedPipeClientStream _pipe;

        private StreamReader? _reader;
        private StreamWriter? _writer;

        private readonly SemaphoreSlim _writeLock =
            new(1, 1);

        private long _nextRequestId;

        private CancellationTokenSource? _receiveCts;
        private Task? _receiveTask;

        private readonly ConcurrentDictionary<
            long,
            TaskCompletionSource<IpcResponse>>
            _pendingRequests = new();

        private static readonly JsonSerializerOptions JsonOptions = new()
        {
            Converters =
            {
                new JsonStringEnumConverter()
            }
        };

        public GameManagerClient()
        {
            _pipe = new NamedPipeClientStream(
                ".",
                PipeName,
                PipeDirection.InOut,
                PipeOptions.Asynchronous);
        }

        public async Task ConnectAsync(
            CancellationToken cancellationToken = default)
        {
            if (_pipe.IsConnected)
            {
                return;
            }

            Console.WriteLine(
                "Connecting to GameManager...");

            await _pipe.ConnectAsync(
                cancellationToken);

            Console.WriteLine(
                "Pipe connected.");

            _reader = new StreamReader(
                _pipe,
                Encoding.UTF8,
                detectEncodingFromByteOrderMarks: false,
                bufferSize: 1024,
                leaveOpen: true);

            _writer = new StreamWriter(
                _pipe,
                new UTF8Encoding(false),
                bufferSize: 1024,
                leaveOpen: true)
            {
                AutoFlush = true
            };

            _receiveCts =
                CancellationTokenSource.CreateLinkedTokenSource(
                    cancellationToken);

            _receiveTask =
                ReceiveLoopAsync(
                    _receiveCts.Token);

            Console.WriteLine(
                "Connected to GameManager.");
        }

        public async Task<T?> GetAsync<T>(
            string resource,
            CancellationToken cancellationToken = default)
        {
            IpcResponse response =
                await SendRequestAsync(
                    new IpcRequest
                    {
                        Operation =
                            IpcOperation.GetResource,
                        Resource = resource
                    },
                    cancellationToken);

            if (response.Value is null)
            {
                return default;
            }

            if (response.Value is JsonElement jsonElement)
            {
                return jsonElement.Deserialize<T>(
                    JsonOptions);
            }

            return (T)response.Value;
        }

        public async Task PingAsync(
            CancellationToken cancellationToken = default)
        {
            IpcResponse response =
                await SendRequestAsync(
                    new IpcRequest
                    {
                        Operation = IpcOperation.Ping
                    },
                    cancellationToken);

            if (!response.Success)
            {
                throw new GameManagerIpcException(
                    response.Error?.Code ??
                    IpcErrorCodes.InternalError,
                    response.Error?.Message ??
                    "Ping failed.");
            }
        }

        public async Task SetAsync<T>(
            string resource,
            T value,
            CancellationToken cancellationToken = default)
        {
            await SendRequestAsync(
                new IpcRequest
                {
                    Operation =
                        IpcOperation.SetResource,
                    Resource = resource,
                    Value = value
                },
                cancellationToken);

            Console.WriteLine(
                $"SET request for '{resource}' succeeded.");
        }

        private async Task<IpcResponse> SendRequestAsync(
            IpcRequest request,
            CancellationToken cancellationToken)
        {
            if (!_pipe.IsConnected ||
                _reader is null ||
                _writer is null)
            {
                throw new InvalidOperationException(
                    "GameManager client is not connected.");
            }

            long requestId =
                Interlocked.Increment(
                    ref _nextRequestId);

            request.RequestId =
                requestId;

            TaskCompletionSource<IpcResponse>
                responseSource =
                    new(
                        TaskCreationOptions
                            .RunContinuationsAsynchronously);

            if (!_pendingRequests.TryAdd(
                    requestId,
                    responseSource))
            {
                throw new InvalidOperationException(
                    $"Failed to register request {requestId}.");
            }

            try
            {
                string requestJson =
                    JsonSerializer.Serialize(
                        request,
                        JsonOptions);

                // Logging the sent IPC request can be useful for debugging, but it may produce a lot of output in production. Uncomment the following line if you want to log sent requests.
                //Console.WriteLine($"Sending IPC request: {requestJson}");

                await WriteAsync(
                    requestJson,
                    cancellationToken);

                IpcResponse response =
                    await responseSource.Task.WaitAsync(
                        cancellationToken);

                if (!response.Success)
                {
                    throw new GameManagerIpcException(
                        response.Error?.Code ??
                        IpcErrorCodes.InternalError,
                        response.Error?.Message ??
                        "GameManager request failed.");
                }

                return response;
            }
            finally
            {
                _pendingRequests.TryRemove(
                    requestId,
                    out _);
            }
        }

        private async Task WriteAsync(
            string message,
            CancellationToken cancellationToken)
        {
            if (_writer is null)
            {
                throw new InvalidOperationException(
                    "GameManager client is not connected.");
            }

            await _writeLock.WaitAsync(
                cancellationToken);

            try
            {
                await _writer.WriteLineAsync(
                    message);
            }
            finally
            {
                _writeLock.Release();
            }
        }

        private async Task ReceiveLoopAsync(
            CancellationToken cancellationToken)
        {
            if (_reader is null)
            {
                return;
            }

            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    string? json =
                        await _reader.ReadLineAsync(
                            cancellationToken);

                    if (json is null)
                    {
                        Console.WriteLine(
                            "GameManager disconnected.");

                        FailPendingRequests(
                            new IOException(
                                "GameManager disconnected."));

                        break;
                    }

                    if (string.IsNullOrWhiteSpace(json))
                    {
                        continue;
                    }

                    // Logging the received IPC message can be useful for debugging, but it may produce a lot of output in production. Uncomment the following line if you want to log received messages.
                    //Console.WriteLine($"Received IPC message: {json}");

                    if (IsChildRequest(json))
                    {
                        await HandleChildRequestAsync(
                            json,
                            cancellationToken);

                        continue;
                    }

                    IpcResponse? response =
                        JsonSerializer.Deserialize<IpcResponse>(
                            json,
                            JsonOptions);

                    if (response is null)
                    {
                        Console.WriteLine(
                            "Received invalid IPC response.");

                        continue;
                    }

                    if (_pendingRequests.TryRemove(
                            response.RequestId,
                            out TaskCompletionSource<IpcResponse>?
                                responseSource))
                    {
                        responseSource.TrySetResult(
                            response);
                    }
                    else
                    {
                        Console.WriteLine(
                            $"Received response for unknown request " +
                            $"{response.RequestId}.");
                    }
                }
            }
            catch (OperationCanceledException)
                when (cancellationToken.IsCancellationRequested)
            {
            }
            catch (IOException exception)
            {
                Console.WriteLine(
                    $"IPC receive loop disconnected: " +
                    $"{exception.Message}");

                FailPendingRequests(exception);
            }
            catch (Exception exception)
            {
                Console.WriteLine(
                    "IPC receive loop failed:");

                Console.WriteLine(exception);

                FailPendingRequests(exception);
            }
        }

        private static bool IsChildRequest(
            string json)
        {
            try
            {
                using JsonDocument document =
                    JsonDocument.Parse(json);

                JsonElement root =
                    document.RootElement;

                return root.TryGetProperty(
                           "Operation",
                           out _)
                       ||
                       root.TryGetProperty(
                           "operation",
                           out _);
            }
            catch (JsonException)
            {
                return false;
            }
        }

        private async Task HandleChildRequestAsync(
            string json,
            CancellationToken cancellationToken)
        {
            IpcChildRequest? request =
                JsonSerializer.Deserialize<IpcChildRequest>(
                    json,
                    JsonOptions);

            if (request is null)
            {
                return;
            }

            // Useful for debugging, but may produce a lot of output in production. Uncomment the following line if you want to log received child requests.
            //Console.WriteLine($"GameManager request received: " + $"{request.Operation}");

            IpcResponse response;

            switch (request.Operation)
            {
                case IpcOperation.GetStatus:
                    response = new IpcResponse
                    {
                        RequestId =
                            request.RequestId,
                        Success = true,
                        Value = "Ready"
                    };

                    break;

                default:
                    response = new IpcResponse
                    {
                        RequestId =
                            request.RequestId,
                        Success = false,
                        Error = new IpcError
                        {
                            Code =
                                IpcErrorCodes.UnknownOperation,
                            Message =
                                $"Unknown operation " +
                                $"'{request.Operation}'."
                        }
                    };

                    break;
            }

            await WriteAsync(
                JsonSerializer.Serialize(
                    response,
                    JsonOptions),
                cancellationToken);
        }

        private void FailPendingRequests(
            Exception exception)
        {
            foreach (
                TaskCompletionSource<IpcResponse> source
                in _pendingRequests.Values)
            {
                source.TrySetException(
                    exception);
            }

            _pendingRequests.Clear();
        }

        public async ValueTask DisposeAsync()
        {
            if (_receiveCts is not null)
            {
                _receiveCts.Cancel();
            }

            if (_receiveTask is not null)
            {
                try
                {
                    await _receiveTask;
                }
                catch (OperationCanceledException)
                {
                }
            }

            _reader?.Dispose();

            if (_writer is not null)
            {
                await _writer.DisposeAsync();
            }

            _receiveCts?.Dispose();

            _pipe.Dispose();
            _writeLock.Dispose();
        }

        public async Task RunHeartbeatAsync(
            TimeSpan interval,
            CancellationToken cancellationToken = default)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    await PingAsync(
                        cancellationToken);

                    Console.WriteLine(
                        "GameManager heartbeat: OK");
                }
                catch (OperationCanceledException)
                    when (cancellationToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception exception)
                {
                    Console.WriteLine(
                        $"GameManager heartbeat failed: " +
                        $"{exception.Message}");

                    throw;
                }

                await Task.Delay(
                    interval,
                    cancellationToken);
            }
        }

        public async Task RegisterAsync(
    string name,
    CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrWhiteSpace(name))
            {
                throw new ArgumentException(
                    "Child name cannot be empty.",
                    nameof(name));
            }

            IpcRequest request = new()
            {
                RequestId =
                    Interlocked.Increment(
                        ref _nextRequestId),

                Operation =
                    IpcOperation.Register,

                Value =
                    name
            };

            IpcResponse response =
                await SendRequestAsync(
                    request,
                    cancellationToken);

            if (!response.Success)
            {
                throw new GameManagerIpcException(
                    response.Error?.Code ?? "UNKNOWN_ERROR",
                    response.Error?.Message ??
                    "Child registration failed.");
            }
        }
    }
}