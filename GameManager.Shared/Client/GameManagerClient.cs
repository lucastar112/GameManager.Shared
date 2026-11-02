using GameManager.Shared.Api;
using GameManager.Shared.Ipc;
using System.Collections.Concurrent;
using System.IO.Pipes;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Channels;
using static System.Net.Mime.MediaTypeNames;

namespace GameManager.Shared.Client;

public sealed class GameManagerClient : IGameManager
{
    // ---------------------------------------------------------
    // Pipe
    // ---------------------------------------------------------

    private const string PipeName = "GameManager";

    private readonly NamedPipeClientStream _pipe;

    private StreamReader? _reader;
    private StreamWriter? _writer;

    // ---------------------------------------------------------
    // Synchronization
    // ---------------------------------------------------------

    private readonly SemaphoreSlim _writeLock = new(1, 1);

    // ---------------------------------------------------------
    // Requests
    // ---------------------------------------------------------

    private long _nextRequestId;

    private readonly ConcurrentDictionary<
        long,
        TaskCompletionSource<IpcResponse>>
        _pendingRequests = new();

    // ---------------------------------------------------------
    // Receive loop
    // ---------------------------------------------------------

    private CancellationTokenSource? _receiveCts;
    private Task? _receiveTask;

    // ---------------------------------------------------------
    // JSON
    // ---------------------------------------------------------

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        Converters =
        {
            new JsonStringEnumConverter()
        }
    };

    // ---------------------------------------------------------
    // Constructor
    // ---------------------------------------------------------

    /// <summary>
    /// Prepares the pipe to be used for the connection
    /// with the GameManager.
    /// </summary>
    public GameManagerClient()
    {
        _pipe = new NamedPipeClientStream(
            ".",
            PipeName,
            PipeDirection.InOut,
            PipeOptions.Asynchronous);
    }

    // ---------------------------------------------------------
    // Connection
    // ---------------------------------------------------------

    /// <summary>
    /// Tries to connect to the GameManager through a pipe.
    /// </summary>
    public async Task ConnectAsync(
        CancellationToken cancellationToken = default)
    {
        if (_pipe.IsConnected)
            return;

        Console.WriteLine(
            "Connecting to GameManager...");

        await _pipe.ConnectAsync(cancellationToken);

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
            ReceiveLoopAsync(_receiveCts.Token);

        Console.WriteLine(
            "Connected to GameManager.");
    }

    // ---------------------------------------------------------
    // Get
    // ---------------------------------------------------------

    /// <summary>
    /// Asks the GameManager to send a value from the game's RAM
    /// located at the given resource.
    /// </summary>
    public async Task<T?> GetAsync<T>(
        string resource,
        int? playerId = null,
        CancellationToken cancellationToken = default)
    {
        IpcResponse response =
            await SendRequestAsync(
                new IpcRequest
                {
                    MessageType = IpcMessageType.Request,
                    Operation = IpcOperation.GetResource,
                    Resource = resource,
                    PlayerId = playerId
                },
                cancellationToken);

        if (response.Value is null)
            return default;

        if (response.Value is JsonElement jsonElement)
        {
            return jsonElement.Deserialize<T>(
                JsonOptions);
        }

        return (T)response.Value;
    }

    // ---------------------------------------------------------
    // Set
    // ---------------------------------------------------------

    /// <summary>
    /// Asks the GameManager to modify a value from the game's RAM.
    /// </summary>
    public async Task SetAsync<T>(
        string resource,
        T value,
        CancellationToken cancellationToken = default)
    {
        await SendRequestAsync(
            new IpcRequest
            {
                MessageType = IpcMessageType.Request,
                Operation = IpcOperation.SetResource,
                Resource = resource,
                Value = value
            },
            cancellationToken);

        Console.WriteLine(
            $"SET request for '{resource}' succeeded.");
    }

    // ---------------------------------------------------------
    // Send request
    // ---------------------------------------------------------

    /// <summary>
    /// Sends a new request to the GameManager and waits for its
    /// matching response.
    /// </summary>
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

        request.RequestId = requestId;

        Console.WriteLine(
    $"[IPC CLIENT] Sending request ID={requestId}, " +
    $"Operation={request.Operation}, Resource={request.Resource}");

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

            Console.WriteLine(
    $"[IPC DEBUG] Sending request ID={requestId}, " +
    $"Operation={request.Operation}, Resource={request.Resource ?? "<none>"}");

            Console.WriteLine(
    $"[CLIENT SEND] ID={request.RequestId}, " +
    $"Operation={request.Operation}, Resource={request.Resource}");

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

    // ---------------------------------------------------------
    // Write
    // ---------------------------------------------------------

    /// <summary>
    /// Sends a message to the GameManager.
    /// </summary>
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

    // ---------------------------------------------------------
    // Receive loop
    // ---------------------------------------------------------

    /// <summary>
    /// Continuously receives and dispatches IPC messages.
    /// </summary>
    private async Task ReceiveLoopAsync(
        CancellationToken cancellationToken)
    {
        if (_reader is null)
            return;

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                string? json =
                    await _reader.ReadLineAsync(
                        cancellationToken);
                Console.WriteLine(
    $"[SERVER RECEIVE] JSON={json}");

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
                    continue;

                Console.WriteLine(
                    $"Received IPC message: {json}");

                // -------------------------------------------------
                // Determine message type
                // -------------------------------------------------

                IpcMessageType? messageType =
                    GetMessageType(json);

                if (messageType is null)
                {
                    Console.WriteLine(
                        "Received IPC message without a valid MessageType.");

                    continue;
                }

                // -------------------------------------------------
                // Dispatch message
                // -------------------------------------------------

                switch (messageType.Value)
                {
                    case IpcMessageType.Response:

                        HandleResponse(json);

                        break;

                    case IpcMessageType.ChildRequest:
                        await HandleChildRequestSafelyAsync(
                            json,
                            cancellationToken);
                        break;

                    case IpcMessageType.Request:

                        Console.WriteLine(
                            "Received unexpected Request message.");

                        break;

                    default:

                        Console.WriteLine(
                            $"Received unsupported IPC message type: " +
                            $"{messageType.Value}");

                        break;
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

    private async Task HandleChildRequestSafelyAsync(
    string json,
    CancellationToken cancellationToken)
    {
        try
        {
            await HandleChildRequestAsync(
                json,
                cancellationToken);
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
        }
        catch (Exception exception)
        {
            Console.WriteLine(
                "[GameManagerClient] Child request handler failed:");

            Console.WriteLine(exception);
        }
    }

    // ---------------------------------------------------------
    // Message type
    // ---------------------------------------------------------

    /// <summary>
    /// Reads the explicit MessageType from an IPC message.
    /// </summary>
    private static IpcMessageType? GetMessageType(
        string json)
    {
        try
        {
            using JsonDocument document =
                JsonDocument.Parse(json);

            JsonElement root =
                document.RootElement;

            if (!root.TryGetProperty(
                    "MessageType",
                    out JsonElement messageTypeElement))
            {
                return null;
            }

            if (messageTypeElement.ValueKind !=
                JsonValueKind.String)
            {
                return null;
            }

            string? messageType =
                messageTypeElement.GetString();

            if (string.IsNullOrWhiteSpace(messageType))
                return null;

            if (Enum.TryParse(
                    messageType,
                    ignoreCase: true,
                    out IpcMessageType result))
            {
                return result;
            }

            return null;
        }
        catch (JsonException)
        {
            return null;
        }
    }

    // ---------------------------------------------------------
    // Response
    // ---------------------------------------------------------

    /// <summary>
    /// Handles a response from the GameManager.
    /// </summary>
    private void HandleResponse(
        string json)
    {
        IpcResponse? response =
            JsonSerializer.Deserialize<IpcResponse>(
                json,
                JsonOptions);

        if (response is null)
        {
            Console.WriteLine(
                "Received invalid IPC response.");

            return;
        }

        Console.WriteLine(
    $"[IPC RESPONSE] RequestId={response.RequestId}, " +
    $"Pending={_pendingRequests.ContainsKey(response.RequestId)}");

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

    // ---------------------------------------------------------
    // Child request
    // ---------------------------------------------------------

    /// <summary>
    /// Handles a request sent by the GameManager to the child.
    /// </summary>
    private async Task HandleChildRequestAsync(
    string json,
    CancellationToken cancellationToken)
    {
        Console.WriteLine(
            "[GameManagerClient] Handling ChildRequest...");

        IpcChildRequest? request =
            JsonSerializer.Deserialize<IpcChildRequest>(
                json,
                JsonOptions);

        if (request is null)
        {
            Console.WriteLine(
                "[GameManagerClient] Invalid child request.");

            return;
        }

        Console.WriteLine(
            $"[GameManagerClient] Child request: " +
            $"{request.Operation}, ID={request.RequestId}");

        IpcResponse response;

        switch (request.Operation)
        {
            case IpcOperation.GetStatus:

                response = new IpcResponse
                {
                    MessageType = IpcMessageType.Response,
                    RequestId = request.RequestId,
                    Success = true,
                    Value = "Ready"
                };

                break;

            default:

                response = new IpcResponse
                {
                    MessageType = IpcMessageType.Response,
                    RequestId = request.RequestId,
                    Success = false,
                    Error = new IpcError
                    {
                        Code = IpcErrorCodes.UnknownOperation,
                        Message =
                            $"Unknown operation '{request.Operation}'."
                    }
                };

                break;
        }

        Console.WriteLine(
            $"[GameManagerClient] Sending ChildRequest response " +
            $"ID={response.RequestId}");

        await WriteAsync(
            JsonSerializer.Serialize(
                response,
                JsonOptions),
            cancellationToken);

        Console.WriteLine(
            $"[GameManagerClient] ChildRequest response sent " +
            $"ID={response.RequestId}");
    }

    // ---------------------------------------------------------
    // Fail pending requests
    // ---------------------------------------------------------

    /// <summary>
    /// Sets all pending requests as failed.
    /// </summary>
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

    // ---------------------------------------------------------
    // Dispose
    // ---------------------------------------------------------

    /// <summary>
    /// Disposes the connection.
    /// </summary>
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

    // ---------------------------------------------------------
    // Register
    // ---------------------------------------------------------

    /// <summary>
    /// Sends authentication/registration information to the
    /// GameManager.
    /// </summary>
    public async Task RegisterAsync(
        string name,
        CancellationToken cancellationToken = default)
    {
        IpcRequest request = new()
        {
            MessageType = IpcMessageType.Request,
            Operation = IpcOperation.Register,
            Value = name
        };

        IpcResponse response =
            await SendRequestAsync(
                request,
                cancellationToken);

        if (!response.Success)
        {
            throw new InvalidOperationException(
                response.Error?.Message ??
                "Registration failed.");
        }

        Console.WriteLine(
            $"{name} registered with GameManager.");
    }

    /// <summary>
    /// Requests that GameManager reload its dynamic resource cache.
    /// </summary>
    public async Task ReloadDynamicCacheAsync(
        CancellationToken cancellationToken = default)
    {
        IpcResponse response = await SendRequestAsync(
            new IpcRequest
            {
                MessageType = IpcMessageType.Request,
                Operation = IpcOperation.ReloadDynamicCache
            },
            cancellationToken);

        Console.WriteLine(
            $"Dynamic cache reload result: {response.Value}");
    }
}