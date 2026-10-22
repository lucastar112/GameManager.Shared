namespace GameManager.Shared.Ipc;

public sealed class IpcResponse
{
    public long RequestId { get; init; }

    public bool Success { get; init; }

    public object? Value { get; init; }

    public IpcMessageType MessageType { get; init; } = IpcMessageType.Response;

    public IpcError? Error { get; init; }
}