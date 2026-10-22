namespace GameManager.Shared.Ipc;

public sealed class IpcRequest
{
    public long RequestId { get; set; }

    public IpcOperation Operation { get; init; }

    public string? Resource { get; init; }

    public int? PlayerId { get; init; }

    public object? Value { get; init; }

    public IpcMessageType MessageType { get; init; } = IpcMessageType.Request;
}