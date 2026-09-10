using GameManager.Shared.Ipc;

public sealed class IpcRequest
{
    public long RequestId { get; set; }

    public IpcOperation Operation { get; set; }

    public string? Resource { get; set; }

    public object? Value { get; set; }

    public IpcMessageType MessageType { get; set; }
}