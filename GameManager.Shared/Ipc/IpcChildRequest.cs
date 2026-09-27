namespace GameManager.Shared.Ipc;

public sealed class IpcChildRequest
{
    public long RequestId { get; init; }

    public IpcOperation Operation { get; init; }

    public IpcMessageType MessageType { get; init; } = IpcMessageType.ChildRequest;
}