namespace GameManager.Shared.Ipc
{
    public sealed class IpcMessage
    {
        public IpcMessageType Type { get; set; }

        public IpcRequest? Request { get; set; }

        public IpcChildRequest? ChildRequest { get; set; }

        public IpcResponse? Response { get; set; }
    }
}