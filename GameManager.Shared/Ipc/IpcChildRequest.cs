namespace GameManager.Shared.Ipc
{
    public sealed class IpcChildRequest
    {
        public long RequestId { get; set; }

        public IpcOperation Operation { get; set; }

        public string? Resource { get; set; }

        public object? Value { get; set; }
    }
}