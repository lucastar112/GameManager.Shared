namespace GameManager.Shared.Ipc
{
    public sealed class GameManagerIpcException : Exception
    {
        public string Code { get; }

        public GameManagerIpcException(
            string code,
            string message)
            : base(message)
        {
            Code = code;
        }
    }
}
