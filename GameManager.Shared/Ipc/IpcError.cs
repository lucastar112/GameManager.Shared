namespace GameManager.Shared.Ipc
{
    public sealed class IpcError
    {
        public string Code { get; init; } = string.Empty;

        public string Message { get; init; } = string.Empty;
    }
    
    public static class IpcErrorCodes
    {
        public const string UnknownOperation = "UNKNOWN_OPERATION";
        public const string InvalidRequest = "INVALID_REQUEST";
        public const string InvalidResource = "INVALID_RESOURCE";
        public const string InvalidValue = "INVALID_VALUE";
        public const string ResourceUnavailable = "RESOURCE_UNAVAILABLE";
        public const string ReadFailed = "READ_FAILED";
        public const string WriteFailed = "WRITE_FAILED";
        public const string InternalError = "INTERNAL_ERROR";
    }
}