namespace GameManager.Shared;

/// <summary>
/// Gates chatty IPC tracing (heartbeats, request/response echo, write-lock traces).
/// Off by default. Enable with the environment variable GAMEMANAGER_IPC_VERBOSE=1
/// or by setting <see cref="Verbose"/> at start-up.
/// </summary>
public static class IpcLog
{
    public static volatile bool Verbose =
        Environment.GetEnvironmentVariable("GAMEMANAGER_IPC_VERBOSE") is "1" or "true" or "TRUE";

    public static void Debug(string message)
    {
        if (Verbose)
            Console.WriteLine(message);
    }
}
