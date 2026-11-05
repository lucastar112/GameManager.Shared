namespace GameManager.Shared.Ipc;

/// <summary>One item of a <see cref="IpcOperation.GetResources"/> request.</summary>
public sealed class IpcResourceRequest
{
    public string Resource { get; set; } = string.Empty;

    public int? PlayerId { get; set; }
}

/// <summary>One item of a <see cref="IpcOperation.GetResources"/> response (same order as the request).</summary>
public sealed class IpcResourceResult
{
    public bool Success { get; set; }

    public object? Value { get; set; }

    public string? Error { get; set; }
}
