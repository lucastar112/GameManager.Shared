namespace GameManager.Shared.Api
{
    public interface IGameManager : IAsyncDisposable
    {
        Task ConnectAsync(CancellationToken cancellationToken = default);

        Task<T?> GetAsync<T>(string resource, CancellationToken cancellationToken = default);

        Task SetAsync<T>(string resource, T value, CancellationToken cancellationToken = default);
    }
}