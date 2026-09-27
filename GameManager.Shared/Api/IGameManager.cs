namespace GameManager.Shared.Api
{
    public interface IGameManager : IAsyncDisposable
    {
        Task ConnectAsync(CancellationToken cancellationToken = default);

        Task<T?> GetAsync<T>(string resource, int? playerId = null, CancellationToken cancellationToken = default);

        //Task SetAsync<T>(string resource, T value, int? playerId = null, CancellationToken cancellationToken = default);

        Task SetAsync<T>(string resource, T value, CancellationToken cancellationToken = default);
    }
}