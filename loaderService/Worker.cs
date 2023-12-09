namespace loaderService;

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly XmlExtractor _extractor;

    public Worker(ILogger<Worker> logger, XmlExtractor extractor)
    {
        _logger = logger;
        _extractor = extractor;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            if (_logger.IsEnabled(LogLevel.Information))
            {
                _logger.LogInformation("Worker running at: {time}", DateTimeOffset.Now);
            }
            var path = "./data/base";
            await _extractor.ExtractTypes(path);
            _logger.LogInformation("Iteration ended: {time}", DateTimeOffset.Now);
            await Task.Delay(60000);
        }
    }
}
