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
                _logger.LogInformation("Worker running at: {time}", DateTimeOffset.Now);

            _logger.LogInformation("Types extraction start at: {time}", DateTimeOffset.Now);
            var typePath = "./data/base";
            await _extractor.ExtractTypes(typePath);
            _logger.LogInformation("Types extraction end at: {time}", DateTimeOffset.Now);

            _logger.LogInformation("Data extraction start at: {time}", DateTimeOffset.Now);
            var dataPath = "./data/base/05";
            await _extractor.ExtractData(dataPath);
            _logger.LogInformation("Data extraction end at: {time}", DateTimeOffset.Now);

            _logger.LogInformation("Iteration end at: {time}", DateTimeOffset.Now);
            await Task.Delay(60000);
        }
    }
}
