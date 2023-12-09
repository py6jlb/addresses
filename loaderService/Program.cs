using loaderService;
using loaderService.Database;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddSingleton<DataContext>();
builder.Services.AddTransient<XmlExtractor>();
builder.Services.AddHostedService<Worker>();

var host = builder.Build();

{
    using var scope = host.Services.CreateScope();
    var context = scope.ServiceProvider.GetRequiredService<DataContext>();
    await context.InitDatabase();
}

host.Run();
