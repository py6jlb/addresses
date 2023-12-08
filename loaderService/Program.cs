using loaderService;
using loaderService.database;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddSingleton<DataContext>();
builder.Services.AddTransient<GarRepository>();
builder.Services.AddHostedService<Worker>();

var host = builder.Build();

{
    using var scope = host.Services.CreateScope();
    var context = scope.ServiceProvider.GetRequiredService<DataContext>();
    await context.InitDatabase();
}

host.Run();
