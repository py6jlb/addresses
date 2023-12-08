using System.Data;
using Microsoft.Data.Sqlite;
using Dapper;

namespace loaderService.database;

public class DataContext
{
    private readonly IConfiguration _configuration;

    public DataContext(IConfiguration configuration)
    {
        _configuration = configuration;
    }

    public IDbConnection CreateConnection()
    {
        return new SqliteConnection(_configuration.GetConnectionString("GarDatabase"));
    }

    public async Task InitDatabase()
    {
        // create database tables if they don't exist
        using var connection = CreateConnection();
        await _initUsers();

        async Task _initUsers()
        {
            var sql = """
                CREATE TABLE IF NOT EXISTS 
                Users (
                    Id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                    Title TEXT,
                    FirstName TEXT,
                    LastName TEXT,
                    Email TEXT,
                    Role INTEGER,
                    PasswordHash TEXT
                );
            """;
            await connection.ExecuteAsync(sql);
        }
    }

}
