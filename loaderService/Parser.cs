
using System.Data;
using System.Text;
using Dapper;
using loaderService.Database;

namespace loaderService;

public class Parser : BackgroundService
{
    private readonly ILogger<Parser> _logger;
    private readonly DataContext _database;

    public Parser(ILogger<Parser> logger, DataContext database)
    {
        _logger = logger;
        _database = database;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            _logger.LogInformation("Задача парсинга адресов: {time}", DateTimeOffset.Now);
            await foreach (var nodes in GetAddresses())
            {
                if(nodes.Any(x=>x.ParentId == 0)){
                    var address = JoinAddress(nodes);
                    _logger.LogInformation($"Адрес: {address}");
                }
                
            }
            await Task.Delay(1000000000);
        }
    }


    private async Task<ObjectLevel[]> GetLevels()
    {
        var sql = @"select 
            level as Level, name as Name, start_date as StartDate, end_date as EndDate, update_date as UpdateDate, is_active as IsActive
         from object_levels order by level";
        using var connection = _database.CreateConnection();
        var levels = await connection.QueryAsync<ObjectLevel>(sql, commandTimeout: 600);
        return levels.Where(x => x.IsActive).ToArray();
    }

    private async IAsyncEnumerable<long> GetObjects()
    {
        var sql = @"select object_id from addr_obj where is_actual = 1";
        using var connection = _database.CreateConnection();

        await using var reader = await connection.ExecuteReaderAsync(sql).ConfigureAwait(false);
        var rowParser = reader.GetRowParser<long>();

        while (await reader.ReadAsync().ConfigureAwait(false))
        {
            yield return rowParser(reader);
        }
        while (await reader.NextResultAsync().ConfigureAwait(false)) { }
    }

    private async Task<AddressNode[]> GetNodes(long objectId)
    {
        var sql = @"
        with recursive h as(
            select 
                ah.object_id as ObjectId,
                ah.parent_obj_id as ParentId,
                ao.name as Name,
                ao.type_name as TypeName,
                ao.level as Level,
                ao.object_guid as ObjectGuid              
            from 
                adm_hierarchy as ah
            join 
                addr_obj as ao on ao.object_id = ah.object_id
            where 
                ah.object_id = @1
            union all
            select 
                ah2.object_id as ObjectId,
                ah2.parent_obj_id as ParentId,
                ao2.name as Name,
                ao2.type_name as TypeName,
                ao2.level as Level,
                ao2.object_guid as ObjectGuid 
            from 
                adm_hierarchy as ah2
            join addr_obj as ao2 on ao2.object_id = ah2.object_id
            join h on h.ParentId = ao2.object_id
        )
        select ObjectId, ParentId, Name, TypeName, Level, ObjectGuid from h limit 100";
        using var connection = _database.CreateConnection();
        var queryParams = new Dictionary<string, object?> { { "@1", objectId }, };

        var res = await connection.QueryAsync<AddressNode>(sql, queryParams);
        if (res == null) return Array.Empty<AddressNode>();
        return res.ToArray();
    }

    private async IAsyncEnumerable<AddressNode[]> GetAddresses()
    {
        await foreach (var number in GetObjects())
        {
            var nodes = await GetNodes(number);
            if (nodes.Any())
            {
                yield return nodes;
            }
            continue;
        }
    }

    private string JoinAddress(AddressNode[] nodes)
    {
        var sb = new StringBuilder();
        var item = nodes.First(x => x.ParentId == 0);
        sb.Append(item.TypeName ?? "").Append(" ").Append(item.Name);
        for (int i = 0; i < nodes.Length - 1; i++)
        {
            var parentId = item.ObjectId;
            item = nodes.FirstOrDefault(x=>x.ParentId == parentId);
            if(item != null){
                sb.Append(", ").Append(item.TypeName ?? "").Append(" ").Append(item.Name);
            }else{
                break;
            }
            
        }
        return sb.ToString();
    }

}
