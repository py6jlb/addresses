
using System.Data;
using System.Text;
using Dapper;
using loaderService.Database;
using OpenSearch.Client;

namespace loaderService;

public class Parser : BackgroundService
{
    private readonly ILogger<Parser> _logger;
    private readonly DataContext _database;
    private readonly OpenSearchClient _osClient;

    public Parser(ILogger<Parser> logger, DataContext database)
    {
        _logger = logger;
        _database = database;
        var nodeAddress = new Uri("https://localhost:9200");
        var config = new ConnectionSettings(nodeAddress).DefaultIndex("addresses").
        ServerCertificateValidationCallback((_, _, _, _) => { return true; }).
        BasicAuthentication("admin", "admin");
        _osClient = new OpenSearchClient(config);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            var parents = new Stack<StackItem>();
            parents.Push(new() { Level = -1, ObjectId = 0 });
            var previousNodes = new List<AddressNode>();

            _logger.LogInformation("Задача парсинга адресов: {time}", DateTimeOffset.Now);

            while (parents.TryPop(out var parent))
            {
                var children = await FindByParentId(parent.ObjectId);
                if (children.Any())
                {
                    foreach (var item in children)
                    {
                        parents.Push(new() { Level = parent.Level + 1, ObjectId = item });
                    }
                }


                if (parent.ObjectId != 0)
                {
                    if (previousNodes.Count > parent.Level)
                    {
                        previousNodes = previousNodes.Take(parent.Level).ToList();
                    }

                    var node = await GetNodeById(parent.ObjectId);
                    if(node == null) continue;
                    node.DeepLevel = parent.Level;
                    var nodes = new List<AddressNode>();
                    nodes.AddRange(previousNodes);
                    nodes.Add(node);
                    var address = JoinAddress(nodes.ToArray());
                    await StoreAddress(nodes.ToArray(), address.Address, address.ObjectGuid);
                    _logger.LogInformation($"Адрес: {address.Address}, {address.ObjectGuid}");

                    if (children.Any())
                    {
                        previousNodes.Add(node);
                    }
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

    private JoinResult JoinAddress(AddressNode[] nodes)
    {

        var guids = nodes.Select(x => x.ObjectGuid).ToArray();
        var path = string.Join("|", guids);
        var sb = new StringBuilder();
        var item = nodes.First(x => x.ParentId == 0);
        sb.Append(item.TypeName ?? "").Append(" ").Append(item.Name);
        string lastItemId = item.ObjectGuid;
        for (int i = 0; i < nodes.Length - 1; i++)
        {

            var parentId = item.ObjectId;
            item = nodes.FirstOrDefault(x => x.ParentId == parentId);
            if (item != null)
            {
                sb.Append(", ").Append(item.TypeName ?? "").Append(" ").Append(item.Name);
                lastItemId = item.ObjectGuid;
            }
            else
            {
                break;
            }

        }
        return new JoinResult { Address = sb.ToString(), ObjectGuid = lastItemId, Path = path };
    }


    private async Task StoreAddress(AddressNode[] nodes, string joindeAddress, string objectGuid)
    {
        if (!string.IsNullOrWhiteSpace(objectGuid) && Guid.TryParse(objectGuid, out var id))
        {
            var a = new OpenSearchAddress { Id = id, Nodes = nodes, Address = joindeAddress };
            var response = await _osClient.IndexAsync(a, i => i.Index("addresses"));
        }
    }

    private async Task<IEnumerable<long>> FindByParentId(long parentId)
    {
        var sql = @"
        select 
            ah.object_id            
        from 
            adm_hierarchy as ah
        join 
            addr_obj as ao on ao.object_id = ah.object_id
        where 
            ah.is_active = 1
            and ao.is_actual = 1
            and ah.parent_obj_id = @1";
        using var connection = _database.CreateConnection();
        var queryParams = new Dictionary<string, object?> { { "@1", parentId }, };
        var result = await connection.QueryAsync<long>(sql, queryParams).ConfigureAwait(false);
        return result;
    }

    private async Task<AddressNode> GetNodeById(long objectId)
    {
        var sql = @"        
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
            ah.object_id = @1";
        using var connection = _database.CreateConnection();
        var queryParams = new Dictionary<string, object?> { { "@1", objectId }, };
        var result = await connection.QueryFirstAsync<AddressNode>(sql, queryParams).ConfigureAwait(false);
        return result;
    }

}

class JoinResult
{
    public string Address { get; set; }
    public string ObjectGuid { get; set; }
    public string Path { get; set; }
}



class OpenSearchAddress
{
    public Guid Id { get; set; }
    public AddressNode[] Nodes { get; set; }
    public string Address { get; set; }
}


class StackItem
{
    public int Level { get; set; }
    public long ObjectId { get; set; }
}