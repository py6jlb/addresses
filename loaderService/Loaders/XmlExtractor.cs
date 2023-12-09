using System.Xml;
using Dapper;
using loaderService.Database;

namespace loaderService;
public class XmlExtractor
{
    private readonly IConfiguration _configuration;
    private readonly DataContext _database;

    public XmlExtractor(IConfiguration configuration, DataContext database)
    {
        _database = database;
        _configuration = configuration;
    }

    #region TYPES
    private async Task ExtractAddHouseTypes(string path)
    {
        const string fileMask = "AS_ADDHOUSE_TYPES*.XML";
        var selectQuery = @"SELECT COUNT(*) FROM add_house_types WHERE id = @1";
        var insertQuery = @"INSERT INTO add_house_types VALUES (@1, @2, @3, @4, @5, @6, @7, @8)";
        var updateQuery = @"UPDATE add_house_types SET name = @2, short_name = @3, desc = @4, is_active = @5, update_date = @6, start_date = @7, end_date = @8 WHERE id = @1";

        var file = Directory.GetFiles(path, fileMask);
        if (!file.Any()) return;
        var filePath = file[0];
        using var reader = XmlReader.Create(filePath, new XmlReaderSettings { Async = true });
        using var connection = _database.CreateConnection();
        connection.Open();
        while (await reader.ReadAsync())
        {
            if (reader.NodeType != XmlNodeType.Element || reader.GetAttribute("ID") == null) continue;
            var res = new AdditionalHouseType();

            res.Id = int.Parse(reader.GetAttribute("ID"));
            res.Name = reader.GetAttribute("NAME");
            res.ShortName = reader.GetAttribute("SHORTNAME");
            res.Desc = reader.GetAttribute("DESC");
            res.IsActive = bool.Parse(reader.GetAttribute("ISACTIVE"));
            res.UpdateDate = DateTime.Parse(reader.GetAttribute("UPDATEDATE"));
            res.StartDate = DateTime.Parse(reader.GetAttribute("STARTDATE"));
            res.EndDate = DateTime.Parse(reader.GetAttribute("ENDDATE"));


            var count = await connection.ExecuteScalarAsync<int>(
                selectQuery,
                param: new Dictionary<string, object> { { "@1", res.Id } },
                commandTimeout: 600);

            var queryParams = new Dictionary<string, object?> {
                { "@1", res.Id },
                { "@2", res.Name },
                { "@3", res.ShortName },
                { "@4", res.Desc },
                { "@5", res.IsActive },
                { "@6", res.UpdateDate },
                { "@7", res.StartDate },
                { "@8", res.EndDate },
            };
            if (count > 0)
            {
                await connection.ExecuteAsync(updateQuery, queryParams, commandTimeout: 600);
            }
            else
            {
                await connection.ExecuteAsync(insertQuery, queryParams, commandTimeout: 600);
            }
        }
    }

    private async Task ExtractAddrObjTypes(string path)
    {
        const string fileMask = "AS_ADDR_OBJ_TYPES_*.XML";
        var selectQuery = @"SELECT COUNT(*) FROM addr_obj_types WHERE id = @1";
        var insertQuery = @"INSERT INTO addr_obj_types VALUES (@1, @2, @3, @4, @5, @6, @7, @8, @9)";
        var updateQuery = @"UPDATE addr_obj_types SET level = @2, name = @3, short_name = @4, desc = @5, is_active = @6, update_date = @7, start_date = @8, end_date = @9 WHERE id = @1";

        var file = Directory.GetFiles(path, fileMask);
        if (!file.Any()) return;
        var filePath = file[0];
        using var reader = XmlReader.Create(filePath, new XmlReaderSettings { Async = true });
        using var connection = _database.CreateConnection();
        connection.Open();
        while (await reader.ReadAsync())
        {
            if (reader.NodeType != XmlNodeType.Element || reader.GetAttribute("ID") == null) continue;
            var res = new AddressObjectType();

            res.Id = int.Parse(reader.GetAttribute("ID"));
            res.Level = int.Parse(reader.GetAttribute("LEVEL"));
            res.Name = reader.GetAttribute("NAME");
            res.ShortName = reader.GetAttribute("SHORTNAME");
            res.Desc = reader.GetAttribute("DESC");
            res.IsActive = bool.Parse(reader.GetAttribute("ISACTIVE"));
            res.UpdateDate = DateTime.Parse(reader.GetAttribute("UPDATEDATE"));
            res.StartDate = DateTime.Parse(reader.GetAttribute("STARTDATE"));
            res.EndDate = DateTime.Parse(reader.GetAttribute("ENDDATE"));


            var count = await connection.ExecuteScalarAsync<int>(
                selectQuery,
                param: new Dictionary<string, object> { { "@1", res.Id } },
                commandTimeout: 600);

            var queryParams = new Dictionary<string, object?> {
                { "@1", res.Id },
                { "@2", res.Level },
                { "@3", res.Name },
                { "@4", res.ShortName },
                { "@5", res.Desc },
                { "@6", res.IsActive },
                { "@7", res.UpdateDate },
                { "@8", res.StartDate },
                { "@9", res.EndDate },
            };
            if (count > 0)
            {
                await connection.ExecuteAsync(updateQuery, queryParams, commandTimeout: 600);
            }
            else
            {
                await connection.ExecuteAsync(insertQuery, queryParams, commandTimeout: 600);
            }
        }
    }

    private async Task ExtractHouseTypes(string path)
    {
        const string fileMask = "AS_HOUSE_TYPES*.XML";
        var selectQuery = @"SELECT COUNT(*) FROM house_types WHERE id = @1";
        var insertQuery = @"INSERT INTO house_types VALUES (@1, @2, @3, @4, @5, @6, @7, @8)";
        var updateQuery = @"UPDATE house_types SET name = @2, short_name = @3, desc = @4, is_active = @5, update_date = @6, start_date = @7, end_date = @8 WHERE id = @1";

        var file = Directory.GetFiles(path, fileMask);
        if (!file.Any()) return;
        var filePath = file[0];
        using var reader = XmlReader.Create(filePath, new XmlReaderSettings { Async = true });
        using var connection = _database.CreateConnection();
        connection.Open();
        while (await reader.ReadAsync())
        {
            if (reader.NodeType != XmlNodeType.Element || reader.GetAttribute("ID") == null) continue;
            var res = new HouseType();

            res.Id = int.Parse(reader.GetAttribute("ID"));
            res.Name = reader.GetAttribute("NAME");
            res.ShortName = reader.GetAttribute("SHORTNAME");
            res.Desc = reader.GetAttribute("DESC");
            res.IsActive = bool.Parse(reader.GetAttribute("ISACTIVE"));
            res.UpdateDate = DateTime.Parse(reader.GetAttribute("UPDATEDATE"));
            res.StartDate = DateTime.Parse(reader.GetAttribute("STARTDATE"));
            res.EndDate = DateTime.Parse(reader.GetAttribute("ENDDATE"));


            var count = await connection.ExecuteScalarAsync<int>(
                selectQuery,
                param: new Dictionary<string, object> { { "@1", res.Id } },
                commandTimeout: 600);

            var queryParams = new Dictionary<string, object?> {
                { "@1", res.Id },
                { "@2", res.Name },
                { "@3", res.ShortName },
                { "@4", res.Desc },
                { "@5", res.IsActive },
                { "@6", res.UpdateDate },
                { "@7", res.StartDate },
                { "@8", res.EndDate },
            };
            if (count > 0)
            {
                await connection.ExecuteAsync(updateQuery, queryParams, commandTimeout: 600);
            }
            else
            {
                await connection.ExecuteAsync(insertQuery, queryParams, commandTimeout: 600);
            }
        }
    }

    private async Task ExtractNormativeDocsKinds(string path)
    {
        const string fileMask = "AS_NORMATIVE_DOCS_KINDS*.XML";
        var selectQuery = @"SELECT COUNT(*) FROM normative_docs_kind WHERE id = @1";
        var insertQuery = @"INSERT INTO normative_docs_kind VALUES (@1, @2)";
        var updateQuery = @"UPDATE normative_docs_kind SET name = @2 WHERE id = @1";

        var file = Directory.GetFiles(path, fileMask);
        if (!file.Any()) return;
        var filePath = file[0];
        using var reader = XmlReader.Create(filePath, new XmlReaderSettings { Async = true });
        using var connection = _database.CreateConnection();
        connection.Open();
        while (await reader.ReadAsync())
        {
            if (reader.NodeType != XmlNodeType.Element || reader.GetAttribute("ID") == null) continue;
            var res = new NormativeDocKind();

            res.Id = int.Parse(reader.GetAttribute("ID"));
            res.Name = reader.GetAttribute("NAME");


            var count = await connection.ExecuteScalarAsync<int>(
                selectQuery,
                param: new Dictionary<string, object> { { "@1", res.Id } },
                commandTimeout: 600);

            var queryParams = new Dictionary<string, object?> {
                { "@1", res.Id },
                { "@2", res.Name },
            };
            if (count > 0)
            {
                await connection.ExecuteAsync(updateQuery, queryParams, commandTimeout: 600);
            }
            else
            {
                await connection.ExecuteAsync(insertQuery, queryParams, commandTimeout: 600);
            }
        }
    }

    private async Task ExtractNormativeDocsTypes(string path)
    {
        const string fileMask = "AS_NORMATIVE_DOCS_TYPES*.XML";
        var selectQuery = @"SELECT COUNT(*) FROM normative_docs_types WHERE id = @1";
        var insertQuery = @"INSERT INTO normative_docs_types VALUES (@1, @2, @3, @4)";
        var updateQuery = @"UPDATE normative_docs_types SET name = @2, start_date = @3, end_date = @4 WHERE id = @1";

        var file = Directory.GetFiles(path, fileMask);
        if (!file.Any()) return;
        var filePath = file[0];
        using var reader = XmlReader.Create(filePath, new XmlReaderSettings { Async = true });
        using var connection = _database.CreateConnection();
        connection.Open();
        while (await reader.ReadAsync())
        {
            if (reader.NodeType != XmlNodeType.Element || reader.GetAttribute("ID") == null) continue;
            var res = new NormativeDocType();

            res.Id = int.Parse(reader.GetAttribute("ID"));
            res.Name = reader.GetAttribute("NAME");
            res.StartDate = DateTime.Parse(reader.GetAttribute("STARTDATE"));
            res.EndDate = DateTime.Parse(reader.GetAttribute("ENDDATE"));


            var count = await connection.ExecuteScalarAsync<int>(
                selectQuery,
                param: new Dictionary<string, object> { { "@1", res.Id } },
                commandTimeout: 600);

            var queryParams = new Dictionary<string, object?> {
                { "@1", res.Id },
                { "@2", res.Name },
                { "@3", res.StartDate },
                { "@4", res.EndDate },
            };
            if (count > 0)
            {
                await connection.ExecuteAsync(updateQuery, queryParams, commandTimeout: 600);
            }
            else
            {
                await connection.ExecuteAsync(insertQuery, queryParams, commandTimeout: 600);
            }
        }
    }

    private async Task ExtractObjectLevels(string path)
    {
        const string fileMask = "AS_OBJECT_LEVELS*.XML";
        var selectQuery = @"SELECT COUNT(*) FROM object_levels WHERE level = @1";
        var insertQuery = @"INSERT INTO object_levels VALUES (@1, @2, @3, @4, @5, @6)";
        var updateQuery = @"UPDATE object_levels SET name = @2, start_date = @3, end_date = @4, update_date = @5, is_active = @6 WHERE level = @1";

        var file = Directory.GetFiles(path, fileMask);
        if (file.Length == 0) return;
        var filePath = file[0];
        using var reader = XmlReader.Create(filePath, new XmlReaderSettings { Async = true });
        using var connection = _database.CreateConnection();
        connection.Open();
        while (await reader.ReadAsync())
        {
            if (reader.NodeType != XmlNodeType.Element || reader.GetAttribute("LEVEL") == null) continue;
            var res = new ObjectLevel();

            res.Level = int.Parse(reader.GetAttribute("LEVEL"));
            res.Name = reader.GetAttribute("NAME");
            res.StartDate = DateTime.Parse(reader.GetAttribute("STARTDATE"));
            res.EndDate = DateTime.Parse(reader.GetAttribute("ENDDATE"));
            res.UpdateDate = DateTime.Parse(reader.GetAttribute("UPDATEDATE"));
            res.IsActive = bool.Parse(reader.GetAttribute("ISACTIVE"));


            var count = await connection.ExecuteScalarAsync<int>(
                selectQuery,
                param: new Dictionary<string, object> { { "@1", res.Level } },
                commandTimeout: 600);

            var queryParams = new Dictionary<string, object?> {
                { "@1", res.Level },
                { "@2", res.Name },
                { "@3", res.StartDate },
                { "@4", res.EndDate },
                { "@5", res.UpdateDate },
                { "@6", res.IsActive },
            };
            if (count > 0)
            {
                await connection.ExecuteAsync(updateQuery, queryParams, commandTimeout: 600);
            }
            else
            {
                await connection.ExecuteAsync(insertQuery, queryParams, commandTimeout: 600);
            }
        }
    }

    private async Task ExtractOperationTypes(string path)
    {
        const string fileMask = "AS_OPERATION_TYPES_*.XML";
        var selectQuery = @"SELECT COUNT(*) FROM operation_types WHERE id = @1";
        var insertQuery = @"INSERT INTO operation_types VALUES (@1, @2, @3, @4, @5, @6)";
        var updateQuery = @"UPDATE operation_types SET name = @2, is_active = @3, update_date = @4, start_date = @5, end_date = @6 WHERE id = @1";

        var file = Directory.GetFiles(path, fileMask);
        if (!file.Any()) return;
        var filePath = file[0];
        using var reader = XmlReader.Create(filePath, new XmlReaderSettings { Async = true });
        using var connection = _database.CreateConnection();
        connection.Open();
        while (await reader.ReadAsync())
        {
            if (reader.NodeType != XmlNodeType.Element || reader.GetAttribute("ID") == null) continue;
            var res = new OperationType();

            res.Id = int.Parse(reader.GetAttribute("ID"));
            res.Name = reader.GetAttribute("NAME");
            res.IsActive = bool.Parse(reader.GetAttribute("ISACTIVE"));
            res.UpdateDate = DateTime.Parse(reader.GetAttribute("UPDATEDATE"));
            res.StartDate = DateTime.Parse(reader.GetAttribute("STARTDATE"));
            res.EndDate = DateTime.Parse(reader.GetAttribute("ENDDATE"));


            var count = await connection.ExecuteScalarAsync<int>(
                selectQuery,
                param: new Dictionary<string, object> { { "@1", res.Id } },
                commandTimeout: 600);

            var queryParams = new Dictionary<string, object?> {
                { "@1", res.Id },
                { "@2", res.Name },
                { "@3", res.IsActive },
                { "@4", res.UpdateDate },
                { "@5", res.StartDate },
                { "@6", res.EndDate },
            };
            if (count > 0)
            {
                await connection.ExecuteAsync(updateQuery, queryParams, commandTimeout: 600);
            }
            else
            {
                await connection.ExecuteAsync(insertQuery, queryParams, commandTimeout: 600);
            }
        }
    }

    private async Task ExtractParamTypes(string path)
    {
        const string fileMask = "AS_PARAM_TYPES_*.XML";
        var selectQuery = @"SELECT COUNT(*) FROM param_types WHERE id = @1";
        var insertQuery = @"INSERT INTO param_types VALUES (@1, @2, @3, @4, @5, @6, @7, @8)";
        var updateQuery = @"UPDATE param_types SET name = @2, code = @3, desc = @4, is_active = @5, update_date = @6, start_date = @7, end_date = @8 WHERE id = @1";

        var file = Directory.GetFiles(path, fileMask);
        if (!file.Any()) return;
        var filePath = file[0];
        using var reader = XmlReader.Create(filePath, new XmlReaderSettings { Async = true });
        using var connection = _database.CreateConnection();
        connection.Open();
        while (await reader.ReadAsync())
        {
            if (reader.NodeType != XmlNodeType.Element || reader.GetAttribute("ID") == null) continue;
            var res = new ParamType();

            res.Id = int.Parse(reader.GetAttribute("ID"));
            res.Name = reader.GetAttribute("NAME");
            res.Code = reader.GetAttribute("CODE");
            res.Desc = reader.GetAttribute("DESC");
            res.IsActive = bool.Parse(reader.GetAttribute("ISACTIVE"));
            res.UpdateDate = DateTime.Parse(reader.GetAttribute("UPDATEDATE"));
            res.StartDate = DateTime.Parse(reader.GetAttribute("STARTDATE"));
            res.EndDate = DateTime.Parse(reader.GetAttribute("ENDDATE"));


            var count = await connection.ExecuteScalarAsync<int>(
                selectQuery,
                param: new Dictionary<string, object> { { "@1", res.Id } },
                commandTimeout: 600);

            var queryParams = new Dictionary<string, object?> {
                { "@1", res.Id },
                { "@2", res.Name },
                { "@3", res.Code },
                { "@4", res.Desc },
                { "@5", res.IsActive },
                { "@6", res.UpdateDate },
                { "@7", res.StartDate },
                { "@8", res.EndDate },
            };
            if (count > 0)
            {
                await connection.ExecuteAsync(updateQuery, queryParams, commandTimeout: 600);
            }
            else
            {
                await connection.ExecuteAsync(insertQuery, queryParams, commandTimeout: 600);
            }
        }
    }

    private async Task ExtractRoomTypes(string path)
    {
        const string fileMask = "AS_ROOM_TYPES_*.XML";
        var selectQuery = @"SELECT COUNT(*) FROM room_types WHERE id = @1";
        var insertQuery = @"INSERT INTO room_types VALUES (@1, @2, @3, @4, @5, @6, @7)";
        var updateQuery = @"UPDATE room_types SET name = @2, desc = @3, is_active = @4, update_date = @5, start_date = @6, end_date = @7 WHERE id = @1";

        var file = Directory.GetFiles(path, fileMask);
        if (!file.Any()) return;
        var filePath = file[0];
        using var reader = XmlReader.Create(filePath, new XmlReaderSettings { Async = true });
        using var connection = _database.CreateConnection();
        connection.Open();
        while (await reader.ReadAsync())
        {
            if (reader.NodeType != XmlNodeType.Element || reader.GetAttribute("ID") == null) continue;
            var res = new RoomType();

            res.Id = int.Parse(reader.GetAttribute("ID"));
            res.Name = reader.GetAttribute("NAME");
            res.Desc = reader.GetAttribute("DESC");
            res.IsActive = bool.Parse(reader.GetAttribute("ISACTIVE"));
            res.UpdateDate = DateTime.Parse(reader.GetAttribute("UPDATEDATE"));
            res.StartDate = DateTime.Parse(reader.GetAttribute("STARTDATE"));
            res.EndDate = DateTime.Parse(reader.GetAttribute("ENDDATE"));


            var count = await connection.ExecuteScalarAsync<int>(
                selectQuery,
                param: new Dictionary<string, object> { { "@1", res.Id } },
                commandTimeout: 600);

            var queryParams = new Dictionary<string, object?> {
                { "@1", res.Id },
                { "@2", res.Name },
                { "@3", res.Desc },
                { "@4", res.IsActive },
                { "@5", res.UpdateDate },
                { "@6", res.StartDate },
                { "@7", res.EndDate },
            };
            if (count > 0)
            {
                await connection.ExecuteAsync(updateQuery, queryParams, commandTimeout: 600);
            }
            else
            {
                await connection.ExecuteAsync(insertQuery, queryParams, commandTimeout: 600);
            }
        }
    }

    private async Task ExtractApartmentTypes(string path)
    {
        const string fileMask = "AS_APARTMENT_TYPES*.XML";
        var selectQuery = @"SELECT COUNT(*) FROM apartment_types WHERE id = @1";
        var insertQuery = @"INSERT INTO apartment_types VALUES (@1, @2, @3, @4, @5, @6, @7, @8)";
        var updateQuery = @"UPDATE apartment_types SET name = @2, short_name = @3, desc = @4, is_active = @5, update_date = @6, start_date = @7, end_date = @8 WHERE id = @1";

        var file = Directory.GetFiles(path, fileMask);
        if (!file.Any()) return;
        var filePath = file[0];
        using var reader = XmlReader.Create(filePath, new XmlReaderSettings { Async = true });
        using var connection = _database.CreateConnection();
        connection.Open();
        while (await reader.ReadAsync())
        {
            if (reader.NodeType != XmlNodeType.Element || reader.GetAttribute("ID") == null) continue;
            var res = new ApartmentType();

            res.Id = int.Parse(reader.GetAttribute("ID"));
            res.Name = reader.GetAttribute("NAME");
            res.ShortName = reader.GetAttribute("SHORTNAME");
            res.Desc = reader.GetAttribute("DESC");
            res.IsActive = bool.Parse(reader.GetAttribute("ISACTIVE"));
            res.UpdateDate = DateTime.Parse(reader.GetAttribute("UPDATEDATE"));
            res.StartDate = DateTime.Parse(reader.GetAttribute("STARTDATE"));
            res.EndDate = DateTime.Parse(reader.GetAttribute("ENDDATE"));


            var count = await connection.ExecuteScalarAsync<int>(
                selectQuery,
                param: new Dictionary<string, object> { { "@1", res.Id } },
                commandTimeout: 600);

            var queryParams = new Dictionary<string, object?> {
                { "@1", res.Id },
                { "@2", res.Name },
                { "@3", res.ShortName },
                { "@4", res.Desc },
                { "@5", res.IsActive },
                { "@6", res.UpdateDate },
                { "@7", res.StartDate },
                { "@8", res.EndDate },
            };
            if (count > 0)
            {
                await connection.ExecuteAsync(updateQuery, queryParams, commandTimeout: 600);
            }
            else
            {
                await connection.ExecuteAsync(insertQuery, queryParams, commandTimeout: 600);
            }
        }
    }
    #endregion
    public async Task ExtractTypes(string path)
    {
        await ExtractAddHouseTypes(path);
        await ExtractAddrObjTypes(path);
        await ExtractHouseTypes(path);
        await ExtractNormativeDocsKinds(path);
        await ExtractNormativeDocsTypes(path);
        await ExtractObjectLevels(path);
        await ExtractOperationTypes(path);
        await ExtractParamTypes(path);
        await ExtractRoomTypes(path);
        await ExtractApartmentTypes(path);
    }

    #region DATA
    private async Task ExtractNormativeDocs(string path)
    {
        await Task.Delay(1);
    }

    private async Task ExtractCarplacesParams(string path)
    {
        await Task.Delay(1);
    }

    private async Task ExtractRoomParams(string path)
    {
        //TODO: Нужно взять регион в котором есть машиноместа для отладки
        await Task.Delay(1);
    }

    private async Task ExtractApartmentsParams(string path)
    {
        await Task.Delay(1);
    }

    private async Task ExtractHouseParams(string path)
    {
        await Task.Delay(1);
    }

    private async Task ExtractSteadParams(string path)
    {
        await Task.Delay(1);
    }

    private async Task ExtractAddrObjParams(string path)
    {
        await Task.Delay(1);
    }

    private async Task ExtractAddrObjDivision(string path)
    {
        await Task.Delay(1);
    }

    private async Task ExtractChangeHistory(string path)
    {
        await Task.Delay(1);
    }

    private async Task ExtractMunHierarchy(string path)
    {
        await Task.Delay(1);
    }

    private async Task ExtractAdmHierarchy(string path)
    {
        await Task.Delay(1);
    }

    private async Task ExtractCarplaces(string path)
    {
        await Task.Delay(1);
    }

    private async Task ExtractRooms(string path)
    {
        await Task.Delay(1);
    }

    private async Task ExtractApartments(string path)
    {
        await Task.Delay(1);
    }

    private async Task ExtractSteads(string path)
    {
        await Task.Delay(1);
    }

    private async Task ExtractHouses()
    {
        await Task.Delay(1);
    }

    private async Task ExtractAddrObj(string path)
    {
        await Task.Delay(1);
    }

    private async Task ExtractReestrObjects(string path)
    {
        await Task.Delay(1);
    }
    #endregion
    public async Task ExtractData(string path)
    {
        await ExtractCarplacesParams(path);
        await ExtractRoomParams(path);
        await ExtractNormativeDocs(path);
        await ExtractApartmentsParams(path);
        await ExtractHouseParams(path);
        await ExtractSteadParams(path);
        await ExtractAddrObjParams(path);
        await ExtractAddrObjDivision(path);
        await ExtractChangeHistory(path);
        await ExtractMunHierarchy(path);
        await ExtractAdmHierarchy(path);
        await ExtractCarplaces(path);
        await ExtractRooms(path);
        await ExtractApartments(path);
        await ExtractSteads(path);
        await ExtractAddrObj(path);
        await ExtractReestrObjects(path);
    }

}