using System.Data;
using System.Xml;
using Dapper;
using loaderService.Database;

namespace loaderService;
public class XmlExtractor
{
    private readonly IConfiguration _configuration;
    private readonly DataContext _database;

    private const int _capacity = 1001;
    private const int condition = 1000;

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
    private async Task ExtractNormativeDocs(string path, bool delta)
    {
        var paramNames = new[] { "@1", "@2", "@3", "@4", "@5", "@6", "@7", "@8", "@9", "@10", "@11", "@12", };
        const string fileMask = "AS_NORMATIVE_DOCS_*.XML";
        var selectQuery = @"SELECT COUNT(*) FROM normative_docs WHERE id = @1";
        var insertQuery = @"INSERT INTO normative_docs VALUES (@1, @2, @3, @4, @5, @6, @7, @8, @9, @10, @11, @12)";
        var updateQuery = @"UPDATE normative_docs SET name = @2, date = @3, number = @4, type = @5, kind = @6, update_date = @7, org_name = @8, reg_num = @9, reg_date = @10, acc_date = @11, comment = @12 WHERE id = @1";

        var file = Directory.GetFiles(path, fileMask);
        if (!file.Any()) return;
        var filePath = file[0];
        using var reader = XmlReader.Create(filePath, new XmlReaderSettings { Async = true });
        using var connection = _database.CreateConnection();
        connection.Open();

        var updateCommandParamValues = new List<Dictionary<string, object?>>(_capacity);
        var addCommandParamValues = new List<Dictionary<string, object?>>(_capacity);
        while (await reader.ReadAsync())
        {
            if (reader.NodeType != XmlNodeType.Element || reader.GetAttribute("ID") == null) continue;
            var id = int.Parse(reader.GetAttribute("ID"));
            var date = reader.GetAttribute("DATE");
            var regDate = reader.GetAttribute("REGDATE");
            var accDate = reader.GetAttribute("ACCDATE");
            var type = reader.GetAttribute("TYPE");
            var kind = reader.GetAttribute("KIND");
            var updateDate = reader.GetAttribute("UPDATEDATE");
            var queryParams = new Dictionary<string, object?> {
                { "@1", id },
                { "@2", reader.GetAttribute("NAME") },
                { "@3", date != null ? DateTime.Parse(date) : null },
                { "@4", reader.GetAttribute("NUMBER") },
                { "@5", type != null ? int.Parse(type) : null },
                { "@6", kind != null ? int.Parse(kind) : null },
                { "@7", updateDate != null ? DateTime.Parse(updateDate) : null },
                { "@8", reader.GetAttribute("ORGNAME") },
                { "@9", reader.GetAttribute("REGNUM") },
                { "@10", regDate != null ? DateTime.Parse(regDate) : null },
                { "@11", accDate != null ? DateTime.Parse(accDate) : null },
                { "@12", reader.GetAttribute("COMMENT") },
            };


            if (delta)
            {
                var count = await connection.ExecuteScalarAsync<int>(
                    selectQuery,
                    param: new Dictionary<string, object> { { "@1", id } },
                    commandTimeout: 600);
                if (count > 0)
                    updateCommandParamValues.Add(queryParams);
                else
                    addCommandParamValues.Add(queryParams);
            }
            else
            {
                addCommandParamValues.Add(queryParams);
            }

            if (addCommandParamValues.Count == condition)
            {
                ToDb(connection, insertQuery, paramNames, addCommandParamValues);
                addCommandParamValues.Clear();
            }

            if (updateCommandParamValues.Count == condition)
            {
                ToDb(connection, updateQuery, paramNames, updateCommandParamValues);
                updateCommandParamValues.Clear();
            }
        }

        if (addCommandParamValues.Count > 0)
        {
            ToDb(connection, insertQuery, paramNames, addCommandParamValues);
            addCommandParamValues.Clear();
        }

        if (updateCommandParamValues.Count > 0)
        {
            ToDb(connection, updateQuery, paramNames, updateCommandParamValues);
            updateCommandParamValues.Clear();
        }
    }

    private async Task ExtractCarplacesParams(string path, bool delta)
    {
        var paramNames = new[] { "@1", "@2", "@3", "@4", "@5", "@6", "@7", "@8", "@9" };
        const string fileMask = "AS_CARPLACES_PARAMS_*.XML";
        var selectQuery = @"SELECT COUNT(*) FROM carplaces_params WHERE id = @1";
        var insertQuery = @"INSERT INTO carplaces_params VALUES (@1, @2, @3, @4, @5, @6, @7, @8, @9)";
        var updateQuery = @"UPDATE carplaces_params SET object_id = @2, change_id = @3, change_end_id = @4, type_id = @5, value = @6, update_date = @7, start_date = @8, end_date = @9 WHERE id = @1";

        var file = Directory.GetFiles(path, fileMask);
        if (!file.Any()) return;
        var filePath = file[0];
        using var reader = XmlReader.Create(filePath, new XmlReaderSettings { Async = true });
        using var connection = _database.CreateConnection();
        connection.Open();

        var updateCommandParamValues = new List<Dictionary<string, object?>>(_capacity);
        var addCommandParamValues = new List<Dictionary<string, object?>>(_capacity);
        while (await reader.ReadAsync())
        {
            if (reader.NodeType != XmlNodeType.Element || reader.GetAttribute("ID") == null) continue;
            var id = int.Parse(reader.GetAttribute("ID"));
            var objectId = reader.GetAttribute("OBJECTID");
            var changeId = reader.GetAttribute("CHANGEID");
            var changeEndId = reader.GetAttribute("CHANGEIDEND");
            var typeId = reader.GetAttribute("TYPEID");
            var value = reader.GetAttribute("VALUE");
            var updateDate = reader.GetAttribute("UPDATEDATE");
            var startDate = reader.GetAttribute("STARTDATE");
            var endDate = reader.GetAttribute("ENDDATE");
            var queryParams = new Dictionary<string, object?> {
                { "@1", id },
                { "@2", objectId != null ? int.Parse(objectId) : null },
                { "@3", changeId != null ? int.Parse(changeId) : null },
                { "@4", changeEndId != null ? int.Parse(changeEndId) : null },
                { "@5", typeId != null ? int.Parse(typeId) : null },
                { "@6", value },
                { "@7", updateDate != null ? DateTime.Parse(updateDate) : null },
                { "@8", startDate != null ? DateTime.Parse(startDate) : null },
                { "@9", endDate != null ? DateTime.Parse(endDate) : null },
            };


            if (delta)
            {
                var count = await connection.ExecuteScalarAsync<int>(
                    selectQuery,
                    param: new Dictionary<string, object> { { "@1", id } },
                    commandTimeout: 600);
                if (count > 0)
                    updateCommandParamValues.Add(queryParams);
                else
                    addCommandParamValues.Add(queryParams);
            }
            else
            {
                addCommandParamValues.Add(queryParams);
            }

            if (addCommandParamValues.Count == condition)
            {
                ToDb(connection, insertQuery, paramNames, addCommandParamValues);
                addCommandParamValues.Clear();
            }

            if (updateCommandParamValues.Count == condition)
            {
                ToDb(connection, updateQuery, paramNames, updateCommandParamValues);
                updateCommandParamValues.Clear();
            }
        }
        if (addCommandParamValues.Count > 0)
        {
            ToDb(connection, insertQuery, paramNames, addCommandParamValues);
            addCommandParamValues.Clear();
        }

        if (updateCommandParamValues.Count > 0)
        {
            ToDb(connection, updateQuery, paramNames, updateCommandParamValues);
            updateCommandParamValues.Clear();
        }
    }

    private async Task ExtractRoomParams(string path, bool delta)
    {
        var paramNames = new[] { "@1", "@2", "@3", "@4", "@5", "@6", "@7", "@8", "@9" };
        const string fileMask = "AS_ROOMS_PARAMS_*.XML";
        var selectQuery = @"SELECT COUNT(*) FROM room_params WHERE id = @1";
        var insertQuery = @"INSERT INTO room_params VALUES (@1, @2, @3, @4, @5, @6, @7, @8, @9)";
        var updateQuery = @"UPDATE room_params SET object_id = @2, change_id = @3, change_end_id = @4, type_id = @5, value = @6, update_date = @7, start_date = @8, end_date = @9 WHERE id = @1";

        var file = Directory.GetFiles(path, fileMask);
        if (!file.Any()) return;
        var filePath = file[0];
        using var reader = XmlReader.Create(filePath, new XmlReaderSettings { Async = true });
        using var connection = _database.CreateConnection();
        connection.Open();

        var updateCommandParamValues = new List<Dictionary<string, object?>>(_capacity);
        var addCommandParamValues = new List<Dictionary<string, object?>>(_capacity);
        while (await reader.ReadAsync())
        {
            if (reader.NodeType != XmlNodeType.Element || reader.GetAttribute("ID") == null) continue;
            var id = int.Parse(reader.GetAttribute("ID"));
            var objectId = reader.GetAttribute("OBJECTID");
            var changeId = reader.GetAttribute("CHANGEID");
            var changeEndId = reader.GetAttribute("CHANGEIDEND");
            var typeId = reader.GetAttribute("TYPEID");
            var value = reader.GetAttribute("VALUE");
            var updateDate = reader.GetAttribute("UPDATEDATE");
            var startDate = reader.GetAttribute("STARTDATE");
            var endDate = reader.GetAttribute("ENDDATE");
            var queryParams = new Dictionary<string, object?> {
                { "@1", id },
                { "@2", objectId != null ? int.Parse(objectId) : null },
                { "@3", changeId != null ? int.Parse(changeId) : null },
                { "@4", changeEndId != null ? int.Parse(changeEndId) : null },
                { "@5", typeId != null ? int.Parse(typeId) : null },
                { "@6", value },
                { "@7", updateDate != null ? DateTime.Parse(updateDate) : null },
                { "@8", startDate != null ? DateTime.Parse(startDate) : null },
                { "@9", endDate != null ? DateTime.Parse(endDate) : null },
            };


            if (delta)
            {
                var count = await connection.ExecuteScalarAsync<int>(
                    selectQuery,
                    param: new Dictionary<string, object> { { "@1", id } },
                    commandTimeout: 600);
                if (count > 0)
                    updateCommandParamValues.Add(queryParams);
                else
                    addCommandParamValues.Add(queryParams);
            }
            else
            {
                addCommandParamValues.Add(queryParams);
            }

            if (addCommandParamValues.Count == condition)
            {
                ToDb(connection, insertQuery, paramNames, addCommandParamValues);
                addCommandParamValues.Clear();
            }

            if (updateCommandParamValues.Count == condition)
            {
                ToDb(connection, updateQuery, paramNames, updateCommandParamValues);
                updateCommandParamValues.Clear();
            }
        }
        if (addCommandParamValues.Count > 0)
        {
            ToDb(connection, insertQuery, paramNames, addCommandParamValues);
            addCommandParamValues.Clear();
        }

        if (updateCommandParamValues.Count > 0)
        {
            ToDb(connection, updateQuery, paramNames, updateCommandParamValues);
            updateCommandParamValues.Clear();
        }
    }

    private async Task ExtractApartmentsParams(string path, bool delta)
    {
        var paramNames = new[] { "@1", "@2", "@3", "@4", "@5", "@6", "@7", "@8", "@9" };
        const string fileMask = "AS_APARTMENTS_PARAMS_*.XML";
        var selectQuery = @"SELECT COUNT(*) FROM apartment_params WHERE id = @1";
        var insertQuery = @"INSERT INTO apartment_params VALUES (@1, @2, @3, @4, @5, @6, @7, @8, @9)";
        var updateQuery = @"UPDATE apartment_params SET object_id = @2, change_id = @3, change_end_id = @4, type_id = @5, value = @6, update_date = @7, start_date = @8, end_date = @9 WHERE id = @1";

        var file = Directory.GetFiles(path, fileMask);
        if (!file.Any()) return;
        var filePath = file[0];
        using var reader = XmlReader.Create(filePath, new XmlReaderSettings { Async = true });
        using var connection = _database.CreateConnection();
        connection.Open();

        var updateCommandParamValues = new List<Dictionary<string, object?>>(_capacity);
        var addCommandParamValues = new List<Dictionary<string, object?>>(_capacity);
        while (await reader.ReadAsync())
        {
            if (reader.NodeType != XmlNodeType.Element || reader.GetAttribute("ID") == null) continue;
            var id = int.Parse(reader.GetAttribute("ID"));
            var objectId = reader.GetAttribute("OBJECTID");
            var changeId = reader.GetAttribute("CHANGEID");
            var changeEndId = reader.GetAttribute("CHANGEIDEND");
            var typeId = reader.GetAttribute("TYPEID");
            var value = reader.GetAttribute("VALUE");
            var updateDate = reader.GetAttribute("UPDATEDATE");
            var startDate = reader.GetAttribute("STARTDATE");
            var endDate = reader.GetAttribute("ENDDATE");
            var queryParams = new Dictionary<string, object?> {
                { "@1", id },
                { "@2", objectId != null ? int.Parse(objectId) : null },
                { "@3", changeId != null ? int.Parse(changeId) : null },
                { "@4", changeEndId != null ? int.Parse(changeEndId) : null },
                { "@5", typeId != null ? int.Parse(typeId) : null },
                { "@6", value },
                { "@7", updateDate != null ? DateTime.Parse(updateDate) : null },
                { "@8", startDate != null ? DateTime.Parse(startDate) : null },
                { "@9", endDate != null ? DateTime.Parse(endDate) : null },
            };


            if (delta)
            {
                var count = await connection.ExecuteScalarAsync<int>(
                    selectQuery,
                    param: new Dictionary<string, object> { { "@1", id } },
                    commandTimeout: 600);
                if (count > 0)
                    updateCommandParamValues.Add(queryParams);
                else
                    addCommandParamValues.Add(queryParams);
            }
            else
            {
                addCommandParamValues.Add(queryParams);
            }

            if (addCommandParamValues.Count == condition)
            {
                ToDb(connection, insertQuery, paramNames, addCommandParamValues);
                addCommandParamValues.Clear();
            }

            if (updateCommandParamValues.Count == condition)
            {
                ToDb(connection, updateQuery, paramNames, updateCommandParamValues);
                updateCommandParamValues.Clear();
            }
        }
        if (addCommandParamValues.Count > 0)
        {
            ToDb(connection, insertQuery, paramNames, addCommandParamValues);
            addCommandParamValues.Clear();
        }

        if (updateCommandParamValues.Count > 0)
        {
            ToDb(connection, updateQuery, paramNames, updateCommandParamValues);
            updateCommandParamValues.Clear();
        }
    }

    private async Task ExtractHouseParams(string path, bool delta)
    {
        var paramNames = new[] { "@1", "@2", "@3", "@4", "@5", "@6", "@7", "@8", "@9" };
        const string fileMask = "AS_HOUSES_PARAMS_*.XML";
        var selectQuery = @"SELECT COUNT(*) FROM house_params WHERE id = @1";
        var insertQuery = @"INSERT INTO house_params VALUES (@1, @2, @3, @4, @5, @6, @7, @8, @9)";
        var updateQuery = @"UPDATE house_params SET object_id = @2, change_id = @3, change_end_id = @4, type_id = @5, value = @6, update_date = @7, start_date = @8, end_date = @9 WHERE id = @1";

        var file = Directory.GetFiles(path, fileMask);
        if (!file.Any()) return;
        var filePath = file[0];
        using var reader = XmlReader.Create(filePath, new XmlReaderSettings { Async = true });
        using var connection = _database.CreateConnection();
        connection.Open();

        var updateCommandParamValues = new List<Dictionary<string, object?>>(_capacity);
        var addCommandParamValues = new List<Dictionary<string, object?>>(_capacity);
        while (await reader.ReadAsync())
        {
            if (reader.NodeType != XmlNodeType.Element || reader.GetAttribute("ID") == null) continue;
            var id = int.Parse(reader.GetAttribute("ID"));
            var objectId = reader.GetAttribute("OBJECTID");
            var changeId = reader.GetAttribute("CHANGEID");
            var changeEndId = reader.GetAttribute("CHANGEIDEND");
            var typeId = reader.GetAttribute("TYPEID");
            var value = reader.GetAttribute("VALUE");
            var updateDate = reader.GetAttribute("UPDATEDATE");
            var startDate = reader.GetAttribute("STARTDATE");
            var endDate = reader.GetAttribute("ENDDATE");
            var queryParams = new Dictionary<string, object?> {
                { "@1", id },
                { "@2", objectId != null ? int.Parse(objectId) : null },
                { "@3", changeId != null ? int.Parse(changeId) : null },
                { "@4", changeEndId != null ? int.Parse(changeEndId) : null },
                { "@5", typeId != null ? int.Parse(typeId) : null },
                { "@6", value },
                { "@7", updateDate != null ? DateTime.Parse(updateDate) : null },
                { "@8", startDate != null ? DateTime.Parse(startDate) : null },
                { "@9", endDate != null ? DateTime.Parse(endDate) : null },
            };


            if (delta)
            {
                var count = await connection.ExecuteScalarAsync<int>(
                    selectQuery,
                    param: new Dictionary<string, object> { { "@1", id } },
                    commandTimeout: 600);
                if (count > 0)
                    updateCommandParamValues.Add(queryParams);
                else
                    addCommandParamValues.Add(queryParams);
            }
            else
            {
                addCommandParamValues.Add(queryParams);
            }

            if (addCommandParamValues.Count == condition)
            {
                ToDb(connection, insertQuery, paramNames, addCommandParamValues);
                addCommandParamValues.Clear();
            }

            if (updateCommandParamValues.Count == condition)
            {
                ToDb(connection, updateQuery, paramNames, updateCommandParamValues);
                updateCommandParamValues.Clear();
            }
        }
        if (addCommandParamValues.Count > 0)
        {
            ToDb(connection, insertQuery, paramNames, addCommandParamValues);
            addCommandParamValues.Clear();
        }

        if (updateCommandParamValues.Count > 0)
        {
            ToDb(connection, updateQuery, paramNames, updateCommandParamValues);
            updateCommandParamValues.Clear();
        }
    }

    private async Task ExtractSteadParams(string path, bool delta)
    {
        var paramNames = new[] { "@1", "@2", "@3", "@4", "@5", "@6", "@7", "@8", "@9" };
        const string fileMask = "AS_STEADS_PARAMS_*.XML";
        var selectQuery = @"SELECT COUNT(*) FROM stead_params WHERE id = @1";
        var insertQuery = @"INSERT INTO stead_params VALUES (@1, @2, @3, @4, @5, @6, @7, @8, @9)";
        var updateQuery = @"UPDATE stead_params SET object_id = @2, change_id = @3, change_end_id = @4, type_id = @5, value = @6, update_date = @7, start_date = @8, end_date = @9 WHERE id = @1";

        var file = Directory.GetFiles(path, fileMask);
        if (!file.Any()) return;
        var filePath = file[0];
        using var reader = XmlReader.Create(filePath, new XmlReaderSettings { Async = true });
        using var connection = _database.CreateConnection();
        connection.Open();

        var updateCommandParamValues = new List<Dictionary<string, object?>>(_capacity);
        var addCommandParamValues = new List<Dictionary<string, object?>>(_capacity);
        while (await reader.ReadAsync())
        {
            if (reader.NodeType != XmlNodeType.Element || reader.GetAttribute("ID") == null) continue;
            var id = int.Parse(reader.GetAttribute("ID"));
            var objectId = reader.GetAttribute("OBJECTID");
            var changeId = reader.GetAttribute("CHANGEID");
            var changeEndId = reader.GetAttribute("CHANGEIDEND");
            var typeId = reader.GetAttribute("TYPEID");
            var value = reader.GetAttribute("VALUE");
            var updateDate = reader.GetAttribute("UPDATEDATE");
            var startDate = reader.GetAttribute("STARTDATE");
            var endDate = reader.GetAttribute("ENDDATE");
            var queryParams = new Dictionary<string, object?> {
                { "@1", id },
                { "@2", objectId != null ? int.Parse(objectId) : null },
                { "@3", changeId != null ? int.Parse(changeId) : null },
                { "@4", changeEndId != null ? int.Parse(changeEndId) : null },
                { "@5", typeId != null ? int.Parse(typeId) : null },
                { "@6", value },
                { "@7", updateDate != null ? DateTime.Parse(updateDate) : null },
                { "@8", startDate != null ? DateTime.Parse(startDate) : null },
                { "@9", endDate != null ? DateTime.Parse(endDate) : null },
            };


            if (delta)
            {
                var count = await connection.ExecuteScalarAsync<int>(
                    selectQuery,
                    param: new Dictionary<string, object> { { "@1", id } },
                    commandTimeout: 600);
                if (count > 0)
                    updateCommandParamValues.Add(queryParams);
                else
                    addCommandParamValues.Add(queryParams);
            }
            else
            {
                addCommandParamValues.Add(queryParams);
            }

            if (addCommandParamValues.Count == condition)
            {
                ToDb(connection, insertQuery, paramNames, addCommandParamValues);
                addCommandParamValues.Clear();
            }

            if (updateCommandParamValues.Count == condition)
            {
                ToDb(connection, updateQuery, paramNames, updateCommandParamValues);
                updateCommandParamValues.Clear();
            }
        }
        if (addCommandParamValues.Count > 0)
        {
            ToDb(connection, insertQuery, paramNames, addCommandParamValues);
            addCommandParamValues.Clear();
        }

        if (updateCommandParamValues.Count > 0)
        {
            ToDb(connection, updateQuery, paramNames, updateCommandParamValues);
            updateCommandParamValues.Clear();
        }
    }

    private async Task ExtractAddrObjParams(string path, bool delta)
    {
        var paramNames = new[] { "@1", "@2", "@3", "@4", "@5", "@6", "@7", "@8", "@9" };
        const string fileMask = "AS_ADDR_OBJ_PARAMS_*.XML";
        var selectQuery = @"SELECT COUNT(*) FROM addr_obj_params WHERE id = @1";
        var insertQuery = @"INSERT INTO addr_obj_params VALUES (@1, @2, @3, @4, @5, @6, @7, @8, @9)";
        var updateQuery = @"UPDATE addr_obj_params SET object_id = @2, change_id = @3, change_end_id = @4, type_id = @5, value = @6, update_date = @7, start_date = @8, end_date = @9 WHERE id = @1";

        var file = Directory.GetFiles(path, fileMask);
        if (!file.Any()) return;
        var filePath = file[0];
        using var reader = XmlReader.Create(filePath, new XmlReaderSettings { Async = true });
        using var connection = _database.CreateConnection();
        connection.Open();

        var updateCommandParamValues = new List<Dictionary<string, object?>>(_capacity);
        var addCommandParamValues = new List<Dictionary<string, object?>>(_capacity);
        while (await reader.ReadAsync())
        {
            if (reader.NodeType != XmlNodeType.Element || reader.GetAttribute("ID") == null) continue;
            var id = int.Parse(reader.GetAttribute("ID"));
            var objectId = reader.GetAttribute("OBJECTID");
            var changeId = reader.GetAttribute("CHANGEID");
            var changeEndId = reader.GetAttribute("CHANGEIDEND");
            var typeId = reader.GetAttribute("TYPEID");
            var value = reader.GetAttribute("VALUE");
            var updateDate = reader.GetAttribute("UPDATEDATE");
            var startDate = reader.GetAttribute("STARTDATE");
            var endDate = reader.GetAttribute("ENDDATE");
            var queryParams = new Dictionary<string, object?> {
                { "@1", id },
                { "@2", objectId != null ? int.Parse(objectId) : null },
                { "@3", changeId != null ? int.Parse(changeId) : null },
                { "@4", changeEndId != null ? int.Parse(changeEndId) : null },
                { "@5", typeId != null ? int.Parse(typeId) : null },
                { "@6", value },
                { "@7", updateDate != null ? DateTime.Parse(updateDate) : null },
                { "@8", startDate != null ? DateTime.Parse(startDate) : null },
                { "@9", endDate != null ? DateTime.Parse(endDate) : null },
            };


            if (delta)
            {
                var count = await connection.ExecuteScalarAsync<int>(
                    selectQuery,
                    param: new Dictionary<string, object> { { "@1", id } },
                    commandTimeout: 600);
                if (count > 0)
                    updateCommandParamValues.Add(queryParams);
                else
                    addCommandParamValues.Add(queryParams);
            }
            else
            {
                addCommandParamValues.Add(queryParams);
            }

            if (addCommandParamValues.Count == condition)
            {
                ToDb(connection, insertQuery, paramNames, addCommandParamValues);
                addCommandParamValues.Clear();
            }

            if (updateCommandParamValues.Count == condition)
            {
                ToDb(connection, updateQuery, paramNames, updateCommandParamValues);
                updateCommandParamValues.Clear();
            }
        }
        if (addCommandParamValues.Count > 0)
        {
            ToDb(connection, insertQuery, paramNames, addCommandParamValues);
            addCommandParamValues.Clear();
        }

        if (updateCommandParamValues.Count > 0)
        {
            ToDb(connection, updateQuery, paramNames, updateCommandParamValues);
            updateCommandParamValues.Clear();
        }
    }

    private async Task ExtractAddrObjDivision(string path, bool delta)
    {
        var paramNames = new[] { "@1", "@2", "@3", "@4" };
        const string fileMask = "AS_ADDR_OBJ_DIVISION_*.XML";
        var selectQuery = @"SELECT COUNT(*) FROM addr_obj_division WHERE id = @1";
        var insertQuery = @"INSERT INTO addr_obj_division VALUES (@1, @2, @3, @4)";
        var updateQuery = @"UPDATE addr_obj_division SET parent_id = @2, child_id = @3, change_id = @4 WHERE id = @1";

        var file = Directory.GetFiles(path, fileMask);
        if (!file.Any()) return;
        var filePath = file[0];
        using var reader = XmlReader.Create(filePath, new XmlReaderSettings { Async = true });
        using var connection = _database.CreateConnection();
        connection.Open();

        var updateCommandParamValues = new List<Dictionary<string, object?>>(_capacity);
        var addCommandParamValues = new List<Dictionary<string, object?>>(_capacity);
        while (await reader.ReadAsync())
        {
            if (reader.NodeType != XmlNodeType.Element || reader.GetAttribute("ID") == null) continue;
            var id = int.Parse(reader.GetAttribute("ID"));
            var parentId = reader.GetAttribute("PARENTID");
            var changeId = reader.GetAttribute("CHANGEID");
            var childId = reader.GetAttribute("CHILDID");

            var queryParams = new Dictionary<string, object?> {
                { "@1", id },
                { "@2", parentId != null ? long.Parse(parentId) : null },
                { "@3", childId != null ? long.Parse(childId) : null },
                { "@4", changeId != null ? long.Parse(changeId) : null },
            };

            if (delta)
            {
                var count = await connection.ExecuteScalarAsync<int>(
                    selectQuery,
                    param: new Dictionary<string, object> { { "@1", id } },
                    commandTimeout: 600);
                if (count > 0)
                    updateCommandParamValues.Add(queryParams);
                else
                    addCommandParamValues.Add(queryParams);
            }
            else
            {
                addCommandParamValues.Add(queryParams);
            }

            if (addCommandParamValues.Count == condition)
            {
                ToDb(connection, insertQuery, paramNames, addCommandParamValues);
                addCommandParamValues.Clear();
            }

            if (updateCommandParamValues.Count == condition)
            {
                ToDb(connection, updateQuery, paramNames, updateCommandParamValues);
                updateCommandParamValues.Clear();
            }
        }
        if (addCommandParamValues.Count > 0)
        {
            ToDb(connection, insertQuery, paramNames, addCommandParamValues);
            addCommandParamValues.Clear();
        }

        if (updateCommandParamValues.Count > 0)
        {
            ToDb(connection, updateQuery, paramNames, updateCommandParamValues);
            updateCommandParamValues.Clear();
        }
    }

    private async Task ExtractChangeHistory(string path, bool delta)
    {
        var paramNames = new[] { "@1", "@2", "@3", "@4", "@5", "@6" };
        const string fileMask = "AS_CHANGE_HISTORY_*.XML";
        var selectQuery = @"SELECT COUNT(*) FROM change_history WHERE change_id = @1";
        var insertQuery = @"INSERT INTO change_history VALUES (@1, @2, @3, @4, @5, @6)";
        var updateQuery = @"UPDATE change_history SET object_id = @2, add_object_id = @3, oper_type_id = @4, n_doc_id = @5, change_date = @6 WHERE change_id = @1";

        var file = Directory.GetFiles(path, fileMask);
        if (!file.Any()) return;
        var filePath = file[0];
        using var reader = XmlReader.Create(filePath, new XmlReaderSettings { Async = true });
        using var connection = _database.CreateConnection();
        connection.Open();

        var updateCommandParamValues = new List<Dictionary<string, object?>>(_capacity);
        var addCommandParamValues = new List<Dictionary<string, object?>>(_capacity);
        while (await reader.ReadAsync())
        {
            if (reader.NodeType != XmlNodeType.Element || reader.GetAttribute("CHANGEID") == null) continue;
            var id = int.Parse(reader.GetAttribute("CHANGEID"));
            var objectId = reader.GetAttribute("OBJECTID");
            var addrObjId = reader.GetAttribute("ADROBJECTID");
            var operTypeId = reader.GetAttribute("OPERTYPEID");
            var nDocId = reader.GetAttribute("NDOCID");
            var changeDate = reader.GetAttribute("CHANGEDATE");

            var queryParams = new Dictionary<string, object?> {
                { "@1", id },
                { "@2", objectId != null ? long.Parse(objectId) : null },
                { "@3", addrObjId },
                { "@4", operTypeId != null ? long.Parse(operTypeId) : null },
                { "@5", nDocId != null ? long.Parse(nDocId) : null },
                { "@6", changeDate != null ? DateTime.Parse(changeDate) : null }
            };

            if (delta)
            {
                var count = await connection.ExecuteScalarAsync<int>(
                    selectQuery,
                    param: new Dictionary<string, object> { { "@1", id } },
                    commandTimeout: 600);
                if (count > 0)
                    updateCommandParamValues.Add(queryParams);
                else
                    addCommandParamValues.Add(queryParams);
            }
            else
            {
                addCommandParamValues.Add(queryParams);
            }

            if (addCommandParamValues.Count == condition)
            {
                ToDb(connection, insertQuery, paramNames, addCommandParamValues);
                addCommandParamValues.Clear();
            }

            if (updateCommandParamValues.Count == condition)
            {
                ToDb(connection, updateQuery, paramNames, updateCommandParamValues);
                updateCommandParamValues.Clear();
            }
        }
        if (addCommandParamValues.Count > 0)
        {
            ToDb(connection, insertQuery, paramNames, addCommandParamValues);
            addCommandParamValues.Clear();
        }

        if (updateCommandParamValues.Count > 0)
        {
            ToDb(connection, updateQuery, paramNames, updateCommandParamValues);
            updateCommandParamValues.Clear();
        }
    }

    private async Task ExtractMunHierarchy(string path, bool delta)
    {
        var paramNames = new[] { "@1", "@2", "@3", "@4", "@5", "@6", "@7", "@8", "@9", "@10", "@11", "@12" };
        const string fileMask = "AS_MUN_HIERARCHY_*.XML";
        var selectQuery = @"SELECT COUNT(*) FROM mun_hierarchy WHERE id = @1";
        var insertQuery = @"INSERT INTO mun_hierarchy VALUES (@1, @2, @3, @4, @5, @6, @7, @8, @9, @10, @11, @12)";
        var updateQuery = @"
        UPDATE 
            mun_hierarchy 
        SET 
            object_id = @2, 
            parent_obj_id = @3
            change_id = @4, 
            oktmo = @5,
            prev_id = @6,
            next_id = @7,
            update_date = @8, 
            start_date = @9, 
            end_date = @10,
            is_active = @11, 
            path = @12            
        WHERE 
            id = @1";

        var file = Directory.GetFiles(path, fileMask);
        if (!file.Any()) return;
        var filePath = file[0];
        using var reader = XmlReader.Create(filePath, new XmlReaderSettings { Async = true });
        using var connection = _database.CreateConnection();
        connection.Open();

        var updateCommandParamValues = new List<Dictionary<string, object?>>(_capacity);
        var addCommandParamValues = new List<Dictionary<string, object?>>(_capacity);
        while (await reader.ReadAsync())
        {
            if (reader.NodeType != XmlNodeType.Element || reader.GetAttribute("ID") == null) continue;
            var id = int.Parse(reader.GetAttribute("ID"));
            var objectId = reader.GetAttribute("OBJECTID");
            var parentObjId = reader.GetAttribute("PARENTOBJID");
            var changeId = reader.GetAttribute("CHANGEID");
            var oktmo = reader.GetAttribute("OKTMO");
            var prevId = reader.GetAttribute("PREVID");
            var nextId = reader.GetAttribute("NEXTID");
            var updateDate = reader.GetAttribute("UPDATEDATE");
            var startDate = reader.GetAttribute("STARTDATE");
            var endDate = reader.GetAttribute("ENDDATE");
            var isActive = reader.GetAttribute("ISACTIVE");
            var pathProp = reader.GetAttribute("PATH");
            var queryParams = new Dictionary<string, object?> {
                { "@1", id },
                { "@2", objectId != null ? int.Parse(objectId) : null },
                { "@3", parentObjId != null ? int.Parse(parentObjId) : null },
                { "@4", changeId != null ? int.Parse(changeId) : null },
                { "@5", oktmo },
                { "@6", prevId != null ? int.Parse(prevId) : null },
                { "@7", nextId != null ? int.Parse(nextId) : null },
                { "@8", updateDate != null ? DateTime.Parse(updateDate) : null },
                { "@9", startDate != null ? DateTime.Parse(startDate) : null },
                { "@10", endDate != null ? DateTime.Parse(endDate) : null },
                { "@11", isActive != null ? int.Parse(isActive) : null },
                { "@12", pathProp },
            };


            if (delta)
            {
                var count = await connection.ExecuteScalarAsync<int>(
                    selectQuery,
                    param: new Dictionary<string, object> { { "@1", id } },
                    commandTimeout: 600);
                if (count > 0)
                    updateCommandParamValues.Add(queryParams);
                else
                    addCommandParamValues.Add(queryParams);
            }
            else
            {
                addCommandParamValues.Add(queryParams);
            }

            if (addCommandParamValues.Count == condition)
            {
                ToDb(connection, insertQuery, paramNames, addCommandParamValues);
                addCommandParamValues.Clear();
            }

            if (updateCommandParamValues.Count == condition)
            {
                ToDb(connection, updateQuery, paramNames, updateCommandParamValues);
                updateCommandParamValues.Clear();
            }
        }
        if (addCommandParamValues.Count > 0)
        {
            ToDb(connection, insertQuery, paramNames, addCommandParamValues);
            addCommandParamValues.Clear();
        }

        if (updateCommandParamValues.Count > 0)
        {
            ToDb(connection, updateQuery, paramNames, updateCommandParamValues);
            updateCommandParamValues.Clear();
        }
    }

    private async Task ExtractAdmHierarchy(string path, bool delta)
    {
        var paramNames = new[] { "@1", "@2", "@3", "@4", "@5", "@6", "@7", "@8", "@9", "@10", "@11", "@12", "@13", "@14", "@15", "@16", "@17" };
        const string fileMask = "AS_ADM_HIERARCHY_*.XML";
        var selectQuery = @"SELECT COUNT(*) FROM adm_hierarchy WHERE id = @1";
        var insertQuery = @"INSERT INTO adm_hierarchy VALUES (@1, @2, @3, @4, @5, @6, @7, @8, @9, @10, @11, @12, @13, @14, @15, @16, @17)";
        var updateQuery = @"
        UPDATE 
            adm_hierarchy 
        SET 
            object_id = @2, 
            parent_obj_id = @3
            change_id = @4,
            region_code = @5,
            area_code = @6,
            city_code = @7,
            place_code = @8,
            plan_code = @9,
            street_code = @19,
            prev_id = @11,
            next_id = @12,
            update_date = @13, 
            start_date = @14, 
            end_date = @15,
            is_active = @16, 
            path = @17            
        WHERE 
            id = @1";

        var file = Directory.GetFiles(path, fileMask);
        if (!file.Any()) return;
        var filePath = file[0];
        using var reader = XmlReader.Create(filePath, new XmlReaderSettings { Async = true });
        using var connection = _database.CreateConnection();
        connection.Open();

        var updateCommandParamValues = new List<Dictionary<string, object?>>(_capacity);
        var addCommandParamValues = new List<Dictionary<string, object?>>(_capacity);
        while (await reader.ReadAsync())
        {
            if (reader.NodeType != XmlNodeType.Element || reader.GetAttribute("ID") == null) continue;
            var id = int.Parse(reader.GetAttribute("ID"));
            var objectId = reader.GetAttribute("OBJECTID");
            var parentObjId = reader.GetAttribute("PARENTOBJID");
            var changeId = reader.GetAttribute("CHANGEID");

            var regionCode = reader.GetAttribute("REGIONCODE");
            var areaCode = reader.GetAttribute("AREACODE");
            var cityCode = reader.GetAttribute("CITYCODE");
            var placeCode = reader.GetAttribute("PLACECODE");
            var planCode = reader.GetAttribute("PLANCODE");
            var streetCode = reader.GetAttribute("STREETCODE");

            var prevId = reader.GetAttribute("PREVID");
            var nextId = reader.GetAttribute("NEXTID");
            var updateDate = reader.GetAttribute("UPDATEDATE");
            var startDate = reader.GetAttribute("STARTDATE");
            var endDate = reader.GetAttribute("ENDDATE");
            var isActive = reader.GetAttribute("ISACTIVE");
            var pathProp = reader.GetAttribute("PATH");
            var queryParams = new Dictionary<string, object?> {
                { "@1", id },
                { "@2", objectId != null ? int.Parse(objectId) : null },
                { "@3", parentObjId != null ? int.Parse(parentObjId) : null },
                { "@4", changeId != null ? int.Parse(changeId) : null },
                { "@5", regionCode },
                { "@6", areaCode },
                { "@7", cityCode },
                { "@8", placeCode },
                { "@9", planCode },
                { "@10", streetCode },
                { "@11", prevId != null ? int.Parse(prevId) : null },
                { "@12", nextId != null ? int.Parse(nextId) : null },
                { "@13", updateDate != null ? DateTime.Parse(updateDate) : null },
                { "@14", startDate != null ? DateTime.Parse(startDate) : null },
                { "@15", endDate != null ? DateTime.Parse(endDate) : null },
                { "@16", isActive != null ? int.Parse(isActive) : null },
                { "@17", pathProp },
            };

            if (delta)
            {
                var count = await connection.ExecuteScalarAsync<int>(
                    selectQuery,
                    param: new Dictionary<string, object> { { "@1", id } },
                    commandTimeout: 600);
                if (count > 0)
                    updateCommandParamValues.Add(queryParams);
                else
                    addCommandParamValues.Add(queryParams);
            }
            else
            {
                addCommandParamValues.Add(queryParams);
            }

            if (addCommandParamValues.Count == condition)
            {
                ToDb(connection, insertQuery, paramNames, addCommandParamValues);
                addCommandParamValues.Clear();
            }

            if (updateCommandParamValues.Count == condition)
            {
                ToDb(connection, updateQuery, paramNames, updateCommandParamValues);
                updateCommandParamValues.Clear();
            }
        }
        if (addCommandParamValues.Count > 0)
        {
            ToDb(connection, insertQuery, paramNames, addCommandParamValues);
            addCommandParamValues.Clear();
        }

        if (updateCommandParamValues.Count > 0)
        {
            ToDb(connection, updateQuery, paramNames, updateCommandParamValues);
            updateCommandParamValues.Clear();
        }
    }

    private async Task ExtractCarplaces(string path, bool delta)
    {
        await Task.Delay(1);
    }

    private async Task ExtractRooms(string path, bool delta)
    {
        var paramNames = new[] { "@1", "@2", "@3", "@4", "@5", "@6", "@7", "@8", "@9", "@10", "@11", "@12", "@13", "@14" };
        const string fileMask = "AS_ROOMS_*.XML";
        var selectQuery = @"SELECT COUNT(*) FROM room WHERE id = @1";
        var insertQuery = @"INSERT INTO room VALUES (@1, @2, @3, @4, @5, @6, @7, @8, @9, @10, @11, @12, @13, @14)";
        var updateQuery = @"
        UPDATE 
            room 
        SET 
            object_id = @2, 
            object_guid = @3
            change_id = @4,
            number = @5,
            room_type = @6,
            oper_type_id = @7,
            prev_id = @8,
            next_id = @9,
            update_date = @10, 
            start_date = @11, 
            end_date = @12,
            is_actual = @13, 
            is_active = @14        
        WHERE 
            id = @1";

        var file = Directory.GetFiles(path, fileMask);
        if (!file.Any()) return;
        var filePath = file.FirstOrDefault(x => !x.Contains("AS_ROOMS_PARAMS"));
        if (filePath == null) return;

        using var reader = XmlReader.Create(filePath, new XmlReaderSettings { Async = true });
        using var connection = _database.CreateConnection();
        connection.Open();

        var updateCommandParamValues = new List<Dictionary<string, object?>>(_capacity);
        var addCommandParamValues = new List<Dictionary<string, object?>>(_capacity);
        while (await reader.ReadAsync())
        {
            if (reader.NodeType != XmlNodeType.Element || reader.GetAttribute("ID") == null) continue;
            var id = int.Parse(reader.GetAttribute("ID"));
            var objectId = reader.GetAttribute("OBJECTID");
            var objectGuid = reader.GetAttribute("OBJECTGUID");
            var changeId = reader.GetAttribute("CHANGEID");
            var number = reader.GetAttribute("NUMBER");
            var roomType = reader.GetAttribute("ROOMTYPE");
            var operTypeId = reader.GetAttribute("OPERTYPEID");
            var prevId = reader.GetAttribute("PREVID");
            var nextId = reader.GetAttribute("NEXTID");
            var updateDate = reader.GetAttribute("UPDATEDATE");
            var startDate = reader.GetAttribute("STARTDATE");
            var endDate = reader.GetAttribute("ENDDATE");
            var isActual = reader.GetAttribute("ISACTUAL");
            var isActive = reader.GetAttribute("ISACTIVE");
            var queryParams = new Dictionary<string, object?> {
                { "@1", id },
                { "@2", objectId != null ? int.Parse(objectId) : null },
                { "@3", objectGuid },
                { "@4", changeId != null ? int.Parse(changeId) : null },
                { "@5", number },
                { "@6", roomType != null ? int.Parse(roomType) : null },
                { "@7", operTypeId != null ? int.Parse(operTypeId) : null},
                { "@8", prevId != null ? int.Parse(prevId) : null },
                { "@9", nextId != null ? int.Parse(nextId) : null },
                { "@10", updateDate != null ? DateTime.Parse(updateDate) : null },
                { "@11", startDate != null ? DateTime.Parse(startDate) : null },
                { "@12", endDate != null ? DateTime.Parse(endDate) : null },
                { "@13", isActual != null ? int.Parse(isActual) : null },
                { "@14", isActive != null ? int.Parse(isActive) : null },
            };

            if (delta)
            {
                var count = await connection.ExecuteScalarAsync<int>(
                    selectQuery,
                    param: new Dictionary<string, object> { { "@1", id } },
                    commandTimeout: 600);
                if (count > 0)
                    updateCommandParamValues.Add(queryParams);
                else
                    addCommandParamValues.Add(queryParams);
            }
            else
            {
                addCommandParamValues.Add(queryParams);
            }

            if (addCommandParamValues.Count == condition)
            {
                ToDb(connection, insertQuery, paramNames, addCommandParamValues);
                addCommandParamValues.Clear();
            }

            if (updateCommandParamValues.Count == condition)
            {
                ToDb(connection, updateQuery, paramNames, updateCommandParamValues);
                updateCommandParamValues.Clear();
            }
        }
        if (addCommandParamValues.Count > 0)
        {
            ToDb(connection, insertQuery, paramNames, addCommandParamValues);
            addCommandParamValues.Clear();
        }

        if (updateCommandParamValues.Count > 0)
        {
            ToDb(connection, updateQuery, paramNames, updateCommandParamValues);
            updateCommandParamValues.Clear();
        }
    }

    private async Task ExtractApartments(string path, bool delta)
    {
        var paramNames = new[] { "@1", "@2", "@3", "@4", "@5", "@6", "@7", "@8", "@9", "@10", "@11", "@12", "@13", "@14" };
        const string fileMask = "AS_APARTMENTS_*.XML";
        var selectQuery = @"SELECT COUNT(*) FROM apartment WHERE id = @1";
        var insertQuery = @"INSERT INTO apartment VALUES (@1, @2, @3, @4, @5, @6, @7, @8, @9, @10, @11, @12, @13, @14)";
        var updateQuery = @"
        UPDATE 
            apartment 
        SET 
            object_id = @2, 
            object_guid = @3
            change_id = @4,
            number = @5,
            apart_type = @6,
            oper_type_id = @7,
            prev_id = @8,
            next_id = @9,
            update_date = @10, 
            start_date = @11, 
            end_date = @12,
            is_actual = @13, 
            is_active = @14        
        WHERE 
            id = @1";

        var file = Directory.GetFiles(path, fileMask);
        if (!file.Any()) return;
        var filePath = file.FirstOrDefault(x => !x.Contains("AS_APARTMENTS_PARAMS"));
        if (filePath == null) return;

        using var reader = XmlReader.Create(filePath, new XmlReaderSettings { Async = true });
        using var connection = _database.CreateConnection();
        connection.Open();

        var updateCommandParamValues = new List<Dictionary<string, object?>>(_capacity);
        var addCommandParamValues = new List<Dictionary<string, object?>>(_capacity);
        while (await reader.ReadAsync())
        {
            if (reader.NodeType != XmlNodeType.Element || reader.GetAttribute("ID") == null) continue;
            var id = int.Parse(reader.GetAttribute("ID"));
            var objectId = reader.GetAttribute("OBJECTID");
            var objectGuid = reader.GetAttribute("OBJECTGUID");
            var changeId = reader.GetAttribute("CHANGEID");
            var number = reader.GetAttribute("NUMBER");
            var apartType = reader.GetAttribute("APARTTYPE");
            var operTypeId = reader.GetAttribute("OPERTYPEID");
            var prevId = reader.GetAttribute("PREVID");
            var nextId = reader.GetAttribute("NEXTID");
            var updateDate = reader.GetAttribute("UPDATEDATE");
            var startDate = reader.GetAttribute("STARTDATE");
            var endDate = reader.GetAttribute("ENDDATE");
            var isActual = reader.GetAttribute("ISACTUAL");
            var isActive = reader.GetAttribute("ISACTIVE");
            var queryParams = new Dictionary<string, object?> {
                { "@1", id },
                { "@2", objectId != null ? int.Parse(objectId) : null },
                { "@3", objectGuid },
                { "@4", changeId != null ? int.Parse(changeId) : null },
                { "@5", number },
                { "@6", apartType != null ? int.Parse(apartType) : null },
                { "@7", operTypeId != null ? int.Parse(operTypeId) : null},
                { "@8", prevId != null ? int.Parse(prevId) : null },
                { "@9", nextId != null ? int.Parse(nextId) : null },
                { "@10", updateDate != null ? DateTime.Parse(updateDate) : null },
                { "@11", startDate != null ? DateTime.Parse(startDate) : null },
                { "@12", endDate != null ? DateTime.Parse(endDate) : null },
                { "@13", isActual != null ? int.Parse(isActual) : null },
                { "@14", isActive != null ? int.Parse(isActive) : null },
            };

            if (delta)
            {
                var count = await connection.ExecuteScalarAsync<int>(
                    selectQuery,
                    param: new Dictionary<string, object> { { "@1", id } },
                    commandTimeout: 600);
                if (count > 0)
                    updateCommandParamValues.Add(queryParams);
                else
                    addCommandParamValues.Add(queryParams);
            }
            else
            {
                addCommandParamValues.Add(queryParams);
            }

            if (addCommandParamValues.Count == condition)
            {
                ToDb(connection, insertQuery, paramNames, addCommandParamValues);
                addCommandParamValues.Clear();
            }

            if (updateCommandParamValues.Count == condition)
            {
                ToDb(connection, updateQuery, paramNames, updateCommandParamValues);
                updateCommandParamValues.Clear();
            }
        }
        if (addCommandParamValues.Count > 0)
        {
            ToDb(connection, insertQuery, paramNames, addCommandParamValues);
            addCommandParamValues.Clear();
        }

        if (updateCommandParamValues.Count > 0)
        {
            ToDb(connection, updateQuery, paramNames, updateCommandParamValues);
            updateCommandParamValues.Clear();
        }
    }

    private async Task ExtractSteads(string path, bool delta)
    {
        var paramNames = new[] { "@1", "@2", "@3", "@4", "@5", "@6", "@7", "@8", "@9", "@10", "@11", "@12", "@13" };
        const string fileMask = "AS_STEADS_*.XML";
        var selectQuery = @"SELECT COUNT(*) FROM stead WHERE id = @1";
        var insertQuery = @"INSERT INTO stead VALUES (@1, @2, @3, @4, @5, @6, @7, @8, @9, @10, @11, @12, @13)";
        var updateQuery = @"
        UPDATE 
            stead
        SET 
            object_id = @2, 
            object_guid = @3
            change_id = @4,
            number = @5,
            oper_type_id = @6,
            prev_id = @7,
            next_id = @8,
            update_date = @9,
            start_date = @10, 
            end_date = @11,
            is_actual = @12, 
            is_active = @13        
        WHERE 
            id = @1";

        var file = Directory.GetFiles(path, fileMask);
        if (!file.Any()) return;
        var filePath = file.FirstOrDefault(x => !x.Contains("AS_STEADS_PARAMS"));
        if (filePath == null) return;

        using var reader = XmlReader.Create(filePath, new XmlReaderSettings { Async = true });
        using var connection = _database.CreateConnection();
        connection.Open();

        var updateCommandParamValues = new List<Dictionary<string, object?>>(_capacity);
        var addCommandParamValues = new List<Dictionary<string, object?>>(_capacity);
        while (await reader.ReadAsync())
        {
            if (reader.NodeType != XmlNodeType.Element || reader.GetAttribute("ID") == null) continue;
            var id = int.Parse(reader.GetAttribute("ID"));
            var objectId = reader.GetAttribute("OBJECTID");
            var objectGuid = reader.GetAttribute("OBJECTGUID");
            var changeId = reader.GetAttribute("CHANGEID");
            var number = reader.GetAttribute("NUMBER");
            var operTypeId = reader.GetAttribute("OPERTYPEID");
            var prevId = reader.GetAttribute("PREVID");
            var nextId = reader.GetAttribute("NEXTID");
            var updateDate = reader.GetAttribute("UPDATEDATE");
            var startDate = reader.GetAttribute("STARTDATE");
            var endDate = reader.GetAttribute("ENDDATE");
            var isActual = reader.GetAttribute("ISACTUAL");
            var isActive = reader.GetAttribute("ISACTIVE");
            var queryParams = new Dictionary<string, object?> {
                { "@1", id },
                { "@2", objectId != null ? int.Parse(objectId) : null },
                { "@3", objectGuid },
                { "@4", changeId != null ? int.Parse(changeId) : null },
                { "@5", number },
                { "@6", operTypeId != null ? int.Parse(operTypeId) : null},
                { "@7", prevId != null ? int.Parse(prevId) : null },
                { "@8", nextId != null ? int.Parse(nextId) : null },
                { "@9", updateDate != null ? DateTime.Parse(updateDate) : null },
                { "@10", startDate != null ? DateTime.Parse(startDate) : null },
                { "@11", endDate != null ? DateTime.Parse(endDate) : null },
                { "@12", isActual != null ? int.Parse(isActual) : null },
                { "@13", isActive != null ? int.Parse(isActive) : null },
            };

            if (delta)
            {
                var count = await connection.ExecuteScalarAsync<int>(
                    selectQuery,
                    param: new Dictionary<string, object> { { "@1", id } },
                    commandTimeout: 600);
                if (count > 0)
                    updateCommandParamValues.Add(queryParams);
                else
                    addCommandParamValues.Add(queryParams);
            }
            else
            {
                addCommandParamValues.Add(queryParams);
            }

            if (addCommandParamValues.Count == condition)
            {
                ToDb(connection, insertQuery, paramNames, addCommandParamValues);
                addCommandParamValues.Clear();
            }

            if (updateCommandParamValues.Count == condition)
            {
                ToDb(connection, updateQuery, paramNames, updateCommandParamValues);
                updateCommandParamValues.Clear();
            }
        }
        if (addCommandParamValues.Count > 0)
        {
            ToDb(connection, insertQuery, paramNames, addCommandParamValues);
            addCommandParamValues.Clear();
        }

        if (updateCommandParamValues.Count > 0)
        {
            ToDb(connection, updateQuery, paramNames, updateCommandParamValues);
            updateCommandParamValues.Clear();
        }
    }

    private async Task ExtractHouses(string path, bool delta)
    {
        var paramNames = new[] { "@1", "@2", "@3", "@4", "@5", "@6", "@7", "@8", "@9", "@10", "@11", "@12", "@13", "@14", "@15", "@16", "@17", "@18" };
        const string fileMask = "AS_HOUSES_*.XML";
        var selectQuery = @"SELECT COUNT(*) FROM house WHERE id = @1";
        var insertQuery = @"INSERT INTO house VALUES (@1, @2, @3, @4, @5, @6, @7, @8, @9, @10, @11, @12, @13, @14, @14, @15, @16, @18)";
        var updateQuery = @"
        UPDATE 
            house 
        SET 
            object_id = @2, 
            object_guid = @3
            change_id = @4,
            house_num = @5,
            add_num1 = @6,
            add_num2 = @7,
            house_type = @8,
            add_type1 = @9,
            add_type2 = @10,
            oper_type_id = @11,
            prev_id = @12,
            next_id = @13,
            update_date = @14, 
            start_date = @15, 
            end_date = @16,
            is_actual = @17, 
            is_active = @18        
        WHERE 
            id = @1";

        var file = Directory.GetFiles(path, fileMask);
        if (!file.Any()) return;
        var filePath = file.FirstOrDefault(x => !x.Contains("AS_HOUSES_PARAMS"));
        if (filePath == null) return;

        using var reader = XmlReader.Create(filePath, new XmlReaderSettings { Async = true });
        using var connection = _database.CreateConnection();
        connection.Open();

        var updateCommandParamValues = new List<Dictionary<string, object?>>(_capacity);
        var addCommandParamValues = new List<Dictionary<string, object?>>(_capacity);
        while (await reader.ReadAsync())
        {
            if (reader.NodeType != XmlNodeType.Element || reader.GetAttribute("ID") == null) continue;
            var id = int.Parse(reader.GetAttribute("ID"));
            var objectId = reader.GetAttribute("OBJECTID");
            var objectGuid = reader.GetAttribute("OBJECTGUID");
            var changeId = reader.GetAttribute("CHANGEID");
            var houseNum = reader.GetAttribute("HOUSENUM");
            var addNum1 = reader.GetAttribute("ADDNUM1");
            var addNum2 = reader.GetAttribute("ADDNUM2");
            var houseType = reader.GetAttribute("HOUSETYPE");
            var addType1 = reader.GetAttribute("ADDTYPE1");
            var addType2 = reader.GetAttribute("ADDTYPE2");
            var operTypeId = reader.GetAttribute("OPERTYPEID");
            var prevId = reader.GetAttribute("PREVID");
            var nextId = reader.GetAttribute("NEXTID");
            var updateDate = reader.GetAttribute("UPDATEDATE");
            var startDate = reader.GetAttribute("STARTDATE");
            var endDate = reader.GetAttribute("ENDDATE");
            var isActual = reader.GetAttribute("ISACTUAL");
            var isActive = reader.GetAttribute("ISACTIVE");
            var queryParams = new Dictionary<string, object?> {
                { "@1", id },
                { "@2", objectId != null ? int.Parse(objectId) : null },
                { "@3", objectGuid },
                { "@4", changeId != null ? int.Parse(changeId) : null },
                { "@5", houseNum },
                { "@6", addNum1 },
                { "@7", addNum2 },
                { "@8", houseType != null ? int.Parse(houseType) : null },
                { "@9", addType1 != null ? int.Parse(addType1) : null },
                { "@10", addType2 != null ? int.Parse(addType2) : null },
                { "@11", operTypeId != null ? int.Parse(operTypeId) : null},
                { "@12", prevId != null ? int.Parse(prevId) : null },
                { "@13", nextId != null ? int.Parse(nextId) : null },
                { "@14", updateDate != null ? DateTime.Parse(updateDate) : null },
                { "@15", startDate != null ? DateTime.Parse(startDate) : null },
                { "@16", endDate != null ? DateTime.Parse(endDate) : null },
                { "@17", isActual != null ? int.Parse(isActual) : null },
                { "@18", isActive != null ? int.Parse(isActive) : null },
            };

            if (delta)
            {
                var count = await connection.ExecuteScalarAsync<int>(
                    selectQuery,
                    param: new Dictionary<string, object> { { "@1", id } },
                    commandTimeout: 600);
                if (count > 0)
                    updateCommandParamValues.Add(queryParams);
                else
                    addCommandParamValues.Add(queryParams);
            }
            else
            {
                addCommandParamValues.Add(queryParams);
            }

            if (addCommandParamValues.Count == condition)
            {
                ToDb(connection, insertQuery, paramNames, addCommandParamValues);
                addCommandParamValues.Clear();
            }

            if (updateCommandParamValues.Count == condition)
            {
                ToDb(connection, updateQuery, paramNames, updateCommandParamValues);
                updateCommandParamValues.Clear();
            }
        }
        if (addCommandParamValues.Count > 0)
        {
            ToDb(connection, insertQuery, paramNames, addCommandParamValues);
            addCommandParamValues.Clear();
        }

        if (updateCommandParamValues.Count > 0)
        {
            ToDb(connection, updateQuery, paramNames, updateCommandParamValues);
            updateCommandParamValues.Clear();
        }
    }

    private async Task ExtractAddrObj(string path, bool delta)
    {
        var paramNames = new[] { "@1", "@2", "@3", "@4", "@5", "@6", "@7", "@8", "@9", "@10", "@11", "@12", "@13", "@14", "@15" };
        const string fileMask = "AS_ADDR_OBJ_*.XML";
        var selectQuery = @"SELECT COUNT(*) FROM addr_obj WHERE id = @1";
        var insertQuery = @"INSERT INTO addr_obj VALUES (@1, @2, @3, @4, @5, @6, @7, @8, @9, @10, @11, @12, @13, @14, @15)";
        var updateQuery = @"
        UPDATE 
            addr_obj 
        SET 
            object_id = @2, 
            object_guid = @3
            change_id = @4,
            name = @5,
            type_name = @6,
            level = @7,
            oper_type_id = @8,
            prev_id = @9,
            next_id = @10,
            update_date = @11, 
            start_date = @12, 
            end_date = @13,
            is_actual = @14, 
            is_active = @15,          
        WHERE 
            id = @1";

        var file = Directory.GetFiles(path, fileMask);
        if (!file.Any()) return;
        var filePath = file.FirstOrDefault(x => !x.Contains("AS_ADDR_OBJ_PARAMS") && !x.Contains("AS_ADDR_OBJ_DIVISION"));
        if (filePath == null) return;

        using var reader = XmlReader.Create(filePath, new XmlReaderSettings { Async = true });
        using var connection = _database.CreateConnection();
        connection.Open();

        var updateCommandParamValues = new List<Dictionary<string, object?>>(_capacity);
        var addCommandParamValues = new List<Dictionary<string, object?>>(_capacity);
        while (await reader.ReadAsync())
        {
            if (reader.NodeType != XmlNodeType.Element || reader.GetAttribute("ID") == null) continue;
            var id = int.Parse(reader.GetAttribute("ID"));
            var objectId = reader.GetAttribute("OBJECTID");
            var objectGuid = reader.GetAttribute("OBJECTGUID");
            var changeId = reader.GetAttribute("CHANGEID");
            var name = reader.GetAttribute("NAME");
            var typeName = reader.GetAttribute("TYPENAME");
            var level = reader.GetAttribute("LEVEL");
            var operTypeId = reader.GetAttribute("OPERTYPEID");
            var prevId = reader.GetAttribute("PREVID");
            var nextId = reader.GetAttribute("NEXTID");
            var updateDate = reader.GetAttribute("UPDATEDATE");
            var startDate = reader.GetAttribute("STARTDATE");
            var endDate = reader.GetAttribute("ENDDATE");
            var isActual = reader.GetAttribute("ISACTUAL");
            var isActive = reader.GetAttribute("ISACTIVE");
            var queryParams = new Dictionary<string, object?> {
                { "@1", id },
                { "@2", objectId != null ? int.Parse(objectId) : null },
                { "@3", objectGuid },
                { "@4", changeId != null ? int.Parse(changeId) : null },
                { "@5", name },
                { "@6", typeName },
                { "@7", level },
                { "@8", operTypeId != null ? int.Parse(operTypeId) : null},
                { "@9", prevId != null ? int.Parse(prevId) : null },
                { "@10", nextId != null ? int.Parse(nextId) : null },
                { "@11", updateDate != null ? DateTime.Parse(updateDate) : null },
                { "@12", startDate != null ? DateTime.Parse(startDate) : null },
                { "@13", endDate != null ? DateTime.Parse(endDate) : null },
                { "@14", isActual != null ? int.Parse(isActual) : null },
                { "@15", isActive != null ? int.Parse(isActive) : null },
            };

            if (delta)
            {
                var count = await connection.ExecuteScalarAsync<int>(
                    selectQuery,
                    param: new Dictionary<string, object> { { "@1", id } },
                    commandTimeout: 600);
                if (count > 0)
                    updateCommandParamValues.Add(queryParams);
                else
                    addCommandParamValues.Add(queryParams);
            }
            else
            {
                addCommandParamValues.Add(queryParams);
            }

            if (addCommandParamValues.Count == condition)
            {
                ToDb(connection, insertQuery, paramNames, addCommandParamValues);
                addCommandParamValues.Clear();
            }

            if (updateCommandParamValues.Count == condition)
            {
                ToDb(connection, updateQuery, paramNames, updateCommandParamValues);
                updateCommandParamValues.Clear();
            }
        }
        if (addCommandParamValues.Count > 0)
        {
            ToDb(connection, insertQuery, paramNames, addCommandParamValues);
            addCommandParamValues.Clear();
        }

        if (updateCommandParamValues.Count > 0)
        {
            ToDb(connection, updateQuery, paramNames, updateCommandParamValues);
            updateCommandParamValues.Clear();
        }
    }

    private async Task ExtractReestrObjects(string path, bool delta)
    {
        var paramNames = new[] { "@1", "@2", "@3", "@4", "@5", "@6", "@7" };
        const string fileMask = "AS_REESTR_OBJECTS_*.XML";
        var selectQuery = @"SELECT COUNT(*) FROM reestr_object WHERE object_id = @1";
        var insertQuery = @"INSERT INTO reestr_object VALUES (@1, @2, @3, @4, @5, @6, @7)";
        var updateQuery = @"
        UPDATE 
            reestr_object 
        SET 
            create_date = @2, 
            change_id = @3,
            level_id = @4,
            update_date = @5,
            object_guid = @6
            is_active = @7 
        WHERE 
            object_id = @1";

        var file = Directory.GetFiles(path, fileMask);
        if (!file.Any()) return;
        var filePath = file[0];

        using var reader = XmlReader.Create(filePath, new XmlReaderSettings { Async = true });
        using var connection = _database.CreateConnection();
        connection.Open();

        var updateCommandParamValues = new List<Dictionary<string, object?>>(_capacity);
        var addCommandParamValues = new List<Dictionary<string, object?>>(_capacity);
        while (await reader.ReadAsync())
        {
            if (reader.NodeType != XmlNodeType.Element || reader.GetAttribute("OBJECTID") == null) continue;
            var id = int.Parse(reader.GetAttribute("OBJECTID"));
            var createDate = reader.GetAttribute("CREATEDATE");
            var changeId = reader.GetAttribute("CHANGEID");
            var levelId = reader.GetAttribute("LEVELID");
            var updateDate = reader.GetAttribute("UPDATEDATE");
            var objectGuid = reader.GetAttribute("OBJECTGUID");
            var isActive = reader.GetAttribute("ISACTIVE");


            var queryParams = new Dictionary<string, object?> {
                { "@1", id },
                { "@2", createDate != null ? DateTime.Parse(createDate) : null },
                { "@3", changeId != null ? int.Parse(changeId) : null },
                { "@4", levelId != null ? int.Parse(levelId) : null },
                { "@5", updateDate != null ? DateTime.Parse(updateDate) : null },
                { "@6", objectGuid },
                { "@7", isActive != null ? int.Parse(isActive) : null }
            };

            if (delta)
            {
                var count = await connection.ExecuteScalarAsync<int>(
                    selectQuery,
                    param: new Dictionary<string, object> { { "@1", id } },
                    commandTimeout: 600);
                if (count > 0)
                    updateCommandParamValues.Add(queryParams);
                else
                    addCommandParamValues.Add(queryParams);
            }
            else
            {
                addCommandParamValues.Add(queryParams);
            }

            if (addCommandParamValues.Count == condition)
            {
                ToDb(connection, insertQuery, paramNames, addCommandParamValues);
                addCommandParamValues.Clear();
            }

            if (updateCommandParamValues.Count == condition)
            {
                ToDb(connection, updateQuery, paramNames, updateCommandParamValues);
                updateCommandParamValues.Clear();
            }
        }
        if (addCommandParamValues.Count > 0)
        {
            ToDb(connection, insertQuery, paramNames, addCommandParamValues);
            addCommandParamValues.Clear();
        }

        if (updateCommandParamValues.Count > 0)
        {
            ToDb(connection, updateQuery, paramNames, updateCommandParamValues);
            updateCommandParamValues.Clear();
        }
    }
    #endregion
    public async Task ExtractData(string path, bool delta = false)
    {
        await ExtractCarplacesParams(path, delta);
        await ExtractRoomParams(path, delta);
        await ExtractNormativeDocs(path, delta);
        await ExtractApartmentsParams(path, delta);
        await ExtractHouseParams(path, delta);
        await ExtractSteadParams(path, delta);
        await ExtractAddrObjParams(path, delta);
        await ExtractAddrObjDivision(path, delta);
        await ExtractChangeHistory(path, delta);
        await ExtractMunHierarchy(path, delta);
        await ExtractAdmHierarchy(path, delta);
        // await ExtractCarplaces(path, delta);
        await ExtractRooms(path, delta);
        await ExtractApartments(path, delta);
        await ExtractSteads(path, delta);
        await ExtractAddrObj(path, delta);
        await ExtractReestrObjects(path, delta);
        await ExtractHouses(path, delta);
    }


    private void ToDb(IDbConnection conn, string commandText, string[] paramNames, List<Dictionary<string, object?>> queryParams)
    {
        using var transaction = conn.BeginTransaction();
        var command = conn.CreateCommand();
        command.CommandText = commandText;

        foreach (var item in paramNames)
        {
            var p = command.CreateParameter();
            p.ParameterName = item;
            command.Parameters.Add(p);
        }

        foreach (var item in queryParams)
        {
            foreach (var i in item)
            {
                var parameter = command.Parameters[i.Key] as IDbDataParameter;
                parameter.Value = i.Value ?? DBNull.Value;
            }
            command.ExecuteNonQuery();
        }
        transaction.Commit();
    }

}