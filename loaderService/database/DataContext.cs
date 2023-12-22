using System.Data;
using Microsoft.Data.Sqlite;
using Dapper;

namespace loaderService.Database;

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

                -- таблицы с параметрами -----------------------------

                CREATE TABLE IF NOT EXISTS 
                add_house_types (
                    id INTEGER,
                    name TEXT,
                    short_name TEXT,
                    desc TEXT,
                    is_active INTEGER,
                    update_date TEXT,
                    start_date TEXT,
                    end_date TEXT
                );

                CREATE TABLE IF NOT EXISTS 
                addr_obj_types (
                    id INTEGER,
                    level INTEGER,
                    name TEXT,
                    short_name TEXT,
                    desc TEXT,
                    is_active INTEGER,
                    update_date TEXT,
                    start_date TEXT,
                    end_date TEXT
                );

                CREATE TABLE IF NOT EXISTS 
                house_types (
                    id INTEGER,
                    name TEXT,
                    short_name TEXT,
                    desc TEXT,
                    is_active INTEGER,
                    update_date TEXT,
                    start_date TEXT,
                    end_date TEXT
                );

                CREATE TABLE IF NOT EXISTS 
                apartment_types (
                    id INTEGER,
                    name TEXT,
                    short_name TEXT,
                    desc TEXT,
                    is_active INTEGER,
                    update_date TEXT,
                    start_date TEXT,
                    end_date TEXT
                );

                CREATE TABLE IF NOT EXISTS 
                normative_docs_kind (
                    id INTEGER,
                    name TEXT
                );

                CREATE TABLE IF NOT EXISTS 
                normative_docs_types (
                    id INTEGER,
                    name TEXT,
                    start_date TEXT,
                    end_date TEXT
                );

                CREATE TABLE IF NOT EXISTS 
                object_levels (
                    level INTEGER,
                    name TEXT,
                    start_date TEXT,
                    end_date TEXT,
                    update_date TEXT,
                    is_active TEXT
                );
                

                CREATE TABLE IF NOT EXISTS 
                operation_types (
                    id INTEGER,
                    name TEXT,
                    is_active INTEGER,
                    update_date TEXT,
                    start_date TEXT,
                    end_date TEXT
                );

                
                CREATE TABLE IF NOT EXISTS 
                param_types (
                    id INTEGER,
                    name TEXT,
                    code TEXT,
                    desc TEXT,
                    is_active INTEGER,
                    update_date TEXT,
                    start_date TEXT,
                    end_date TEXT
                );

                CREATE TABLE IF NOT EXISTS 
                room_types (
                    id INTEGER,
                    name TEXT,
                    desc TEXT,
                    is_active INTEGER,
                    update_date TEXT,
                    start_date TEXT,
                    end_date TEXT
                );

                -- таблицы с данными -----------------------------

                CREATE TABLE IF NOT EXISTS 
                normative_docs (
                    id INTEGER,
                    name TEXT,
                    date TEXT,
                    number INTEGER,
                    type INTEGER,
                    kind INTEGER,
                    update_date TEXT,
                    org_name TEXT,
                    reg_num TEXT,
                    reg_date TEXT,
                    acc_date TEXT,
                    comment TEXT
                );

                CREATE TABLE IF NOT EXISTS 
                room_params (
                    id INTEGER,
                    object_id INTEGER,
                    change_id INTEGER,
                    change_end_id INTEGER,
                    type_id INTEGER,
                    value TEXT,
                    update_date TEXT,
                    start_date TEXT,
                    end_date TEXT
                );

                CREATE TABLE IF NOT EXISTS 
                apartment_params (
                    id INTEGER,
                    object_id INTEGER,
                    change_id INTEGER,
                    change_end_id INTEGER,
                    type_id INTEGER,
                    value TEXT,
                    update_date TEXT,
                    start_date TEXT,
                    end_date TEXT
                );

                CREATE TABLE IF NOT EXISTS 
                house_params (
                    id INTEGER,
                    object_id INTEGER,
                    change_id INTEGER,
                    change_end_id INTEGER,
                    type_id INTEGER,
                    value TEXT,
                    update_date TEXT,
                    start_date TEXT,
                    end_date TEXT
                );

                CREATE TABLE IF NOT EXISTS 
                stead_params (
                    id INTEGER,
                    object_id INTEGER,
                    change_id INTEGER,
                    change_end_id INTEGER,
                    type_id INTEGER,
                    value TEXT,
                    update_date TEXT,
                    start_date TEXT,
                    end_date TEXT
                );

                CREATE TABLE IF NOT EXISTS 
                addr_obj_params (
                    id INTEGER,
                    object_id INTEGER,
                    change_id INTEGER,
                    change_end_id INTEGER,
                    type_id INTEGER,
                    value TEXT,
                    update_date TEXT,
                    start_date TEXT,
                    end_date TEXT
                );

                CREATE TABLE IF NOT EXISTS 
                carplaces_params (
                    id INTEGER,
                    object_id INTEGER,
                    change_id INTEGER,
                    change_end_id INTEGER,
                    type_id INTEGER,
                    value TEXT,
                    update_date TEXT,
                    start_date TEXT,
                    end_date TEXT
                );

                CREATE TABLE IF NOT EXISTS 
                addr_obj_division (
                    id INTEGER,
                    parent_id INTEGER,
                    child_id INTEGER,
                    change_id INTEGER
                );

                CREATE TABLE IF NOT EXISTS 
                change_history (
                    change_id INTEGER,
                    object_id INTEGER,
                    add_object_id TEXT,
                    oper_type_id INTEGER,
                    n_doc_id INTEGER,
                    change_date TEXT
                );

                CREATE TABLE IF NOT EXISTS 
                mun_hierarchy (
                    id INTEGER,
                    object_id INTEGER,
                    parent_obj_id INTEGER,
                    change_id INTEGER,
                    oktmo TEXT,
                    prev_id INTEGER,
                    next_id INTEGER,
                    update_date TEXT,
                    start_date TEXT,
                    end_date TEXT,
                    is_active INTEGER,
                    path TEXT
                );

                
                CREATE TABLE IF NOT EXISTS 
                adm_hierarchy (
                    id INTEGER,
                    object_id INTEGER,
                    parent_obj_id INTEGER,
                    change_id INTEGER,
                    region_code TEXT,
                    area_code TEXT,
                    city_code TEXT,
                    place_code TEXT,
                    plan_code TEXT,
                    street_code TEXT,
                    prev_id INTEGER,
                    next_id INTEGER,
                    update_date TEXT,
                    start_date TEXT,
                    end_date TEXT,
                    is_active INTEGER,
                    path TEXT
                );

                CREATE TABLE IF NOT EXISTS 
                room (
                    id INTEGER,
                    object_id INTEGER,
                    object_guid INTEGER,
                    change_id INTEGER,
                    number TEXT,
                    room_type INTEGER,
                    oper_type_id INTEGER,
                    prev_id INTEGER,
                    next_id INTEGER,
                    update_date TEXT,
                    start_date TEXT,
                    end_date TEXT,
                    is_actual INTEGER,
                    is_active INTEGER
                );

                CREATE TABLE IF NOT EXISTS 
                apartment (
                    id INTEGER,
                    object_id INTEGER,
                    object_guid INTEGER,
                    change_id INTEGER,
                    number TEXT,
                    apart_type INTEGER,
                    oper_type_id INTEGER,
                    prev_id INTEGER,
                    next_id INTEGER,
                    update_date TEXT,
                    start_date TEXT,
                    end_date TEXT,
                    is_actual INTEGER,
                    is_active INTEGER
                );

                CREATE TABLE IF NOT EXISTS 
                stead (
                    id INTEGER,
                    object_id INTEGER,
                    object_guid INTEGER,
                    change_id INTEGER,
                    number TEXT,
                    oper_type_id INTEGER,
                    prev_id INTEGER,
                    next_id INTEGER,
                    update_date TEXT,
                    start_date TEXT,
                    end_date TEXT,
                    is_actual INTEGER,
                    is_active INTEGER
                );

                CREATE TABLE IF NOT EXISTS 
                addr_obj (
                    id INTEGER,
                    object_id INTEGER,
                    object_guid INTEGER,
                    change_id INTEGER,
                    name TEXT,
                    type_name TEXT,
                    level TEXT,
                    oper_type_id INTEGER,
                    prev_id INTEGER,
                    next_id INTEGER,
                    update_date TEXT,
                    start_date TEXT,
                    end_date TEXT,
                    is_actual INTEGER,
                    is_active INTEGER
                );

                CREATE TABLE IF NOT EXISTS 
                reestr_object (
                    object_id INTEGER,
                    create_date TEXT,
                    change_id INTEGER,
                    level_id INTEGER,
                    update_date TEXT,
                    object_guid TEXT,
                    is_active INTEGER
                );

                CREATE TABLE IF NOT EXISTS 
                house (
                    id INTEGER,
                    object_id INTEGER,
                    object_guid INTEGER,
                    change_id INTEGER,
                    house_num TEXT,
                    add_num1 TEXT,
                    add_num2 TEXT,
                    house_type INTEGER,
                    add_type1 INTEGER,
                    add_type2 INTEGER,
                    oper_type_id INTEGER,
                    prev_id INTEGER,
                    next_id INTEGER,
                    update_date TEXT,
                    start_date TEXT,
                    end_date TEXT,
                    is_actual INTEGER,
                    is_active INTEGER
                );

            """;
            await connection.ExecuteAsync(sql);
        }
    }

}
