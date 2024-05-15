def create_pg_schema(schema_name: str):
    """
    
    :param schema_name: schema to create
    :return: SQL-string
    """

    return f"""
        do $$
        
        declare
          create_schema bool;
          
        begin
            select count(schema_name) = 0
            from information_schema.schemata
            where "schema_name" = '{schema_name}'
            into create_schema;
        
          if create_schema then
            create schema {schema_name};
            set search_path to "$user", public, {schema_name};
          end if;
          
        end $$;
    """


def create_updated_at_trigger(schema_name: str):
    """
    
    :param schema_name: schema to add the trigger procedure
    :return: SQL-string
    """

    return f"""
        do $$
        begin
            begin
                perform pg_get_functiondef('table_updated_at()'::regprocedure);
            exception when undefined_function then
        
                create function {schema_name}.table_updated_at() returns trigger as $table_updated_at$
                    begin
                        NEW."updated_at" = NOW();
                        return NEW;
                    end;
                $table_updated_at$ language plpgsql;
            end;
        end $$;
    """


def create_log_bronze_table(schema_name: str, table_name: str):
    """
    
    """
    return f"""
        do $$
        
        declare
          create_table bool;
          
        begin
           select exists (
           select from information_schema.tables 
           where  table_schema='{schema_name}'
           and    table_name='{table_name}'
           ) into create_table;
        
          if not create_table then
            create table {schema_name}."{table_name}"
            (
                "MainLogEventId"            bigint primary key,
                "LcName"                    varchar not null,
                "MainLogEventStartTime"     timestamp,
                "MainLogEventEndTime"       timestamp,
                "MainLogEventName"          varchar,
                "StartErrorTypeName"        varchar,
                "EndErrorTypeName"          varchar,
                "created_at"     timestamp default now(),
                "updated_at"     timestamp default now()
            );
            
            create trigger "update_trigger_on_{table_name}"
                before update
                on {schema_name}."{table_name}"
                for each row
                execute procedure {schema_name}."table_updated_at"();

          end if;
          
        end $$;
    """

def create_matched_events_table(schema_name: str, table_name: str):
    return f"""
        do $$
        
        declare
          create_table bool;
          
        begin
           select exists (
           select from information_schema.tables 
           where  table_schema='{schema_name}'
           and    table_name='{table_name}'
           ) into create_table;
        
          if not create_table then
            create table {schema_name}."{table_name}"
            (   "LcName"                    varchar not null,
                "MainLogEventStartTime"     timestamp,
                "MainLogEventId"            bigint primary key,
                "MatchedEventId"            varchar,
                "MatchedEventDuration"      real,
                "MatchedEventSignature"     varchar,
                "created_at"     timestamp default now(),
                "updated_at"     timestamp default now()
            );
            
            create trigger "update_trigger_on_{table_name}"
                before update
                on {schema_name}."{table_name}"
                for each row
                execute procedure {schema_name}."table_updated_at"();

          end if;
          
        end $$;
    """

def create_alert_criteria_table(schema_name: str, table_name: str):
    return f"""
        do $$
        
        declare
          create_table bool;
          
        begin
           select exists (
           select from information_schema.tables 
           where  table_schema='{schema_name}'
           and    table_name='{table_name}'
           ) into create_table;
        
          if not create_table then
            create table {schema_name}."{table_name}"
            (   "lc" VARCHAR ( 255 ) UNIQUE NOT NULL,
                "lower_crit" FLOAT8 NOT NULL DEFAULT 10,
                "higher_crit" FLOAT8 NOT NULL DEFAULT 10,
                "created_on"     timestamp default now(),
                "updated_at"     timestamp default now()
            );
            
            create trigger "update_trigger_on_{table_name}"
                before update
                on {schema_name}."{table_name}"
                for each row
                execute procedure {schema_name}."table_updated_at"();

          end if;
          
        end $$;
    """

def query_raw_data(continue_from_id: int):
    return f"""
    select
       -- main lc event data
       lc.name as "LcName",
       ei.id as "MainLogEventId",
       ei.start_time as "MainLogEventStartTime",
       ei.end_time as "MainLogEventEndTime",
       et.id as "MainLogEventTypeId",
       et.name as "MainLogEventName",

       -- main event starts with fault
       ei.start_time_fault as "StartErrorTriggerId",
       et_v_alg.id as "StartErrorTypeId",
       et_v_alg.name as "StartErrorTypeName",

       -- main event ends with fault
       ei.end_time_fault as "EndErrorTriggerId",
       et_v_lp.id as "EndErrorTypeId",
       et_v_lp.name as "EndErrorTypeName"

    from
        (select * from event_instance
        where extract(year from "start_time") >= 2023
        and id > {continue_from_id}
        and start_time IS NOT NULL
        and end_time IS NOT NULL) as ei
    --- event type
    left join event_trigger as etrig on
        etrig.id = ei.event
    left join event_type as et on
        etrig.event_type = et.id
    left join levercrossing as lc on
        lc.id = etrig.levercrossing
    --- start error
    left join event_trigger as etrig_v_alg on
        etrig_v_alg.id = ei.start_time_fault
    left join event_type as et_v_alg on
        etrig_v_alg.event_type = et_v_alg.id
    --- end error
    left join event_trigger as etrig_v_lp on
        etrig_v_lp.id = ei.end_time_fault
    left join event_type as et_v_lp on
        etrig_v_lp.event_type = et_v_lp.id;
    """

def query_data_from_bronze(lc_name: str, max_id: int):

    return f"""select * from level_crossing_logs.source_log_data
               where "MainLogEventId" > {max_id}
               and "LcName" = '{lc_name}'
               order by "MainLogEventId" ASC;"""


