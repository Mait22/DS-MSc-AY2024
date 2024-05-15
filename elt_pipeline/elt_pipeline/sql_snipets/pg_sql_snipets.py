def create_pg_xml_schema(schema_name: str):
    """
    
    :param schema_name: schema name
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
            create schema invoice_xmls_sc;
            set search_path to "$user", public, {schema_name};
          end if;
          
        end $$;
    """


def create_updated_at_trigger(schema_name: str):
    """
    
    :param schema_name: schema name to place the trigger
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


def create_pg_account_dim_table(schema_name: str, table_name: str):
    """
    
    :param schema_name: schema name in which to create a table
    :parm table_name: account table name
    :return: SQL-string
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
                "MainAccountId"     varchar not null primary key,
                "AccountName"       varchar not null,
            
                created_at     timestamp default now(),
                updated_at     timestamp default now()
            );
            
            create trigger "update_trigger_on_{table_name}"
                before update
                on {schema_name}."{table_name}"
                for each row
                execute procedure {schema_name}."table_updated_at"();

          end if;
          
        end $$;
    """


def create_pg_dim_table(schema_name: str, table_name: str, dim_name: str):
    """

    :param schema_name: schema name to create a table
    :param table_name:  name of the table to create
    :param table_name:  dimension the table represents (to create column names)
    :return: SQL string
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
                "{dim_name}Id"          bigint primary key,
                "{dim_name}Value"       varchar not null,
                "{dim_name}Name"        varchar,
            
                created_at     timestamp default now(),
                updated_at     timestamp default now()
            );
            
            create trigger "update_trigger_on_{table_name}"
                before update
                on {schema_name}."{table_name}"
                for each row
                execute procedure {schema_name}."table_updated_at"();

          end if;
          
        end $$;
    """

def create_pg_xml_fact_table(schema_name: str, table_name: str):
    """

    :param schema_name: schema name to create a table
    :param table_name:  name of the table to create
    :return: SQL string
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
                "SurrogateKey"                          varchar primary key,
                "SellerName"                            varchar,
                "RegNumber"                             varchar,
                "InvoiceNumber"                         varchar,
                "InvoiceDate"                           varchar,
                "Amount"                                varchar,
                "ItemUnit"                              varchar,
                "Description"                           varchar,
                "AddRate"                               varchar,
                "ItemPrice"                             varchar,
                "ItemTotal"                             varchar,
                "SellerProductId"                       varchar,
                "EAN"                                   varchar,
                "Filename"                              varchar,
                "ToKeep"                                boolean,
            
                created_at     timestamp default now(),
                updated_at     timestamp default now()
            );
            
            create trigger "update_trigger_on_{table_name}"
                before update
                on {schema_name}."{table_name}"
                for each row
                execute procedure {schema_name}."table_updated_at"();
                
          end if;
          
        end $$;
    """


def create_pg_xml_mn_table(schema_name: str,
                           first_table: str, 
                           second_table: str, 
                           first_table_pk: str, 
                           second_table_pk: str,
                           first_table_pk_type: str,
                           second_table_pk_type: str
                           ):
    """
    
    :param schema_name: schema name to insert the n-m table
    :param first_table: table n
    :param second_table: table m
    :param first_table_pk: primary key from table n
    :param second_table_pk: primary key from table m
    :param first_table_pk_type: primary key type from table n
    :param second_table_pk_type:  primary key type from table m
    :return: SQL string
    """
    
    return f"""
        do $$
        
        declare
          create_table bool;
          
        begin
           select exists (
           select from information_schema.tables 
           where  table_schema='{schema_name}'
           and    table_name='{first_table + "_" + second_table}'
           ) into create_table;
        
          if not create_table then
            create table {schema_name}."{first_table + "_x_" + second_table}"
            (   
                "SurrogateKey"                          varchar primary key,
                "FactTable{first_table_pk}"               {first_table_pk_type} references {schema_name}.{first_table} ("{first_table_pk}"),
                "DimTable{second_table_pk}"             {second_table_pk_type} references {schema_name}.{second_table} ("{second_table_pk}"),
            
                created_at     timestamp default now(),
                updated_at     timestamp default now()
            );
            
            create trigger "update_trigger_on_{first_table + "_x_" + second_table}"
                before update
                on {schema_name}."{first_table + "_x_" + second_table}"
                for each row
                execute procedure {schema_name}."table_updated_at"();
                
          end if;
          
        end $$;
    """


