# log_elt_pipeline - LC logs

1. The data assets ´set_up_dw_landing´ sets up landing schema, tables, procedures and triggers in the previously referenced Postgres 16-alpine Docker container-based temporary DW.

2. Dagster data asset ´load_raw_data´ queries data from the source system.

3. Dagster data asset ´get_data_to_analyze´ loads data for specified level crossings (intra asset parameter) from DW’s table ´source_log_data´ into Python dictionary: keys by level crossing name; values Pandas data frames of source log data.

4. The data asset match_events runs the train passing event matching algorithm per level crossing.

5. The data asset ´load_matched_events_to_dw´ upserts matched data to  DW’s ´matched_log_data´ table. 

