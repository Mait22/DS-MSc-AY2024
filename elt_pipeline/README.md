# elt_pipeline - purchase XML-s

1. A Dagster data asset ´XML_to_results_df´ is created. Based on the custom script, this asset tries to fetch the following values for each of the invoice lines from the purchase XML-s: 
- Seller name 
- Seller registration number
- Invoice number 
- Invoice date
- Purchased item amount 
- Item unit 
- Description of the purchases item
- Price reduction (AddRate) rate
- Per item pierce (ItemPrice)
- Item total (usually item amount times per item price)
- Item’s EAN (European Article Number) number
- Seller’s product ID 
- XML filename. 

2. The subsequent branching data asset ´result_DFs_to_S3´ saves the invoice rows data frames from the previous step to the Minio S3 bucket as JSON files (one JSON file per previously fetched XML). 

3. In another branch, data asset ´set_up_dw_xml_landing´ sets up a gold layer data landing schema, tables, procedures and triggers in previously referenced Postgres 16-alpine Docker container-based temporary DW. If required elements are already present in the DW, the step is skipped at the database level, i.e. the Postgres DW I/O Dagster resource calls a Postgres’ parameterized table, procedure and trigger creation SQL script templates, kind of a poor man’s DBT equivalent, that include either try-catch logic or queries against Postgres’ system tables. 

4. The data asset ´load_fact_table_to_dw´ upserts (insert if id is not present; update if id present) fact table entries from the data asset XML_to_results_df to Postgres temporary DW. The following pattern generates the surrogate key: ´[invoice row order]_[invoice number]_[source XML filename]_[date]´.

5. The data asset ´get_dims_to_load´ uses ´mssql_io_manager´ resource to fetch financial dimensions that have been related to the given e-invoice in the Dynamics D365 ERP MS SQL database. 

6. Finally, the data asset ´load_dim_tables_to_dw´ upserts the values in the dimension tables. Of note, here the column data types in target Postgres tables correspond to the data types in the source database (MS SQL). Asset ´load_dim__fact_rels_to_dw´ upserts values to n-m tables.

