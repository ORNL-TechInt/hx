alter_addcol_not_empty = ("On alter(), addcol must not be empty")

alter_dropcol_not_empty = ("On alter, dropcol must not be empty")

alter_mutual_excl = ("addcol and dropcol are mutually exclusive")

alter_action_req = ("ALTER requires an action")

bad_bindings_mysql = ("not enough arguments for format string")

bad_bindings_sqlite = ("Incorrect number of bindings supplied")

cfg_missing_parm_S = ("%s required on call to DBI()")

compkey_dup_mysql_msg = ("1062: Duplicate entry")

compkey_dup_sqlite_msg = ("columns prefix, suffix are not unique")

cov_no_data = ("Coverage.py warning: No data was collected.\r\n")

crit_incomplete = ("Criteria are not fully specified")

ctxt_invalid = ("is not valid in the context where it")

data_ignored = ("Data would be ignored")

data_list_notmt = ("On insert(), data list must not be empty")

data_notmt = ("On update(), data must not be empty")

data_list_S = ("On %s(), data must be a list of tuples")

db_bad_file = ("file is encrypted or is not a database")

db_closed = ("Cannot operate on a closed database")

db_closed_already_rgx = ("(closing a closed connection|" +
                         "Connection is not active)")

db_open_fail = ("unable to open database file")

db2_unsupported_S = ("%s not supported for DB2")

dbtype_required = ("A dbtype is required")

default_int_float = ("config.get_time: default must be int or float")

default_piddir = ("/tmp/crawler")

data_tuple_S = ("On %s(), data must be a tuple")

disk_io_err = ("disk I/O error")

duplcol_mysql = ("Duplicate column name 'size'")

duplcol_sqlite = ("duplicate column name: size")

fields_list_S = ("On %s(), fields must be a list")

fields_notmt_S = ("On %s(), fields must not be empty")

fields_notmt = ("On insert(), fields list must not be empty")

invalid_addcol = ("Invalid addcol argument")

invalid_dropcol_mysql = ("Invalid dropcol argument")

invalid_attr_rgx = ("Attribute '.*' is not valid for .*")

invalid_attr_SS = ("Attribute '%s' is not valid for %s")

invalid_time_unit_S = ("invalid time unit '%s'")

invalid_time_mag_S = ("invalid time magnitude '%s'")

missing_arg_S = ("A %s or cfg object and section name is required")

missing_db_section = ("No database section present")

no_such_column_S = ("no such column: %s")

no_such_dropcol_S = ("Can't DROP '%s'; check that column/key exists")

no_such_table_del_rgx = ("(\\(1146, \"Table '.*?' doesn't exist\"\\)|" +
                         "delete from .*? where name='.*?': " +
                         "no such table: .*? \\(dbname=.*?\\))")

no_such_table_desc_rgx = ("(Unknown table '.*'|" +
                          "DESCRIBE not supported for DB2)")

no_such_table_drop_rgx = ("(1051: Unknown table '.*' \\(dbname=.*\\)|" +
                          "no such table: .* \\(dbname=.*\\))")

no_such_table_S = ("Unknown table '%s'")

no_such_table_upd_rgx = ("(\\(1146, \"Table '.*?' doesn't exist\"\\)|" +
                         "no such table: .*? \\(dbname=.*?\\))")

param_bound = ("params bound not matching")

param_bound_rgx = ("\d+ %s \d+ required" % param_bound)

param_noquote = ("Parameter placeholders should not be quoted")

section_required = ("A section name is required")

select_gb_str = ("On select(), groupby clause must be a string")

select_l_nint = ("On select(), limit must be an int")

select_nld = ("On select(), data must be a tuple")

select_nso = ("On select(), orderby clause must be a string")

where_str_S = ("On %s(), where clause must be a string")

table_already_mysql = ("1050: Table 'test_create_already' already exists")

table_already_sqlite = ("table test_create_already already exists")

table_nocol_rgx = ("table \S+ has no column named \S+")

tbl_name_str_S = ("On %s(), table name must be a string")

tbl_name_notmt_S = ("On %s(), table name must not be empty")

tbl_prefix_required = ("Table prefix string (tbl_prefix) is required")

too_many_val = ("too many values to unpack")

unknown_dbtype_S = ("Unrecognized database type: %s")

unknown_column_rgx = ("Unknown column '\S+' in '\S.*'")

unknown_column_gb_S = ("Unknown column '%s' in 'group statement'")

unrecognized_arg_S = ("Unrecognized argument to %s. " +
                      "Only 'cfg=<config>' is accepted")

unsupp_dropcol_sqlite = ("SQLite does not support dropping columns")

valid_dbtype = ("dbtype must be 'sqlite', 'mysql', or 'db2'")

wildcard_selects = ("Wildcard selects are not supported. " +
                    "Please supply a list of fields.")
