alter_mutual_excl = ("addcol and dropcol are mutually exclusive")

alter_table_string = ("On alter(), table name must be a string")

cfg_missing_parm_S = ("%s required on call to DBI()")

compkey_dup_mysql_msg = ("1062: Duplicate entry")

compkey_dup_sqlite_msg = ("columns prefix, suffix are not unique")

cov_no_data = ("Coverage.py warning: No data was collected.\r\n")

create_table_string = ("On create(), table name must be a string")

db_closed = ("Cannot operate on a closed database")

db_closed_already_rgx = ("(closing a closed connection|" +
                         "Connection is not active)")

db2_unsupported_S = ("%s not supported for DB2")

dbtype_required = ("A dbtype is required")

default_int_float = ("config.get_time: default must be int or float")

default_piddir = ("/tmp/crawler")

drop_table_empty = ("On drop(), table name must not be empty")

drop_table_string = ("On drop(), table name must be a string")

invalid_attr_rgx = ("Attribute '.*' is not valid for .*")

invalid_attr_SS = ("Attribute '%s' is not valid for %s")

invalid_time_unit_S = ("invalid time unit '%s'")

invalid_time_mag_S = ("invalid time magnitude '%s'")

missing_arg_S = ("A %s or cfg object and section name is required")

missing_db_section = ("No database section present")

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

section_required = ("A section name is required")

table_already_mysql = ("1050: Table 'test_create_already' already exists")

table_already_sqlite = ("table test_create_already already exists")

tbl_prefix_required = ("Table prefix string (tbl_prefix) is required")

too_many_val = ("too many values to unpack")

unknown_dbtype_S = ("Unrecognized database type: %s")

unrecognized_arg_S = ("Unrecognized argument to %s. " +
                      "Only 'cfg=<config>' is accepted")

valid_dbtype = ("dbtype must be 'sqlite', 'mysql', or 'db2'")

wildcard_selects = ("Wildcard selects are not supported. " +
                    "Please supply a list of fields.")
