alter_table_string = ("On alter(), table name must be a string")

cfg_missing_parm_S = ("%s required on call to DBI()")

compkey_dup_mysql_msg = ("1062: Duplicate entry")

compkey_dup_sqlite_msg = ("columns prefix, suffix are not unique")

cov_no_data = ("Coverage.py warning: No data was collected.\r\n")

create_table_string = ("On create(), table name must be a string")

db_closed = ("Cannot operate on a closed database")

db_closed_already_rgx = ("(closing a closed connection|" +
                         "Connection is not active)")

dbname_cfg_required = ("A dbname or cfg object and section name is required" +
                       "(dbname=")

dbtype_required = ("A dbtype is required")

default_int_float = ("config.get_time: default must be int or float")

default_piddir = ("/tmp/crawler")

invalid_attr_rgx = ("Attribute '.*' is not valid for .*")

invalid_attr_SS = ("Attribute '%s' is not valid for %s")

invalid_time_unit_S = ("invalid time unit '%s'")

invalid_time_mag_S = ("invalid time magnitude '%s'")

missing_arg_S = ("A %s or cfg object and section name is required")

missing_db_section = ("No database section present")

section_required = ("A section name is required")

table_already_mysql = ("1050: Table 'test_create_already' already exists")

table_already_sqlite = ("table test_create_already already exists")

tbl_prefix_required = ("Table prefix string (tbl_prefix) is required")

too_many_val = ("too many values to unpack")

valid_dbtype = ("dbtype must be 'sqlite', 'mysql', or 'db2'")

unknown_dbtype_S = ("Unrecognized database type: %s")

unrecognized_arg_S = ("Unrecognized argument to %s. " +
                      "Only 'cfg=<config>' is accepted")
