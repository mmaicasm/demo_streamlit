# %%
# Librerias necesarias
from snowflake.snowpark.session import Session

# Datos secretos de conexión
from secrets import login_info

# Parámetros de conexión
connection_parameters = {
  "account": login_info["snowflake_account"],
  "user": login_info["snowflake_user"],
  "password": login_info["snowflake_password"],
  "role": "SYSADMIN",
  "database": "TEST_SNOWPARK",
  "schema": "CITIBIKE",
  "warehouse": "COMPUTE_WH"
}

# %%
def connect():
  session = Session.builder.configs(connection_parameters).create()
  return session
# %%
