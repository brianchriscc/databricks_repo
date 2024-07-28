# Databricks notebook source
# Define the storage account details
container_name = "people"
storage_account_name = "briandatacont"
mount_name = container_name 
scope_name = "scope"

# Retrieve the service principal credentials from the secret scope
client_id = dbutils.secrets.get(scope=scope_name, key="clientid")
client_secret = dbutils.secrets.get(scope=scope_name, key="st")
tenant_id = dbutils.secrets.get(scope=scope_name, key="tenantid")

# Construct the source URL
source_url = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/"

# Configure the extra configs
configs = {
  "fs.azure.account.auth.type": "OAuth",
  "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id": client_id,
  "fs.azure.account.oauth2.client.secret": client_secret,
  "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
}

# Mount the ADLS Gen2 container to DBFS
try:
    dbutils.fs.mount(
  source = source_url,
  mount_point = f"/mnt/{mount_name}",
  extra_configs = configs
)

except Exception as e:
    pass
