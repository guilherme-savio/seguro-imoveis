# Create an ADLS Gen2 and containers for landing, bronze, silver, and gold zones
resource "azurerm_storage_account" "storage" {
  name                     = "seguroimoveisdatalake"
  resource_group_name      = var.resource_group_name
  location                 = var.resource_group_location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = true
}

resource "azurerm_storage_container" "landing-zone" {
  name                  = "landing-zone"
  storage_account_name  = azurerm_storage_account.storage.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "bronze" {
  name                  = "bronze"
  storage_account_name  = azurerm_storage_account.storage.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "silver" {
  name                  = "silver"
  storage_account_name  = azurerm_storage_account.storage.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "gold" {
  name                  = "gold"
  storage_account_name  = azurerm_storage_account.storage.name
  container_access_type = "private"
}


# Create an Azure SQL Server and database
resource "azurerm_mssql_server" "sql_server" {
  name                         = "seguroimoveis-sql"
  resource_group_name          = var.resource_group_name
  location                     = var.resource_group_location
  version                      = "12.0"
  administrator_login          = "adminuser"
  administrator_login_password = var.sql_server_admin_password
}

resource "azurerm_mssql_database" "database" {
  name                        = "seguroimoveis"
  server_id                   = azurerm_mssql_server.sql_server.id
  collation                   = "SQL_Latin1_General_CP1_CI_AS"
  max_size_gb                 = 64
  read_scale                  = false
  zone_redundant              = false
  auto_pause_delay_in_minutes = -1
  min_capacity                = 10
  read_replica_count          = 0
  sku_name                    = "GP_S_Gen5_10"
  geo_backup_enabled          = false
}

# Create a firewall rule to allow all IP addresses
resource "azurerm_mssql_firewall_rule" "firewall_rule" {
  name             = "allow-all-ip"
  server_id        = azurerm_mssql_server.sql_server.id
  start_ip_address = "0.0.0.0"
  end_ip_address   = "255.255.255.255"
}

