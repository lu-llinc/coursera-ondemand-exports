# Basic stuff
# Written by: Jasper Ginn

rm(list=ls())
# Install postgresql R connector
install.packages("RPostgreSQL")
# Load
library(RPostgreSQL)
# Crendentials
creds <- list("host" = "127.0.0.1",
              "dbname" = "terrorism_ondemand",
              "user" = "jasper",
              "pwd" = "root")
# Connect
drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, dbname=creds$dbname,
                 host = creds$host,
                 user = creds$user,
                 password = creds$pwd)
# List tables
dbListTables(con)
# Select users
stat <- "SELECT * FROM users;"
res <- dbGetQuery(con, stat)
