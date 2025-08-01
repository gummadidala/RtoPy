
# Database Functions
suppressMessages({
    library(odbc)
    library(RMariaDB)
    library(yaml)
})

cred <- read_yaml("Monthly_Report_AWS.yaml")

# My own function to perform multiple inserts at once.
# Hadn't found a way to do this through native functions.
mydbAppendTable <- function(conn, name, value, chunksize = 1e4) {

    df <- value %>%
        mutate(
            across(where(is.Date), ~format(., "%F")),
            across(where(is.POSIXct), ~format(., "%F %H:%M:%S")),
            across(where(is.factor), as.character),
            across(where(is.character), ~replace(., is.na(.), "")),
            across(where(is.character), ~str_replace_all(., "'", "\\\\'")),
            across(where(is.character), ~paste0("'", ., "'")),
            across(where(is.numeric), ~replace(., !is.finite(.), NA)))

    table_name <- name

    vals <- unite(df, "z", names(df), sep = ",") %>% pull(z)
    vals <- glue("({vals})") %>% str_replace_all("NA", "NULL")
    vals_list <- split(vals, ceiling(seq_along(vals)/chunksize))

    query0 <- glue("INSERT INTO {table_name} (`{paste0(colnames(df), collapse = '`, `')}`) VALUES ")

    for (v in vals_list) {
        query <- paste0(query0, paste0(v, collapse = ','))
        dbExecute(conn, query)
    }
}


# -- Previously from Monthly_Report_Functions.R

get_atspm_connection <- function(conf_atspm) {

    if (Sys.info()["sysname"] == "Windows") {

        dbConnect(odbc::odbc(),
                  dsn = conf_atspm$odbc_dsn,
                  uid = Sys.getenv(conf_atspm$uid_env),
                  pwd = Sys.getenv(conf_atspm$pwd_env))

    } else if (Sys.info()["sysname"] == "Linux") {

        dbConnect(odbc::odbc(),
                  driver = "FreeTDS",
                  dsn = conf_atspm$odbc_dsn,
                  uid = Sys.getenv(conf_atspm$uid_env),
                  pwd = Sys.getenv(conf_atspm$pwd_env))
    }
}


get_maxview_connection <- function(dsn = "maxview") {

    if (Sys.info()["sysname"] == "Windows") {

        dbConnect(odbc::odbc(),
                  dsn = dsn,
                  uid = Sys.getenv("MAXV_USERNAME"),
                  pwd = Sys.getenv("MAXV_PASSWORD"))

    } else if (Sys.info()["sysname"] == "Linux") {

        dbConnect(odbc::odbc(),
                  driver = "FreeTDS",
                  dsn = dsn,
                  uid = Sys.getenv("MAXV_USERNAME"),
                  pwd = Sys.getenv("MAXV_PASSWORD"))
    }
}


get_maxview_eventlog_connection <- function() {
    get_maxview_connection(dsn = "MaxView_EventLog")
}


get_cel_connection <- get_maxview_eventlog_connection


get_aurora_connection <- function(
    f = RMariaDB::dbConnect,
    driver = RMariaDB::MariaDB(),
    load_data_local_infile = FALSE
) {

    f(drv = driver,
      host = cred$RDS_HOST,
      port = 3306,
      dbname = cred$RDS_DATABASE,
      username = cred$RDS_USERNAME,
      password = cred$RDS_PASSWORD,
      load_data_local_infile = load_data_local_infile)
}

get_aurora_connection_pool <- function() {
    get_aurora_connection(f = pool::dbPool)
}


RAthena_options(file_parser="vroom", clear_s3_resource=FALSE)
reticulate::use_condaenv("sigops")

get_athena_connection <- function(conf_athena, f = dbConnect) {
    f(RAthena::athena(), schema = conf_athena$database, s3_staging_dir=conf_athena$staging_dir)
}


get_athena_connection_pool <- function(conf_athena) {
    get_athena_connection(conf_athena, pool::dbPool)
}


add_athena_partition <- function(conf_athena, bucket, table_name, date_) {
    tryCatch({
        conn_ <- get_athena_connection(conf_athena)
        dbExecute(conn_,
                  sql(glue(paste("ALTER TABLE {conf_athena$database}.{table_name}",
                                 "ADD PARTITION (date='{date_}')"))))
        msg <- glue("Successfully created partition (date='{date_}') for {conf_athena$database}.{table_name}")
        print(msg)
    }, error = function(e) {
        error_message <- stringr::str_extract(as.character(e), "message:.*?\\.")
        error_message <- ifelse(is.na(error_message), as.character(e), error_message)
        print(error_message)
        msg <- glue("{error_message} - Partition (date='{date_}') for {conf_athena$database}.{table_name}")
    }, finally = {
        dbDisconnect(conn_)
    })
}



query_data <- function(
    metric,
    level = "corridor",
    resolution = "monthly",
    hourly = FALSE,
    zone_group,
    corridor = NULL,
    month = NULL,
    quarter = NULL,
    upto = TRUE) {

    # metric is one of {vpd, tti, aog, ...}
    # level is one of {corridor, subcorridor, signal}
    # resolution is one of {quarterly, monthly, weekly, daily}

    per <- switch(
        resolution,
        "quarterly" = "qu",
        "monthly" = "mo",
        "weekly" = "wk",
        "daily" = "dy")

    mr_ <- switch(
        level,
        "corridor" = "cor",
        "subcorridor" = "sub",
        "signal" = "sig")

    tab <- if (hourly & !is.null(metric$hourly_table)) {
        metric$hourly_table
    } else {
        metric$table
    }

    table <- glue("{mr_}_{per}_{tab}")

    # Special cases--groups of corridors
    if (level == "corridor" & (grepl("RTOP", zone_group))) {
        if (zone_group == "All RTOP") {
            zones <- c("All RTOP", "RTOP1", "RTOP2", RTOP1_ZONES, RTOP2_ZONES)
        } else if (zone_group == "RTOP1") {
            zones <- c("All RTOP", "RTOP1", RTOP1_ZONES)
        } else if (zone_group == "RTOP2") {
            zones <- c("All RTOP", "RTOP2", RTOP2_ZONES)
        }
        zones <- paste(glue("'{zones}'"), collapse = ",")
        where_clause <- glue("WHERE Zone_Group in ({zones})")
        where_clause <- paste(where_clause, "AND Corridor NOT LIKE 'Zone%'")
    } else if (zone_group == "Zone 7" ) {
        zones <- c("Zone 7", "Zone 7m", "Zone 7d")
        zones <- paste(glue("'{zones}'"), collapse = ",")
        where_clause <- glue("WHERE Zone_Group in ({zones})")
    } else if (level == "signal" & zone_group == "All") {
        # Special case used by the map which currently shows signal-level data
        # for all signals all the time.
        where_clause <- "WHERE True"
    } else {
        where_clause <- "WHERE Zone_Group = '{zone_group}'"
    }

    query <- glue(paste(
        "SELECT * FROM {table}",
        where_clause))

    comparison <- ifelse(upto, "<=", "=")

    if (typeof(month) == "character") {
        month <- as_date(month)
    }

    if (hourly & !is.null(metric$hourly_table)) {
        if (resolution == "monthly") {
            query <- paste(query, glue("AND Hour <= '{month + months(1) - hours(1)}'"))
            if (!upto) {
                query <- paste(query, glue("AND Hour >= '{month}'"))
            }
        }
    } else if (resolution == "monthly") {
        query <- paste(query, glue("AND Month {comparison} '{month}'"))
    } else if (resolution == "quarterly") { # current_quarter is not null
        query <- paste(query, glue("AND Quarter {comparison} {quarter}"))

    } else if (resolution == "weekly" | resolution == "daily") {
        query <- paste(query, glue("AND Date {comparison} '{month + months(1) - days(1)}'"))

    } else {
        "oops"
    }

    df <- data.frame()

    tryCatch({
        df <- dbGetQuery(sigops_connection_pool, query)

        if (!is.null(corridor)) {
            df <- filter(df, Corridor == corridor)
        }

        date_string <- intersect(c("Month", "Date"), names(df))
        if (length(date_string)) {
            df[[date_string]] = as_date(df[[date_string]])
        }
        datetime_string <- intersect(c("Hour"), names(df))
        if (length(datetime_string)) {
            df[[datetime_string]] = as_datetime(df[[datetime_string]])
        }
    }, error = function(e) {
        print(e)
    })

    df
}



# udc_trend_table
query_udc_trend <- function() {
    df <- dbReadTable(sigops_connection_pool, "cor_mo_udc_trend_table")
    udc_list <- jsonlite::fromJSON(df$data)
    lapply(udc_list, function(x) {
        data <- as.data.frame(x) %>% mutate(Month = as_date(Month))
        colnames(data) <- str_replace_all(colnames(data), "[:punct:]", " ")
        data
    })
}



# udc hourly table
query_udc_hourly <- function(zone_group, month) {
    df <- dbReadTable(sigops_connection_pool, "cor_mo_hourly_udc")
    df$Month <- as_date(df$Month)
    df$month_hour <- as_datetime(df$month_hour)
    subset(df, Zone == zone_group & Month <= as_date(month))
}

# TODO: mark/user_delay_costs not calculating since Nov 2020.
# Figure out where script is supposed to run (SAM?) and get it scheduled again.
# Run on Lenny for back dates in the meantime.



query_health_data <- function(
    health_metric,
    level,
    zone_group,
    corridor = NULL,
    month = NULL) {

    # metric is one of {ops, maint, safety}
    # level is one of {corridor, subcorridor, signal}
    # resolution is one of {quarterly, monthly, weekly, daily}

    per <- "mo"

    mr_ <- switch(
        level,
        "corridor" = "sub",
        "subcorridor" = "sub",
        "signal" = "sig")

    tab <- health_metric

    table <- glue("{mr_}_{per}_{tab}")

    # Special cases--groups of corridors
    if ((level == "corridor" | level == "subcorridor") & (grepl("RTOP", zone_group)) | zone_group == "Zone 7" ) {
        if (zone_group == "All RTOP") {
            zones <- c("All RTOP", "RTOP1", "RTOP2", RTOP1_ZONES, RTOP2_ZONES)
        } else if (zone_group == "RTOP1") {
            zones <- c("All RTOP", "RTOP1", RTOP1_ZONES)
        } else if (zone_group == "RTOP2") {
            zones <- c("All RTOP", "RTOP2", RTOP2_ZONES)
        } else if (zone_group == "Zone 7") {
            zones <- c("Zone 7m", "Zone 7d")
        }
        zones <- paste(glue("'{zones}'"), collapse = ",")
        where_clause <- glue("WHERE Zone_Group in ({zones})")
    } else if ((level == "corridor" | level == "subcorridor") & corridor == "All Corridors") {
        where_clause <- glue("WHERE Zone_Group = '{zone_group}'")  # Zone_Group is a proxy for Zone
    } else {  #} if (level == "corridor" | level == "subcorridor") {
        where_clause <- glue("WHERE Corridor = '{corridor}'")
    }
    where_clause <- glue("{where_clause} AND Month = '{month}'")

    query <- glue(paste(
        "SELECT * FROM {table}",
        where_clause))

    tryCatch({
        df <- dbGetQuery(sigops_connection_pool, query)
        df$Month = as_date(df$Month)
        #df <- subset(df, select = -Zone_Group)  # This was a proxy for Zone for indexing consistency
    }, error = function(e) {
        print(e)
    })

    df
}



create_aurora_partitioned_table <- function(aurora, table_name, period_field = "Timeperiod") {
    months <- seq(Sys.Date() - months(10), Sys.Date() + months(1), by = "1 month")
    new_partition_dates <- format(months, "%Y-%m-01")
    new_partition_names <- format(months, "p_%Y%m")
    partitions <- glue("PARTITION {new_partition_names} VALUES LESS THAN ('{new_partition_dates} 00:00:00'),")

    table_suffix <- stringr::str_extract(table_name, "[^_]*$")
    var <- case_when(
        table_suffix == "aogh" ~ "aog",
        table_suffix == "vph" ~ ifelse(period_field == "Timeperiod", "vol", "vph"),
        table_suffix == "paph" ~ ifelse(period_field == "Timeperiod", "vol", "paph"),
        table_suffix == "prh" ~ "pr",
        table_suffix == "qsh" ~ "qs_freq",
        table_suffix == "sfh" ~ "sf_freq")

    stmt <- glue(
        "CREATE TABLE `{table_name}_part` (
          `Zone_Group` varchar(128) DEFAULT NULL,
          `Corridor` varchar(128) DEFAULT NULL,
          `{period_field}` datetime NOT NULL,
          `{var}` double DEFAULT NULL,
          `ones` double DEFAULT NULL,
          `delta` double DEFAULT NULL,
          `Description` varchar(128) DEFAULT NULL,
          `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
          PRIMARY KEY (`id`, `{period_field}`),
          UNIQUE KEY `idx_{table_name}_unique` (`{period_field}`, `Zone_Group`, `Corridor`),
          KEY `idx_{table_name}_zone_period` (`Zone_Group`, `{period_field}`),
          KEY `idx_{table_name}_corridor_period` (`Corridor`, `{period_field}`)
        )
        PARTITION BY RANGE COLUMNS (`{period_field}`) (
            {paste(partitions, collapse=' ')}
            PARTITION future VALUES LESS THAN (MAXVALUE)
        )"
    )
    dbExecute(aurora, stmt)
}


get_aurora_partitions <- function(aurora, table_name) {
    query <- glue(paste(
        "SELECT PARTITION_NAME FROM information_schema.partitions",
        "WHERE TABLE_NAME = '{table_name}'"))
    df <- dbGetQuery(aurora, query)
    if (nrow(df) > 1) {
        df$PARTITION_NAME
    } else {
        NULL
    }
}


add_aurora_partition <- function(aurora, table_name) {
    mo <- Sys.Date() + months(1)
    new_partition_date <- format(mo, "%Y-%m-01")
    new_partition_name <- format(mo, "p_%Y%m")
    existing_partitions <- get_aurora_partitions(aurora, table_name)
    if (!new_partition_name %in% existing_partitions & !is.null(existing_partitions)) {
        statement <- glue(paste(
            "ALTER TABLE {table_name}",
            "REORGANIZE PARTITION future INTO (",
                "PARTITION {new_partition_name} VALUES LESS THAN ('{new_partition_date} 00:00:00'),",
                "PARTITION future VALUES LESS THAN MAXVALUE)"))
        print(statement)
        dbExecute(aurora, statement)
    }
}

drop_aurora_partitions <- function(aurora, table_name, months_to_keep = 8) {
    existing_partitions <- get_aurora_partitions(aurora, table_name)
    existing_partitions <- existing_partitions[existing_partitions != "future"]
    if (!is.null(existing_partitions)) {
        drop_partition_name <- format(Sys.Date() - months(months_to_keep), "p_%Y%m")
        drop_partition_names <- existing_partitions[existing_partitions <= drop_partition_name]

        for (partition_name in drop_partition_names) {
            statement <- glue("ALTER TABLE {table_name} DROP PARTITION {partition_name};")
            print(statement)
            dbExecute(aurora, statement)
        }
    }
}
