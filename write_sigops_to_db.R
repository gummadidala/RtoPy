
library(aws.s3)
library(qs)


load_bulk_data <- function(conn, table_name, df_) {

    dbcols <- dbListFields(conn, table_name)
    dfcols <- names(df_)
    cols_ <- intersect(dbcols, dfcols)  # Columns common to both df and db tables
    df_ <- df_[cols_]  # Put columns in the right order

    mydbAppendTable(conn, table_name, df_)
}

load_data <- function(conn_pool, table_name, df_) {
    con <- pool::poolCheckout(conn_pool)
    dbAppendTable(con, table_name, df_)
    pool::poolReturn(con)
}

write_sigops_to_db <- function(
    conn, df, dfname, recreate = FALSE,
        calcs_start_date = NULL,
        report_start_date = NULL,
        report_end_date = NULL) {

    # Aggregation periods: qu, mo, wk, dy, ...
    pers <- names(df)
    pers <- pers[pers != "summary_data"]

    table_names <- c()
    for (per in pers) {
        for (tab in names(df[[per]])) {
            dbBegin(conn)  # Because of database cluster. Need to delete and write within the same transaction to avoid conflicts.

            table_name <- glue("{dfname}_{per}_{tab}")
            table_names <- append(table_names, table_name)
            df_ <- df[[per]][[tab]]
            datefield <- intersect(names(df_), c("Month", "Date", "Hour", "Timeperiod"))

            if (per == "wk") {
                start_date <- round_to_tuesday(calcs_start_date)
            } else {
                start_date <- calcs_start_date
            }

            # Sort to align with index to hopefully speed up database writes
            if (length(intersect(names(df_), c(datefield, "Zone_Group", "Corridor"))) == 3) {
                df_ <- arrange(df_, !!as.name(datefield), Zone_Group, Corridor)
            }

            tryCatch({
                if (recreate) {
                    print(glue("{Sys.time()} Writing {table_name} | 3 | recreate = {recreate}"))
                    # Overwrite to create initial data types
                    DBI::dbWriteTable(
                        conn,
                        table_name,
                        head(df_, 3),
                        overwrite = TRUE,
                        row.names = FALSE)
                    dbExecute(conn, glue("TRUNCATE TABLE {table_name}"))
                } else {
                    if (table_name %in% dbListTables(conn)) {
                        # Drop and add partitions
                        drop_aurora_partitions(conn, table_name)
                        add_aurora_partition(conn, table_name)

                        # Clear head of table prior to report start date
                        if (!is.null(report_start_date) & length(datefield) == 1) {
                            db_first_day <- dbGetQuery(
                                conn, glue("SELECT MIN({datefield}) AS first_day FROM {table_name}")
                            ) %>% pull(first_day)

                            if (db_first_day <= as_date(report_start_date)) {
                                lapply(
                                    seq(as_date(db_first_day), as_date(report_start_date), by="1 day"),
                                    function(first_day) {
                                        dbExecute(conn, glue(
                                            "DELETE from {table_name} WHERE {datefield} < '{first_day}'"))
                                    }
                                )
                            }
                        }
                        # Clear Tail Prior to Append
                        if (!is.null(start_date) & length(datefield) == 1) {
                            end_date <- ifelse(is.null(report_end_date), Sys.Date(), report_end_date)
                            lapply(
                                rev(seq(as_date(start_date), as_date(end_date), by="1 day")),
                                function(last_day) {
                                    dbExecute(conn, glue(
                                        "DELETE from {table_name} WHERE {datefield} >= '{last_day}'"))
                                }
                            )
                        } else if (is.null(start_date) | "Quarter" %in% names(df_)) {
                            dbExecute(conn, glue("TRUNCATE TABLE {table_name}"))
                        } else {
                            print(glue("More than one datefield ({paste(datefield, collapse = ', ')}) in data frame {table_name}."))
                        }
                        # Filter Dates and Append
                        if (!is.null(start_date) & length(datefield) == 1) {
                            df_ <- filter(df_, !!as.name(datefield) >= start_date)
                        }
                        if (!is.null(report_end_date) & length(datefield) == 1) {
                            df_ <- filter(df_, !!as.name(datefield) < ymd(report_end_date) + months(1))
                        }

                        print(glue("{Sys.time()} Writing {table_name} | {scales::comma_format()(nrow(df_))} | recreate = {recreate}"))
                        load_bulk_data(conn, table_name, df_)
                    }
                }

            }, error = function(e) {
                print(glue("{Sys.time()} {table_name} {e}"))
            })
            dbCommit(conn)
        }
    }
    invisible(table_names)
}

write_to_db_once_off <- function(conn, df, dfname, recreate = FALSE, calcs_start_date = NULL, report_end_date = NULL) {

    table_name <- dfname
    datefield <- intersect(names(df), c("Month", "Date", "Hour", "Timeperiod"))
    start_date <- calcs_start_date

    tryCatch({
        if (recreate) {
            print(glue("{Sys.time()} Writing {table_name} | 3 | recreate = {recreate}"))
            DBI::dbWriteTable(conn,
                         table_name,
                         head(df, 3),
                         overwrite = TRUE,
                         row.names = FALSE)
        } else {
            if (table_name %in% dbListTables(conn)) {
                # Filter Dates and Append
                if (!is.null(start_date) & length(datefield) == 1) {
                    df <- filter(df, !!as.name(datefield) >= start_date)
                }
                if (!is.null(report_end_date) & length(datefield) == 1) {
                    df <- filter(df, !!as.name(datefield) < ymd(report_end_date) + months(1))
                }
                print(glue("{Sys.time()} Writing {table_name} | {scales::comma_format()(nrow(df))} | recreate = {recreate}"))

                # Clear Prior to Append
                dbExecute(conn, glue("TRUNCATE TABLE {table_name}"))

                DBI::dbWriteTable(
                    conn,
                    table_name,
                    df,
                    overwrite = FALSE,
                    append = TRUE,
                    row.names = FALSE
                )
            }
        }

    }, error = function(e) {
        print(glue("{Sys.time()} {e}"))
    })
}


set_index_aurora <- function(conn, table_name) {

    fields <- dbListFields(conn, table_name)
    period <- intersect(fields, c("Month", "Date", "Hour", "Timeperiod", "Quarter"))

    if (length(period) > 1) {
        print("More than one possible period in table fields")
        return(0)
    }

    indexes <- dbGetQuery(conn, glue("SHOW INDEXES FROM {table_name}"))

    # Indexes on Zone Group and Period
    if (!glue("idx_{table_name}_zone_period") %in% indexes$Key_name) {
        dbExecute(conn, glue(paste(
            "CREATE INDEX idx_{table_name}_zone_period",
            "ON {table_name} (Zone_Group, {period})")))
    }

    # Indexes on Corridor and Period
    if (!glue("idx_{table_name}_corridor_period") %in% indexes$Key_name) {
        dbExecute(conn, glue(paste(
            "CREATE INDEX idx_{table_name}_corridor_period",
            "ON {table_name} (Corridor, {period})")))
    }

    # Unique Index on Period, Zone Group and Corridor
    if (!glue("idx_{table_name}_unique") %in% indexes$Key_name) {
        dbExecute(conn, glue(paste(
            "CREATE UNIQUE INDEX idx_{table_name}_unique",
            "ON {table_name} ({period}, Zone_Group, Corridor)")))
    }
}



add_primary_key_id_field_aurora <- function(conn, table_name) {
    print(glue("{Sys.time()} {table_name}"))
    dbBegin(conn)
    if (!"id" %in% dbListFields(conn, table_name)) {
        try(
            dbExecute(conn, glue(
                "ALTER TABLE {table_name} ADD id INT UNSIGNED NOT NULL AUTO_INCREMENT, ADD PRIMARY KEY (id)"))
        )
    }
    dbCommit(conn)
}



convert_to_key_value_df <- function(key, df) {
    data.frame(
        key = key,
        data = rjson::toJSON(df),
        stringsAsFactors = FALSE)

}



recreate_database <- function(conn, df, dfname) {

    # Prep before writing to db. These come from Health_Metrics.R
    if ("maint" %in% names(df$mo)) {
        df$mo$maint <- mutate(df$mo$maint, Zone_Group = Zone)
    }
    if ("ops" %in% names(df$mo)) {
        df$mo$ops <- mutate(df$mo$ops, Zone_Group = Zone)
    }
    if ("safety" %in% names(df$mo)) {
        df$mo$safety <- mutate(df$mo$safety, Zone_Group = Zone)
    }

    # This is a more complex data structure. Convert to a JSON string that can be unwound on query.
    if ("udc_trend_table" %in% names(df$mo)) {
        df$mo$udc_trend_table <- convert_to_key_value_df("udc", df$mo$udc_trend_table)
    }

    table_names <- write_sigops_to_db(conn, df, dfname, recreate = TRUE)

    if ("udc_trend_table" %in% names(df$mo)) {
        write_to_db_once_off(conn, df$mo$udc_trend_table, glue("{dfname}_mo_udc_trend"), recreate = TRUE)
    }
    if ("hourly_udc" %in% names(df$mo)) {
        write_to_db_once_off(conn, df$mo$hourly_udc, glue("{dfname}_mo_hourly_udc"), recreate = TRUE)
    }
    if ("summary_data" %in% names(df)) {
        write_to_db_once_off(conn, df$summary_data, glue("{dfname}_summary_data"), recreate = TRUE)
    }

    if (class(conn) == "MySQLConnection" | class(conn)[[1]] == "MariaDBConnection") { # Aurora
        print(glue("{Sys.time()} Aurora Database Connection"))

        # Get CREATE TABLE Statements for each Table
        create_dfs <- lapply(
            table_names,
            function(table_name) {
                tryCatch({
                    dbGetQuery(conn, glue("show create table {table_name};"))
                }, error = function(e) {
                    NULL
                })
            })

        create_statements <- bind_rows(create_dfs) %>%
            as_tibble()

        # Modify CREATE TABLE Statements
        # To change text to VARCHAR with fixed size because this is required for indexing these fields
        # This is needed for Aurora
        for (swap in list(
            c("bigint[^ ,]+", "INT"),
            c("varchar[^ ,]+", "VARCHAR(128)"),
            c("`Zone_Group` [^ ,]+", "`Zone_Group` VARCHAR(128)"),
            c("`Corridor` [^ ,]+", "`Corridor` VARCHAR(128)"),
            c("`Quarter` [^ ,]+", "`Quarter` VARCHAR(8)"),
            c("`Date` [^ ,]+", "`Date` DATE"),
            c("`Month` [^ ,]+", "`Month` DATE"),
            c("`Hour` [^ ,]+", "`Hour` DATETIME"),
            c("`Timeperiod` [^ ,]+", "`Timeperiod` DATETIME"),
            c( "delta` [^ ,]+", "delta` DOUBLE"),
            c("`ones` [^ ,]+", "`ones` DOUBLE"),
            c("`data` [^ ,]+", "`data` mediumtext"),
            c("`Description` [^ ,]+", "`Description` VARCHAR(128)"),
            c( "Score` [^ ,]+", "Score` DOUBLE")
        )
        ) {
            create_statements[["Create Table"]] <- stringr::str_replace_all(create_statements[["Create Table"]], swap[1], swap[2])
        }

        # Delete and recreate with proper data types
        lapply(create_statements$Table, function(x) {
            dbRemoveTable(conn, x)
        })
        lapply(create_statements[["Create Table"]], function(x) {
            dbExecute(conn, x)
        })
        # Create Indexes
        lapply(create_statements$Table, function(x) {
            try(
                set_index_aurora(conn, x)
            )
        })
        # Add Primary Key id field.
        lapply(create_statements$Table, function(x) {
            add_primary_key_id_field_aurora(conn, x)
        })
    }
}



append_to_database <- function(
    conn, df, dfname,
    calcs_start_date = NULL,
    report_start_date = NULL,
    report_end_date = NULL
) {
    dbExecute(conn, "SET SESSION innodb_lock_wait_timeout = 50000;")

    # Prep before writing to db. These come from Health_Metrics.R
    if ("maint" %in% names(df$mo)) {
        df$mo$maint <- mutate(df$mo$maint, Zone_Group = Zone)
    }
    if ("ops" %in% names(df$mo)) {
        df$mo$ops <- mutate(df$mo$ops, Zone_Group = Zone)
    }
    if ("safety" %in% names(df$mo)) {
        df$mo$safety <- mutate(df$mo$safety, Zone_Group = Zone)
    }

    # This is a more complex data structure. Convert to a JSON string that can be unwound on query.
    if ("udc_trend_table" %in% names(df$mo)) {
        if (length(intersect(names(df$mo$udc_trend_table), c("key", "data"))) < 2) {
            df$mo$udc_trend_table <- convert_to_key_value_df("udc", df$mo$udc_trend_table)
        }
    }

    if ("summary_data" %in% names(df)) {
        write_to_db_once_off(conn, df$summary_data, glue("{dfname}_summary_data"), recreate = FALSE)
    }

    write_sigops_to_db(
        conn, df, dfname,
        recreate = FALSE,
        calcs_start_date,
        report_start_date,
        report_end_date)
}
