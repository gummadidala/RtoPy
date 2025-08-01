
s3_upload_parquet <- function(df, date_, fn, bucket, table_name, conf_athena) {

    df <- ungroup(df)

    if ("Date" %in% names(df)) {
        df <- df %>% select(-Date)
    }


    if ("Detector" %in% names(df)) {
        df <- mutate(df, Detector = as.character(Detector))
    }
    if ("CallPhase" %in% names(df)) {
        df <- mutate(df, CallPhase = as.character(CallPhase))
    }
    if ("SignalID" %in% names(df)) {
        df <- mutate(df, SignalID = as.character(SignalID))
    }

    keep_trying(
        s3write_using,
        n_tries = 5,
        df,
        write_parquet,
        use_deprecated_int96_timestamps = TRUE,
        bucket = bucket,
        object = glue("mark/{table_name}/date={date_}/{fn}.parquet"),
        opts = list(multipart = TRUE, body_as_string = TRUE)
    )

    add_athena_partition(conf_athena, bucket, table_name, date_)
}



s3_upload_parquet_date_split <- function(df, prefix, bucket, table_name, conf_athena, parallel = FALSE) {

    if (!("Date" %in% names(df))) {
        if ("Timeperiod" %in% names(df)) {
            df <- mutate(df, Date = date(Timeperiod))
        } else if ("Hour" %in% names(df)) {
            df <- mutate(df, Date = date(Hour))
        }
    }

    d <- unique(df$Date)
    if (length(d) == 1) { # just one date. upload.
        date_ <- d
        s3_upload_parquet(df, date_,
                          fn = glue("{prefix}_{date_}"),
                          bucket = bucket,
                          table_name = table_name,
                          conf_athena = conf_athena)
    } else { # loop through dates
        if (parallel & Sys.info()["sysname"] != "Windows") {
            df %>%
                split(.$Date) %>%
                mclapply(mc.cores = usable_cores, FUN = function(x) {
                    date_ <- as.character(x$Date[1])
                    s3_upload_parquet(x, date_,
                                      fn = glue("{prefix}_{date_}"),
                                      bucket = bucket,
                                      table_name = table_name,
                                      conf_athena = conf_athena)
                    Sys.sleep(1)
                })
        } else {
            df %>%
                split(.$Date) %>%
                lapply(function(x) {
                    date_ <- as.character(x$Date[1])
                    s3_upload_parquet(x, date_,
                                      fn = glue("{prefix}_{date_}"),
                                      bucket = bucket,
                                      table_name = table_name,
                                      conf_athena = conf_athena)
                })

        }
    }

}


s3_read_parquet <- function(bucket, object, date_ = NULL) {

    if (is.null(date_)) {
        date_ <- str_extract(object, "\\d{4}-\\d{2}-\\d{2}")
    }
    tryCatch({
        s3read_using(read_parquet, bucket = bucket, object = object) %>%
            select(-starts_with("__")) %>%
            mutate(Date = ymd(date_))
    }, error = function(e) {
        print(e)
        data.frame()
    })
}


s3_read_parquet_parallel <- function(table_name,
                                     start_date,
                                     end_date,
                                     signals_list = NULL,
                                     bucket = NULL,
                                     callback = function(x) {x},
                                     parallel = FALSE,
                                     s3root = 'mark') {

    dates <- seq(ymd(start_date), ymd(end_date), by = "1 day")

    func <- function(date_) {
        prefix <- glue("{s3root}/{table_name}/date={date_}")
        objects = aws.s3::get_bucket(bucket = bucket, prefix = prefix)
        lapply(objects, function(obj) {
            s3_read_parquet(bucket = bucket, object = get_objectkey(obj), date_) %>%
                convert_to_utc() %>%
                callback()
        }) %>% bind_rows()
    }
    # When using mclapply, it fails. When using lapply, it works. 6/23/2020
    # Give to option to run in parallel, like when in interactive mode
    if (parallel & Sys.info()["sysname"] != "Windows") {
        dfs <- mclapply(dates, mc.cores = usable_cores, FUN = func)
    } else {
        dfs <- lapply(dates, func)
    }
    dfs[lapply(dfs, nrow)>0] %>% bind_rows()
}
