
# Monthly_Report_Calcs_2.R

source("Monthly_Report_Calcs_init.R")


# -- Run etl_dashboard (Python): cycledata, detectionevents to S3/Athena --
print(glue("{Sys.time()} etl [7 of 11]"))

if (conf$run$etl == TRUE || is.null(conf$run$etl)) {

    # run python script and wait for completion
    system(glue("~/miniconda3/bin/conda run -n sigops python etl_dashboard.py {start_date} {end_date}"))
}

# --- ----------------------------- -----------

# # GET ARRIVALS ON GREEN #####################################################
print(glue("{Sys.time()} aog [8 of 11]"))

if (conf$run$arrivals_on_green == TRUE || is.null(conf$run$arrivals_on_green)) {

    # run python script and wait for completion
    system(glue("~/miniconda3/bin/conda run -n sigops python get_aog.py {start_date} {end_date}"))
}
invisible(gc())

# # GET QUEUE SPILLBACK #######################################################
get_queue_spillback_date_range <- function(start_date, end_date) {
    date_range <- seq(ymd(start_date), ymd(end_date), by = "1 day")

    lapply(date_range, function(date_) {
        print(date_)

        detection_events <- get_detection_events(date_, date_, conf$athena, signals_list)
        if (nrow(collect(head(detection_events))) > 0) {

            qs <- get_qs(detection_events, intervals = c("hour", "15min"))

            s3_upload_parquet_date_split(
                qs$hour,
                bucket = conf$bucket,
                prefix = "qs",
                table_name = "queue_spillback",
                conf_athena = conf$athena
            )
            s3_upload_parquet_date_split(
                qs$`15min`,
                bucket = conf$bucket,
                prefix = "qs",
                table_name = "queue_spillback_15min",
                conf_athena = conf$athena
            )
        }
    })
}
print(glue("{Sys.time()} queue spillback [9 of 11]"))

if (conf$run$queue_spillback == TRUE || is.null(conf$run$queue_spillback)) {
    get_queue_spillback_date_range(start_date, end_date)
}



# # GET PED DELAY ########################################################

# Ped delay using ATSPM method, based on push button-start of walk durations
print(glue("{Sys.time()} ped delay [10 of 11]"))

get_pd_date_range <- function(start_date, end_date) {
    date_range <- seq(ymd(start_date), ymd(end_date), by = "1 day")

    lapply(date_range, function(date_) {
        print(date_)
        run_parallel <- length(date_range) > 1

        pd <- get_ped_delay(date_, conf, signals_list)
        if (nrow(pd) > 0) {
            s3_upload_parquet_date_split(
                pd,
                bucket = conf$bucket,
                prefix = "pd",
                table_name = "ped_delay",
                conf_athena = conf$athena
            )
        }
    })
    invisible(gc())
}

if (conf$run$ped_delay == TRUE || is.null(conf$run$ped_delay)) {
    get_pd_date_range(start_date, end_date)
}



# # GET SPLIT FAILURES ########################################################

print(glue("{Sys.time()} split failures [11 of 11]"))

get_sf_date_range <- function(start_date, end_date) {
    date_range <- seq(ymd(start_date), ymd(end_date), by = "1 day")

    lapply(date_range, function(date_) {
        print(date_)

        sf <- get_sf_utah(date_, conf, signals_list, intervals = c("hour", "15min"))

        if (nrow(sf$hour) > 0) {
            s3_upload_parquet_date_split(
                sf$hour,
                bucket = conf$bucket,
                prefix = "sf",
                table_name = "split_failures",
                conf_athena = conf$athena
            )
        }
        if (nrow(sf$`15min`) > 0) {
            s3_upload_parquet_date_split(
                sf$`15min`,
                bucket = conf$bucket,
                prefix = "sf",
                table_name = "split_failures_15min",
                conf_athena = conf$athena
            )
        }
    })
}

if (conf$run$split_failures == TRUE || is.null(conf$run$split_failures)) {
    # Utah method, based on green, start-of-red occupancies
    get_sf_date_range(start_date, end_date)
}



# # FLASH EVENTS #############################################################
print(glue("{Sys.time()} flash events [12 of 12]"))

if (conf$run$flash_events == TRUE || is.null(conf$run$flash_events)) {
    # run python script and wait for completion
    system(glue("~/miniconda3/bin/conda run -n sigops python get_flash_events.py"))
}
invisible(gc())



closeAllConnections()

print("\n--------------------- End Monthly Report calcs -----------------------\n")
