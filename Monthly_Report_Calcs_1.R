
# Monthly_Report_Calcs_1.R

source("Monthly_Report_Calcs_init.R")


# # GET CAMERA UPTIMES ########################################################

print(glue("{Sys.time()} parse cctv logs [1 of 11]"))

if (conf$run$cctv == TRUE || is.null(conf$run$cctv)) {
    # Run python scripts asynchronously
    system("~/miniconda3/bin/conda run -n sigops python parse_cctvlog.py", wait = FALSE)
    system("~/miniconda3/bin/conda run -n sigops python parse_cctvlog_encoders.py", wait = FALSE)
}

# # GET RSU UPTIMES ###########################################################

print(glue("{Sys.time()} parse rsu logs [2 of 11]"))

if (conf$run$rsus == TRUE) {
    # Run python script asynchronously
    system("~/miniconda3/bin/conda run -n sigops python parse_rsus.py", wait = FALSE)
}

# # TRAVEL TIMES FROM RITIS API ###############################################

print(glue("{Sys.time()} travel times [3 of 11]"))

if (conf$run$travel_times == TRUE || is.null(conf$run$travel_times)) {
    # Run python script asynchronously
    system("~/miniconda3/bin/conda run -n sigops python get_travel_times_v2.py mark travel_times_1hr.yaml", wait = FALSE)
    system("~/miniconda3/bin/conda run -n sigops python get_travel_times_v2.py mark travel_times_15min.yaml", wait = FALSE)
    system("~/miniconda3/bin/conda run -n sigops python get_travel_times_1min_v2.py mark", wait = FALSE)
}

# # COUNTS ####################################################################

print(glue("{Sys.time()} counts [4 of 11]"))

if (conf$run$counts == TRUE || is.null(conf$run$counts)) {
    date_range <- seq(ymd(start_date), ymd(end_date), by = "1 day")

    if (length(date_range) == 1) {
        get_counts2(
            date_range[1],
            bucket = conf$bucket,
            conf_athena = conf$athena,
            uptime = TRUE,
            counts = TRUE
        )
    } else {
        foreach(date_ = date_range, .errorhandling = "pass") %dopar% {
            keep_trying(get_counts2, n_tries = 2,
                date_,
                bucket = conf$bucket,
                conf_athena = conf$athena,
                uptime = TRUE,
                counts = TRUE
            )
        }
    }
}


print("\n---------------------- Finished counts ---------------------------\n")

print(glue("{Sys.time()} monthly cu [5 of 11]"))

# Group into months to calculate filtered and adjusted counts
# adjusted counts needs a full month to fill in gaps based on monthly averages


# Read Raw Counts for a month from files and output:
#   filtered_counts_1hr
#   adjusted_counts_1hr
#   BadDetectors

print(glue("{Sys.time()} counts-based measures [6 of 11]"))

get_counts_based_measures <- function(month_abbrs) {
    lapply(month_abbrs, function(yyyy_mm) {
        # yyyy_mm <- month_abbrs[1] # for debugging

        #-----------------------------------------------
        # 1-hour counts, filtered, adjusted, bad detectors

        # start and end days of the month
        sd <- ymd(paste0(yyyy_mm, "-01"))
        ed <- sd + months(1) - days(1)
        ed <- min(ed, ymd(end_date))
        date_range <- seq(sd, ed, by = "1 day")


        print("1-hour adjusted counts")
        prep_db_for_adjusted_counts_arrow("filtered_counts_1hr", conf, date_range)
        get_adjusted_counts_arrow("filtered_counts_1hr", "adjusted_counts_1hr", conf)

        fc_ds <- keep_trying(
            function() arrow::open_dataset(sources = "filtered_counts_1hr/"),
            n_tries = 3, timeout = 60)
        ac_ds <- keep_trying(
            function() arrow::open_dataset(sources = "adjusted_counts_1hr/"),
            n_tries = 3, timeout = 60)

        lapply(date_range, function(date_) {
            if (nrow(head(ac_ds))==0) {
                adjusted_counts_1hr <- tibble(
                    SignalID=character(0),
                    CallPhase=integer(0),
                    Detector=integer(0),
                    Timeperiod=as.POSIXct(NA),
                    vol=integer(0))
            } else {
                adjusted_counts_1hr <- ac_ds %>%
                    filter(Date == date_) %>%
                    select(-c(Date, date)) %>%
                    collect()
            }
            s3_upload_parquet_date_split(
                adjusted_counts_1hr,
                bucket = conf$bucket,
                prefix = "adjusted_counts_1hr",
                table_name = "adjusted_counts_1hr",
                conf_athena = conf$athena
            )
        })

        lapply(date_range, function(date_) {
            date_str <- format(date_, "%F")
            write_signal_details(date_str, conf, signals_list)
        })


        mclapply(date_range, mc.cores = usable_cores, mc.preschedule = FALSE, FUN = function(date_) {
            date_str <- format(date_, "%F")

            print(glue("reading adjusted_counts_1hr: {date_str}"))
            if (nrow(head(ac_ds))==0) {
                adjusted_counts_1hr <- ac_ds
            } else {
                adjusted_counts_1hr <- ac_ds %>%
                    filter(date == date_str) %>%
                    select(-date) %>%
                    collect()
            }
            if (!is.null(adjusted_counts_1hr) && nrow(adjusted_counts_1hr)) {
                adjusted_counts_1hr <- adjusted_counts_1hr %>%
                    mutate(
                        Date = date(Date),
                        SignalID = factor(SignalID),
                        CallPhase = factor(CallPhase),
                        Detector = factor(Detector)
                    )

                # VPD
                print(glue("vpd: {date_}"))
                vpd <- get_vpd(adjusted_counts_1hr) # calculate over current period
                s3_upload_parquet_date_split(
                    vpd,
                    bucket = conf$bucket,
                    prefix = "vpd",
                    table_name = "vehicles_pd",
                    conf_athena = conf$athena
                )

                # VPH
                print(glue("vph: {date_}"))
                vph <- get_vph(adjusted_counts_1hr, interval = "1 hour")
                s3_upload_parquet_date_split(
                    vph,
                    bucket = conf$bucket,
                    prefix = "vph",
                    table_name = "vehicles_ph",
                    conf_athena = conf$athena
                )
            }
        })
        if (dir.exists("filtered_counts_1hr")) {
            unlink("filtered_counts_1hr", recursive = TRUE)
        }
        if (dir.exists("adjusted_counts_1hr")) {
            unlink("adjusted_counts_1hr", recursive = TRUE)
        }



        #-----------------------------------------------
        # 15-minute counts and throughput
        # FOR EVERY TUE, WED, THU OVER THE WHOLE MONTH
        print("15-minute counts and throughput")

        print("15-minute adjusted counts")
        prep_db_for_adjusted_counts_arrow("filtered_counts_15min", conf, date_range)
        get_adjusted_counts_arrow("filtered_counts_15min", "adjusted_counts_15min", conf)

        fc_ds <- keep_trying(
            function() arrow::open_dataset(sources = "filtered_counts_15min/"),
            n_tries = 3, timeout = 60)
        ac_ds <- keep_trying(
            function() arrow::open_dataset(sources = "adjusted_counts_15min/"),
            n_tries = 3, timeout = 60)

        lapply(date_range, function(date_) {
            if (nrow(head(ac_ds))==0) {
                adjusted_counts_15min <- tibble(
                    SignalID=character(0),
                    CallPhase=integer(0),
                    Detector=integer(0),
                    Timeperiod=as.POSIXct(NA),
                    vol=integer(0))
            } else {
                adjusted_counts_15min <- ac_ds %>%
                    filter(Date == date_) %>%
                    select(-c(Date, date)) %>%
                    collect()
            }
            s3_upload_parquet_date_split(
                adjusted_counts_15min,
                bucket = conf$bucket,
                prefix = "adjusted_counts_15min",
                table_name = "adjusted_counts_15min",
                conf_athena = conf$athena
            )

            throughput <- get_thruput(adjusted_counts_15min)
            s3_upload_parquet_date_split(
                throughput,
                bucket = conf$bucket,
                prefix = "tp",
                table_name = "throughput",
                conf_athena = conf$athena
            )

            # Vehicles per 15-minute timeperiod
            print(glue("vp15: {date_}"))
            vp15 <- get_vph(adjusted_counts_15min, interval = "15 min")
            s3_upload_parquet_date_split(
                vp15,
                bucket = conf$bucket,
                prefix = "vp15",
                table_name = "vehicles_15min",
                conf_athena = conf$athena
            )
        })

        if (dir.exists("filtered_counts_15min")) {
            unlink("filtered_counts_15min", recursive = TRUE)
        }
        if (dir.exists("adjusted_counts_15min")) {
            unlink("adjusted_counts_15min", recursive = TRUE)
        }
    })
}
if (conf$run$counts_based_measures == TRUE || is.null(conf$run$counts_based_measures)) {
    get_counts_based_measures(month_abbrs)
}

closeAllConnections()
print("--- Finished counts-based measures ---")
