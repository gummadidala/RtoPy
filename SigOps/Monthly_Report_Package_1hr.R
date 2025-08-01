
# Monthly_Report_Package.R -- For Hourly Data

source("Monthly_Report_Package_init.R")

# For hourly counts (no monthly or weekly), go back to first missing day in the database
calcs_start_date <- get_date_from_string(
    conf$start_date, table_include_regex_pattern = "sig_hr_", exceptions = 0
)
report_end_date <- min(as_date(report_end_date), calcs_start_date + days(7))

print(glue("{Sys.time()} 1hr Package Start Date: {calcs_start_date}"))
print(glue("{Sys.time()} Report End Date: {report_end_date}"))

# Need to keep some data in rds prior to the calcs_start_date to calculate accurate deltas
rds_start_date <- calcs_start_date - days(1)

# Only keep 6 months of data for 1hr aggregations
report_start_date <- floor_date(as_date(report_end_date), unit = "months") - months(6)


# # DETECTOR UPTIME ###########################################################

print(glue("{Sys.time()} Vehicle Detector Uptime [1 of 29 (sigops 1hr)]"))

# DAILY PEDESTRIAN PUSHBUTTON UPTIME ##########################################

print(glue("{Sys.time()} Ped Pushbutton Uptime [2 of 29 (sigops 1hr)]"))

# # WATCHDOG ##################################################################

print(glue("{Sys.time()} watchdog alerts [3 of 29 (sigops 1hr)]"))

# DAILY PEDESTRIAN ACTIVATIONS ################################################

print(glue("{Sys.time()} Daily Pedestrian Activations [4 of 29 (sigops 1hr)]"))

# HOURLY PEDESTRIAN ACTIVATIONS ###############################################

print(glue("{Sys.time()} Hourly Pedestrian Activations [5 of 29 (sigops 1hr)]"))

tryCatch(
    {
        paph <- s3_read_parquet_parallel(
            bucket = conf$bucket,
            table_name = "counts_ped_1hr",
            start_date = rds_start_date,
            end_date = report_end_date,
            signals_list = signals_list,
            parallel = FALSE
        ) %>%
            filter(!is.na(CallPhase)) %>%   # Added 1/14/20 to perhaps exclude non-programmed ped pushbuttons
            mutate(
                SignalID = factor(SignalID),
                Detector = factor(Detector),
                CallPhase = factor(CallPhase),
                Date = date(Date),
                DOW = wday(Date),
                Week = week(Date),
                vol = as.numeric(vol)
            )

        bad_ped_detectors <- s3_read_parquet_parallel(
            bucket = conf$bucket,
            table_name = "bad_ped_detectors",
            start_date = rds_start_date,
            end_date = report_end_date,
            signals_list = signals_list,
            parallel = FALSE
        ) %>%
            mutate(
                SignalID = factor(SignalID),
                Detector = factor(Detector))

        # Filter out bad days
        paph <- paph %>%
            rename(Hour = Timeperiod, paph = vol) %>%
            select(SignalID, Hour, CallPhase, Detector, paph) %>%
            anti_join(bad_ped_detectors)

        all_timeperiods <- seq(
            floor_date(min(paph$Hour), unit = "days"),
            floor_date(max(paph$Hour), unit = "days") + days(1) - hours(1),
            by = "1 hour"
        )
        paph <- paph %>%
            expand(nesting(SignalID, Detector, CallPhase), Hour = all_timeperiods)%>%
            left_join(paph) %>%
            replace_na(list(vol = 0))

        hourly_pa <- get_period_sum(paph, "paph", "Hour")
        cor_hourly_pa <- get_cor_monthly_avg_by_period(hourly_pa, corridors, "paph", "Hour")

        hourly_pa <- sigify(hourly_pa, cor_hourly_pa, corridors) %>%
                select(Zone_Group, Corridor, Hour, paph, delta)


        addtoRDS(
            hourly_pa, "hourly_pa.rds", "paph", rds_start_date, calcs_start_date
        )

        rm(paph)
        rm(bad_ped_detectors)
        rm(hourly_pa)
        rm(cor_hourly_pa)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)

# GET PEDESTRIAN DELAY ########################################################

print(glue("{Sys.time()} Pedestrian Delay [6 of 29 (sigops 1hr)]"))

# GET COMMUNICATIONS UPTIME ###################################################

print(glue("{Sys.time()} Communication Uptime [7 of 29 (sigops 1hr)]"))

# DAILY VOLUMES ###############################################################

print(glue("{Sys.time()} Daily Volumes [8 of 29 (sigops 1hr)]"))

# HOURLY VOLUMES ##############################################################

print(glue("{Sys.time()} Hourly Volumes [9 of 29 (sigops 1hr)]"))

tryCatch(
    {
        vph <- s3_read_parquet_parallel(
            bucket = conf$bucket,
            table_name = "vehicles_ph",
            start_date = rds_start_date,
            end_date = report_end_date,
            signals_list = signals_list
        ) %>%
            mutate(
                SignalID = factor(SignalID),
                CallPhase = factor(2), # Hack because next function needs a CallPhase
                Date = date(Date)
            )

        hourly_vol <- get_period_sum(vph, "vph", "Hour")
        cor_hourly_vol <- get_cor_monthly_avg_by_period(hourly_vol, corridors, "vph", "Hour")

        hourly_vol <- sigify(hourly_vol, cor_hourly_vol, corridors) %>%
                select(Zone_Group, Corridor, Hour, vph, delta)


        addtoRDS(
            hourly_vol, "hourly_vol.rds", "vph", rds_start_date, calcs_start_date
        )

        rm(vph)
        rm(hourly_vol)
        rm(cor_hourly_vol)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)

# DAILY THROUGHPUT ############################################################

print(glue("{Sys.time()} Daily Throughput [10 of 29 (sigops 1hr)]"))

# DAILY ARRIVALS ON GREEN #####################################################

print(glue("{Sys.time()} Daily AOG [11 of 29 (sigops 1hr)]"))

# HOURLY ARRIVALS ON GREEN ####################################################

print(glue("{Sys.time()} Hourly AOG [12 of 29 (sigops 1hr)]"))

tryCatch(
    {
        aog <- s3_read_parquet_parallel(
            bucket = conf$bucket,
            table_name = "arrivals_on_green",
            start_date = rds_start_date,
            end_date = report_end_date,
            signals_list = signals_list
        ) %>%
            mutate(
                SignalID = factor(SignalID),
                CallPhase = factor(CallPhase),
                Date = date(Date),
                DOW = wday(Date),
                Week = week(Date)
            )

        # Don't fill in gaps (leave as NA)
        # since no volume means no value for aog or pr (it's not 0)
        aog <- aog %>%
            rename(Hour = Date_Hour) %>%
            select(SignalID, CallPhase, Hour, aog, pr, vol, Date)

        hourly_aog <- aog %>%
            # Don't fill in gaps (leave as NA)
            # since no volume means no value for aog or pr (it's not 0)
            complete(
                nesting(SignalID, Date),
                Hour = seq(as_datetime(rds_start_date), as_datetime(report_end_date) - hours(1), by = "1 hour")
            ) %>%
            get_period_avg("aog", "Hour", "vol")
        cor_hourly_aog <- get_cor_monthly_avg_by_period(hourly_aog, corridors, "aog", "Hour")

        hourly_aog <- sigify(hourly_aog, cor_hourly_aog, corridors) %>%
                select(Zone_Group, Corridor, Hour, aog, delta)


        addtoRDS(
            hourly_aog, "hourly_aog.rds", "aog", rds_start_date, calcs_start_date
        )

        rm(hourly_aog)
        rm(cor_hourly_aog)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)

# DAILY PROGRESSION RATIO #####################################################

print(glue("{Sys.time()} Daily Progression Ratio [13 of 29 (sigops 1hr)]"))

# HOURLY PROGESSION RATIO #####################################################

print(glue("{Sys.time()} Hourly Progression Ratio [14 of 29 (sigops 1hr)]"))

tryCatch(
    {
        hourly_pr <- aog %>%
            # Don't fill in gaps (leave as NA)
            # since no volume means no value for aog or pr (it's not 0)
            complete(
                nesting(SignalID, Date),
                Hour = seq(as_datetime(rds_start_date), as_datetime(report_end_date) - hours(1), by = "1 hour")
            ) %>%
            get_period_avg("pr", "Hour", "vol")

        cor_hourly_pr <- get_cor_monthly_avg_by_period(hourly_pr, corridors, "pr", "Hour")

        hourly_pr <- sigify(hourly_pr, cor_hourly_pr, corridors) %>%
                select(Zone_Group, Corridor, Hour, pr, delta)


        addtoRDS(
            hourly_pr, "hourly_pr.rds", "pr", rds_start_date, calcs_start_date
        )

        rm(aog)
        rm(hourly_pr)
        rm(cor_hourly_pr)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)

# DAILY SPLIT FAILURES ########################################################

print(glue("{Sys.time()} Daily Split Failures [15 of 29 (sigops 1hr)]"))

# HOURLY SPLIT FAILURES #######################################################

print(glue("{Sys.time()} Hourly Split Failures [16 of 29 (sigops 1hr)]"))

tryCatch(
    {
        sf <- s3_read_parquet_parallel(
            bucket = conf$bucket,
            table_name = "split_failures",
            start_date = rds_start_date,
            end_date = report_end_date,
            signals_list = signals_list,
            callback = function(x) filter(x, CallPhase == 0)
        ) %>%
            mutate(
                SignalID = factor(SignalID),
                CallPhase = factor(CallPhase),
                Date = date(Date)
            )

        sf <- sf %>%
            rename(Hour = Date_Hour) %>%
            select(SignalID, CallPhase, Hour, sf_freq, Date)

        hourly_sf <- sf %>%
            complete(
                nesting(SignalID, Date, CallPhase),
                Hour = seq(as_datetime(rds_start_date), as_datetime(report_end_date) - hours(1), by = "1 hour"),
                fill = list(sf_freq = 0)
            ) %>%
            get_period_avg("sf_freq", "Hour")

        cor_hourly_sf <- get_cor_monthly_avg_by_period(hourly_sf, corridors, "sf_freq", "Hour")

        hourly_sf <- sigify(hourly_sf, cor_hourly_sf, corridors) %>%
                select(Zone_Group, Corridor, Hour, sf_freq, delta)


        addtoRDS(
            hourly_sf, "hourly_sf.rds", "sf_freq", rds_start_date, calcs_start_date
        )

        rm(sf)
        rm(hourly_sf)
        rm(cor_hourly_sf)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)

# DAILY QUEUE SPILLBACK #######################################################

print(glue("{Sys.time()} Daily Queue Spillback [17 of 29 (sigops 1hr)]"))

# HOURLY QUEUE SPILLBACK ######################################################

print(glue("{Sys.time()} Hourly Queue Spillback [18 of 29 (sigops 1hr)]"))

tryCatch(
    {
        qs <- s3_read_parquet_parallel(
            bucket = conf$bucket,
            table_name = "queue_spillback",
            start_date = calcs_start_date,
            end_date = report_end_date,
            signals_list = signals_list
        ) %>%
            mutate(
                SignalID = factor(SignalID),
                CallPhase = factor(CallPhase),
                Date = date(Date)
            )

        qs <- qs %>%
            rename(Hour = Date_Hour) %>%
            select(SignalID, CallPhase, Hour, qs_freq, Date)

        hourly_qs <- qs %>%
            complete(
                nesting(SignalID, Date, CallPhase),
                Hour = seq(as_datetime(rds_start_date), as_datetime(report_end_date) - hours(1), by = "1 hour"),
                fill = list(qs_freq = 0)
            ) %>%
            get_period_avg("qs_freq", "Hour")

        cor_hourly_qs <- get_cor_monthly_avg_by_period(hourly_qs, corridors, "qs_freq", "Hour")

        hourly_qs <- sigify(hourly_qs, cor_hourly_qs, corridors) %>%
                select(Zone_Group, Corridor, Hour, qs_freq, delta)


        addtoRDS(
            hourly_qs, "hourly_qs.rds", "qs_freq", rds_start_date, calcs_start_date
        )

        rm(qs)
        rm(hourly_qs)
        rm(cor_hourly_qs)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)

# TRAVEL TIME AND BUFFER TIME INDEXES #########################################

print(glue("{Sys.time()} Travel Time Indexes [19 of 29 (sigops 1hr)]"))

# CCTV UPTIME From 511 and Encoders

print(glue("{Sys.time()} CCTV Uptimes [20 of 29 (sigops 1hr)]"))

# ACTIVITIES ##################################################################

print(glue("{Sys.time()} TEAMS [21 of 29 (sigops 1hr)]"))

# USER DELAY COSTS   ##########################################################

print(glue("{Sys.time()} User Delay Costs [22 of 29 (sigops 1hr)]"))

# Flash Events ################################################################

print(glue("{Sys.time()} Flash Events [23 of 29 (sigops 1hr)]"))

# BIKE/PED SAFETY INDEX #######################################################

print(glue("{Sys.time()} Bike/Ped Safety Index [24 of 29 (sigops 1hr)]"))

# RELATIVE SPEED INDEX ########################################################

print(glue("{Sys.time()} Relative Speed Index [25 of 29 (sigops 1hr)]"))

# CRASH INDICES ###############################################################

print(glue("{Sys.time()} Crash Indices [26 of 29 (sigops 1hr)]"))





# Package up for Flexdashboard

# All of the hourly bins.

tryCatch(
    {
        sig <- list()
        sig$hr <- list(
            "vph" = readRDS("hourly_vol.rds"),
            "paph" = readRDS("hourly_pa.rds"),
            "aogh" = readRDS("hourly_aog.rds"),
            "prh" = readRDS("hourly_pr.rds"),
            "sfh" = readRDS("hourly_sf.rds"),
            "qsh" = readRDS("hourly_qs.rds")
        )
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)





print(glue("{Sys.time()} Upload to AWS [28 of 29 (sigops 1hr)]"))


print(glue("{Sys.time()} Write to Database [29 of 29 (sigops 1hr)]"))

source("write_sigops_to_db.R")

# Update Aurora Nightly
aurora <- keep_trying(func = get_aurora_connection, n_tries = 5)
# recreate_database(conn)

tryCatch({
    append_to_database(
        aurora, sig, "sig", calcs_start_date, report_start_date = report_start_date, report_end_date = NULL)
},
finally = {
    dbDisconnect(aurora)
})
