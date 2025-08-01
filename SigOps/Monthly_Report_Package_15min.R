
# Monthly_Report_Package.R -- For 15-min Data

source("Monthly_Report_Package_init.R")

# For hourly counts (no monthly or weekly), go back to first missing day in the database
calcs_start_date <- get_date_from_string(
    conf$start_date, table_include_regex_pattern = "sig_qhr_", exceptions = 0
)
report_end_date <- min(as_date(report_end_date), calcs_start_date + days(4))

print(glue("{Sys.time()} 15min Package Start Date: {calcs_start_date}"))
print(glue("{Sys.time()} Report End Date: {report_end_date}"))

# Need to keep some data in rds prior to the calcs_start_date to calculate accurate deltas
rds_start_date <- calcs_start_date - days(1)

# Only keep 6 months of data for 15min aggregations.
report_start_date <- floor_date(as_date(report_end_date), unit = "months") - months(6)


# # DETECTOR UPTIME ###########################################################

print(glue("{Sys.time()} Vehicle Detector Uptime [1 of 29 (sigops 15min)]"))

# DAILY PEDESTRIAN PUSHBUTTON UPTIME ##########################################

print(glue("{Sys.time()} Ped Pushbutton Uptime [2 of 29 (sigops 15min)]"))

# # WATCHDOG ##################################################################

print(glue("{Sys.time()} watchdog alerts [3 of 29 (sigops 15min)]"))

# DAILY PEDESTRIAN ACTIVATIONS ################################################

print(glue("{Sys.time()} Daily Pedestrian Activations [4 of 29 (sigops 15min)]"))

# HOURLY PEDESTRIAN ACTIVATIONS ###############################################

print(glue("{Sys.time()} 15-Minute Pedestrian Activations [5 of 29 (sigops 15min)]"))

tryCatch(
    {
        paph <- s3_read_parquet_parallel(
            bucket = conf$bucket,
            table_name = "counts_ped_15min",
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
            select(SignalID, Timeperiod, CallPhase, Detector, vol) %>%
            anti_join(bad_ped_detectors)

        all_timeperiods <- seq(
            floor_date(min(paph$Timeperiod), unit = "days"),
            floor_date(max(paph$Timeperiod), unit = "days") + days(1) - minutes(15),
            by = "15 min"
        )
        paph <- paph %>%
            expand(nesting(SignalID, Detector, CallPhase), Timeperiod = all_timeperiods)%>%
            left_join(paph) %>%
            replace_na(list(vol = 0))

        pa_15min <- get_period_sum(paph, "vol", "Timeperiod")
        cor_15min_pa <- get_cor_monthly_avg_by_period(pa_15min, corridors, "vol", "Timeperiod")

        pa_15min <- sigify(pa_15min, cor_15min_pa, corridors) %>%
                select(Zone_Group, Corridor, Timeperiod, vol, delta)


        addtoRDS(
            pa_15min, "pa_15min.rds", "vol", rds_start_date, calcs_start_date
        )
        addtoRDS(
            cor_15min_pa, "cor_15min_pa.rds", "vol", rds_start_date, calcs_start_date
        )

        rm(paph)
        rm(bad_ped_detectors)
        rm(pa_15min)
        rm(cor_15min_pa)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)

# GET PEDESTRIAN DELAY ########################################################

print(glue("{Sys.time()} Pedestrian Delay [6 of 29 (sigops 15min)]"))

# GET COMMUNICATIONS UPTIME ###################################################

print(glue("{Sys.time()} Communication Uptime [7 of 29 (sigops 15min)]"))

# DAILY VOLUMES ###############################################################

print(glue("{Sys.time()} Daily Volumes [8 of 29 (sigops 15min)]"))

# HOURLY VOLUMES ##############################################################

print(glue("{Sys.time()} 15-Minute Volumes [9 of 29 (sigops 15min)]"))

tryCatch(
    {
        vph <- s3_read_parquet_parallel(
            bucket = conf$bucket,
            table_name = "vehicles_15min",
            start_date = rds_start_date,
            end_date = report_end_date,
            signals_list = signals_list
        ) %>%
            mutate(
                SignalID = factor(SignalID),
                CallPhase = factor(2), # Hack because next function needs a CallPhase
                Date = date(Date)
            ) %>%
            rename(vol = vph)

        vol_15min <- get_period_sum(vph, "vol", "Timeperiod")
        cor_15min_vol <- get_cor_monthly_avg_by_period(vol_15min, corridors, "vol", "Timeperiod")

        vol_15min <- sigify(vol_15min, cor_15min_vol, corridors) %>%
                select(Zone_Group, Corridor, Timeperiod, vol, delta)

        addtoRDS(
            vol_15min, "vol_15min.rds", "vol", rds_start_date, calcs_start_date
        )
        addtoRDS(
            cor_15min_vol, "cor_15min_vol.rds", "vol", rds_start_date, calcs_start_date
        )

        rm(vph)
        rm(vol_15min)
        rm(cor_15min_vol)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)

# DAILY THROUGHPUT ############################################################

print(glue("{Sys.time()} Daily Throughput [10 of 29 (sigops 15min)]"))

# DAILY ARRIVALS ON GREEN #####################################################

print(glue("{Sys.time()} Daily AOG [11 of 29 (sigops 15min)]"))

# HOURLY ARRIVALS ON GREEN ####################################################

print(glue("{Sys.time()} 15-Minute AOG [12 of 29 (sigops 15min)]"))

tryCatch(
    {
        aog <- s3_read_parquet_parallel(
            bucket = conf$bucket,
            table_name = "arrivals_on_green_15min",
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
            rename(Timeperiod = Date_Period) %>%
            select(SignalID, CallPhase, Timeperiod, aog, pr, vol, Date)

        aog_15min <- aog %>%
            # Don't fill in gaps (leave as NA)
            # since no volume means no value for aog or pr (it's not 0)
            complete(
                nesting(SignalID, Date),
                Timeperiod = seq(as_datetime(rds_start_date), as_datetime(report_end_date) - minutes(15), by = "15 min")
            ) %>%
            get_period_avg("aog", "Timeperiod", "vol")

        cor_15min_aog <- get_cor_monthly_avg_by_period(aog_15min, corridors, "aog", "Timeperiod")

        aog_15min <- sigify(aog_15min, cor_15min_aog, corridors) %>%
                select(Zone_Group, Corridor, Timeperiod, aog, delta)


        addtoRDS(
            aog_15min, "aog_15min.rds", "aog", rds_start_date, calcs_start_date
        )
        addtoRDS(
            cor_15min_aog, "cor_15min_aog.rds", "aog", rds_start_date, calcs_start_date
        )

        rm(aog_15min)
        rm(cor_15min_aog)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)

# DAILY PROGRESSION RATIO #####################################################

print(glue("{Sys.time()} Daily Progression Ratio [13 of 29 (sigops 15min)]"))

# HOURLY PROGESSION RATIO #####################################################

print(glue("{Sys.time()} 15-Minute Progression Ratio [14 of 29 (sigops 15min)]"))

tryCatch(
    {
        pr_15min <- aog %>%
            # Don't fill in gaps (leave as NA)
            # since no volume means no value for aog or pr (it's not 0)
            complete(
                nesting(SignalID, Date),
                Timeperiod = seq(as_datetime(rds_start_date), as_datetime(report_end_date) - minutes(15), by = "15 min")
            ) %>%
            get_period_avg("pr", "Timeperiod", "vol")

        cor_15min_pr <- get_cor_monthly_avg_by_period(pr_15min, corridors, "pr", "Timeperiod")

        pr_15min <- sigify(pr_15min, cor_15min_pr, corridors) %>%
                select(Zone_Group, Corridor, Timeperiod, pr, delta)


        addtoRDS(
            pr_15min, "pr_15min.rds", "pr", rds_start_date, calcs_start_date
        )
        addtoRDS(
            cor_15min_pr, "cor_15min_pr.rds", "pr", rds_start_date, calcs_start_date
        )

        rm(aog)
        rm(pr_15min)
        rm(cor_15min_pr)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)

# DAILY SPLIT FAILURES ########################################################

print(glue("{Sys.time()} Daily Split Failures [15 of 29 (sigops 15min)]"))

# HOURLY SPLIT FAILURES #######################################################

print(glue("{Sys.time()} 15-Minute Split Failures [16 of 29 (sigops 15min)]"))

tryCatch(
    {
        sf <- s3_read_parquet_parallel(
            bucket = conf$bucket,
            table_name = "split_failures_15min",
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
            rename(Timeperiod = Date_Hour) %>%
            select(SignalID, CallPhase, Timeperiod, sf_freq, Date)

        sf_15min <- sf %>%
            complete(
                nesting(SignalID, Date, CallPhase),
                Timeperiod = seq(as_datetime(rds_start_date), as_datetime(report_end_date) - minutes(15), by = "15 min"),
                fill = list(sf_freq = 0)
            ) %>%
            get_period_avg("sf_freq", "Timeperiod")

        cor_15min_sf <- get_cor_monthly_avg_by_period(sf_15min, corridors, "sf_freq", "Timeperiod")

        sf_15min <- sigify(sf_15min, cor_15min_sf, corridors) %>%
                select(Zone_Group, Corridor, Timeperiod, sf_freq, delta)


        addtoRDS(
            sf_15min, "sf_15min.rds", "sf_freq", rds_start_date, rds_start_date
        )
        addtoRDS(
            cor_15min_sf, "cor_15min_sf.rds", "sf_freq", rds_start_date, calcs_start_date
        )

        rm(sf)
        rm(sf_15min)
        rm(cor_15min_sf)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)

# DAILY QUEUE SPILLBACK #######################################################

print(glue("{Sys.time()} Daily Queue Spillback [17 of 29 (sigops 15min)]"))

# HOURLY QUEUE SPILLBACK ######################################################

print(glue("{Sys.time()} 15-Minute Queue Spillback [18 of 29 (sigops 15min)]"))

tryCatch(
    {
        qs <- s3_read_parquet_parallel(
            bucket = conf$bucket,
            table_name = "queue_spillback_15min",
            start_date = rds_start_date,
            end_date = report_end_date,
            signals_list = signals_list
        ) %>%
            mutate(
                SignalID = factor(SignalID),
                CallPhase = factor(CallPhase),
                Date = date(Date))

        qs <- qs %>%
            rename(Timeperiod = Date_Hour) %>%
            select(SignalID, CallPhase, Timeperiod, qs_freq, Date)

        qs_15min <- qs %>%
            complete(
                nesting(SignalID, Date, CallPhase),
                Timeperiod = seq(as_datetime(rds_start_date), as_datetime(report_end_date) - minutes(15), by = "15 min"),
                fill = list(qs_freq = 0)
            ) %>%
            get_period_avg("qs_freq", "Timeperiod")

        cor_15min_qs <- get_cor_monthly_avg_by_period(qs_15min, corridors, "qs_freq", "Timeperiod")

        qs_15min <- sigify(qs_15min, cor_15min_qs, corridors) %>%
                select(Zone_Group, Corridor, Timeperiod, qs_freq, delta)


        addtoRDS(
            qs_15min, "qs_15min.rds", "qs_freq", rds_start_date, rds_start_date
        )
        addtoRDS(
            cor_15min_qs, "cor_15min_qs.rds", "qs_freq", rds_start_date, calcs_start_date
        )

        rm(qs)
        rm(qs_15min)
        rm(cor_15min_qs)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)

# TRAVEL TIME AND BUFFER TIME INDEXES #########################################

print(glue("{Sys.time()} Travel Time Indexes [19 of 29 (sigops 15min)]"))

# CCTV UPTIME From 511 and Encoders

print(glue("{Sys.time()} CCTV Uptimes [20 of 29 (sigops 15min)]"))

# ACTIVITIES ##################################################################

print(glue("{Sys.time()} TEAMS [21 of 29 (sigops 15min)]"))

# USER DELAY COSTS   ##########################################################

print(glue("{Sys.time()} User Delay Costs [22 of 29 (sigops 15min)]"))

# Flash Events ################################################################

print(glue("{Sys.time()} Flash Events [23 of 29 (sigops 15min)]"))

# BIKE/PED SAFETY INDEX #######################################################

print(glue("{Sys.time()} Bike/Ped Safety Index [24 of 29 (sigops 15min)]"))

# RELATIVE SPEED INDEX ########################################################

print(glue("{Sys.time()} Relative Speed Index [25 of 29 (sigops 15min)]"))

# CRASH INDICES ###############################################################

print(glue("{Sys.time()} Crash Indices [26 of 29 (sigops 15min)]"))





# Package up for Flexdashboard

print(glue("{Sys.time()} Package for Monthly Report [27 of 29 (sigops 15min)]"))

tryCatch(
    {
        sig <- list()
        sig$qhr <- list(
            "vph" = readRDS("vol_15min.rds"),
            "paph" = readRDS("pa_15min.rds"),
            "aogh" = readRDS("aog_15min.rds"),
            "prh" = readRDS("pr_15min.rds"),
            "sfh" = readRDS("sf_15min.rds"),
            "qsh" = readRDS("qs_15min.rds")
        )
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)





print(glue("{Sys.time()} Upload to AWS [28 of 29 (sigops 15min)]"))


print(glue("{Sys.time()} Write to Database [29 of 29 (sigops 15min)]"))

source("write_sigops_to_db.R")

# Update Aurora Nightly
aurora <- keep_trying(func = get_aurora_connection, n_tries = 5)
# recreate_database(conn)

tryCatch(
{
    append_to_database(
        aurora, sig, "sig", calcs_start_date, report_start_date = report_start_date, report_end_date = NULL)
},
finally = {
    dbDisconnect(aurora)
})
