
# Monthly_Report_Package.R

source("Monthly_Report_Package_init.R")

print(glue("{Sys.time()} Week Calcs Start Date: {wk_calcs_start_date}"))
print(glue("{Sys.time()} Calcs Start Date: {calcs_start_date}"))
print(glue("{Sys.time()} Report End Date: {report_end_date}"))


# # TRAVEL TIMES FROM RITIS API ###############################################

print(glue("{Sys.time()} travel times [0 of 29 (sigops)]"))

if (conf$run$travel_times == TRUE || is.null(conf$run$travel_times)) {
    # Run python script asynchronously
    system("~/miniconda3/bin/conda run -n sigops python ../get_travel_times_v2.py sigops ../travel_times_1hr.yaml", wait = FALSE)
    system("~/miniconda3/bin/conda run -n sigops python ../get_travel_times_v2.py sigops ../travel_times_15min.yaml", wait = FALSE)
    system("~/miniconda3/bin/conda run -n sigops python ../get_travel_times_1min_v2.py sigops", wait = FALSE)
}

# # DETECTOR UPTIME ###########################################################

print(glue("{Sys.time()} Vehicle Detector Uptime [1 of 29 (sigops)]"))

tryCatch(
    {
        cb <- function(x) {
            get_avg_daily_detector_uptime(x) %>%
                mutate(Date = date(Date))
        }

        avg_daily_detector_uptime <- s3_read_parquet_parallel(
            bucket = conf$bucket,
            table_name = "detector_uptime_pd",
            start_date = wk_calcs_start_date,
            end_date = report_end_date,
            signals_list = signals_list,
            callback = cb
        ) %>%
            mutate(
                SignalID = factor(SignalID)
            )

        cor_avg_daily_detector_uptime <-
            get_cor_avg_daily_detector_uptime(avg_daily_detector_uptime, corridors)
        sub_avg_daily_detector_uptime <-
            (get_cor_avg_daily_detector_uptime(avg_daily_detector_uptime, subcorridors) %>%
                filter(!is.na(Corridor)))

        weekly_detector_uptime <-
            get_weekly_detector_uptime(avg_daily_detector_uptime)
        cor_weekly_detector_uptime <-
            get_cor_weekly_detector_uptime(weekly_detector_uptime, corridors)
        sub_weekly_detector_uptime <-
            (get_cor_weekly_detector_uptime(weekly_detector_uptime, subcorridors) %>%
                filter(!is.na(Corridor)))

        monthly_detector_uptime <-
            get_monthly_detector_uptime(avg_daily_detector_uptime)
        cor_monthly_detector_uptime <-
            get_cor_monthly_detector_uptime(avg_daily_detector_uptime, corridors)
        sub_monthly_detector_uptime <-
            (get_cor_monthly_detector_uptime(avg_daily_detector_uptime, subcorridors) %>%
                filter(!is.na(Corridor)))

        addtoRDS(
            avg_daily_detector_uptime, "avg_daily_detector_uptime.rds", "uptime",
            report_start_date, calcs_start_date
        )
        addtoRDS(
            weekly_detector_uptime, "weekly_detector_uptime.rds", "uptime",
            report_start_date, wk_calcs_start_date
        )
        addtoRDS(
            monthly_detector_uptime, "monthly_detector_uptime.rds", "uptime",
            report_start_date, calcs_start_date
        )

        addtoRDS(
            cor_avg_daily_detector_uptime, "cor_avg_daily_detector_uptime.rds", "uptime",
            report_start_date, calcs_start_date
        )
        addtoRDS(
            cor_weekly_detector_uptime, "cor_weekly_detector_uptime.rds", "uptime",
            report_start_date, wk_calcs_start_date
        )
        addtoRDS(
            cor_monthly_detector_uptime, "cor_monthly_detector_uptime.rds", "uptime",
            report_start_date, calcs_start_date
        )

        addtoRDS(
            sub_avg_daily_detector_uptime, "sub_avg_daily_detector_uptime.rds", "uptime",
            report_start_date, calcs_start_date
        )
        addtoRDS(
            sub_weekly_detector_uptime, "sub_weekly_detector_uptime.rds", "uptime",
            report_start_date, wk_calcs_start_date
        )
        addtoRDS(
            sub_monthly_detector_uptime, "sub_monthly_detector_uptime.rds", "uptime",
            report_start_date, calcs_start_date
        )

        # rm(ddu)
        # rm(daily_detector_uptime)
        rm(avg_daily_detector_uptime)
        rm(weekly_detector_uptime)
        rm(monthly_detector_uptime)
        rm(cor_avg_daily_detector_uptime)
        rm(cor_weekly_detector_uptime)
        rm(cor_monthly_detector_uptime)
        rm(sub_avg_daily_detector_uptime)
        rm(sub_weekly_detector_uptime)
        rm(sub_monthly_detector_uptime)
        # gc()
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)

# DAILY PEDESTRIAN PUSHBUTTON UPTIME ###############################################

print(glue("{Sys.time()} Ped Pushbutton Uptime [2 of 29 (sigops)]"))

tryCatch(
    {
        pau_start_date <- pmin(
            ymd(calcs_start_date),
            floor_date(ymd(report_end_date) - as.duration("6 months"), "month")
        ) %>%
            format("%F")

        paph <- s3_read_parquet_parallel(
            bucket = conf$bucket,
            table_name = "counts_ped_1hr",
            start_date = pau_start_date, # We have to look at a longer duration for pau
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

        papd <- paph %>%
            group_by(SignalID, Date, DOW, Week, Detector, CallPhase) %>%
            summarize(papd = sum(vol, na.rm = TRUE), .groups = "drop")

        paph <- paph %>%
            rename(Hour = Timeperiod, paph = vol)

        dates <- seq(as_date(pau_start_date), as_date(report_end_date), by = "1 day")
        pau <- get_pau_gamma(dates, papd, paph, corridors, wk_calcs_start_date, pau_start_date)

        # Remove and replace papd for bad days, similar to filtered_counts.
        # Replace papd with papd averaged over all days in the date range
        # for that signal and pushbutton input (detector)
        papd <- pau %>%
            mutate(papd = ifelse(uptime == 1, papd, NA)) %>%
            group_by(SignalID, Detector, CallPhase, yr = year(Date), mo = month(Date)) %>%
            mutate(
                papd = ifelse(uptime == 1, papd, floor(mean(papd, na.rm = TRUE)))
            ) %>%
            ungroup() %>%
            select(SignalID, Detector, CallPhase, Date, DOW, Week, papd, uptime, all)

        # We have do to this here rather than in Monthly_Report_Calcs
        # because we need a longer time series to calculate ped detector uptime
        # based on the exponential distribution method (as least 6 months)
        get_bad_ped_detectors(pau) %>%
            filter(Date >= calcs_start_date) %>%

            s3_upload_parquet_date_split(
                bucket = conf$bucket,
                prefix = "bad_ped_detectors",
                table_name = "bad_ped_detectors",
                conf_athena = conf$athena,
                parallel = FALSE
            )

        # Hack to make the aggregation functions work
        addtoRDS(
            pau, "pa_uptime.rds", "uptime", report_start_date, calcs_start_date
        )
        pau <- pau %>%
            mutate(CallPhase = Detector)


        daily_pa_uptime <- get_daily_avg(pau, "uptime", peak_only = FALSE)
        weekly_pa_uptime <- get_weekly_avg_by_day(pau, "uptime", peak_only = FALSE)
        monthly_pa_uptime <- get_monthly_avg_by_day(pau, "uptime", "all", peak_only = FALSE)

        cor_daily_pa_uptime <-
            get_cor_weekly_avg_by_day(daily_pa_uptime, corridors, "uptime")
        sub_daily_pa_uptime <-
            get_cor_weekly_avg_by_day(daily_pa_uptime, subcorridors, "uptime") %>%
            filter(!is.na(Corridor))

        cor_weekly_pa_uptime <-
            get_cor_weekly_avg_by_day(weekly_pa_uptime, corridors, "uptime")
        sub_weekly_pa_uptime <-
            get_cor_weekly_avg_by_day(weekly_pa_uptime, subcorridors, "uptime") %>%
            filter(!is.na(Corridor))

        cor_monthly_pa_uptime <-
            get_cor_monthly_avg_by_day(monthly_pa_uptime, corridors, "uptime")
        sub_monthly_pa_uptime <-
            get_cor_monthly_avg_by_day(monthly_pa_uptime, subcorridors, "uptime") %>%
            filter(!is.na(Corridor))

        addtoRDS(
            daily_pa_uptime, "daily_pa_uptime.rds", "uptime",
            report_start_date, calcs_start_date
        )
        addtoRDS(
            cor_daily_pa_uptime, "cor_daily_pa_uptime.rds", "uptime",
            report_start_date, calcs_start_date
        )
        addtoRDS(
            sub_daily_pa_uptime, "sub_daily_pa_uptime.rds", "uptime",
            report_start_date, calcs_start_date
        )

        addtoRDS(
            weekly_pa_uptime, "weekly_pa_uptime.rds", "uptime",
            report_start_date, wk_calcs_start_date
        )
        addtoRDS(
            cor_weekly_pa_uptime, "cor_weekly_pa_uptime.rds", "uptime",
            report_start_date, wk_calcs_start_date
        )
        addtoRDS(
            sub_weekly_pa_uptime, "sub_weekly_pa_uptime.rds", "uptime",
            report_start_date, wk_calcs_start_date
        )

        addtoRDS(
            monthly_pa_uptime, "monthly_pa_uptime.rds", "uptime",
            report_start_date, calcs_start_date
        )
        addtoRDS(
            cor_monthly_pa_uptime, "cor_monthly_pa_uptime.rds", "uptime",
            report_start_date, calcs_start_date
        )
        addtoRDS(
            sub_monthly_pa_uptime, "sub_monthly_pa_uptime.rds", "uptime",
            report_start_date, calcs_start_date
        )

        # rm(papd)
        # rm(bad_ped_detectors)
        rm(pau)
        rm(daily_pa_uptime)
        rm(weekly_pa_uptime)
        rm(monthly_pa_uptime)
        rm(cor_daily_pa_uptime)
        rm(cor_weekly_pa_uptime)
        rm(cor_monthly_pa_uptime)
        rm(sub_daily_pa_uptime)
        rm(sub_weekly_pa_uptime)
        rm(sub_monthly_pa_uptime)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)


# # WATCHDOG ###########################################################

print(glue("{Sys.time()} watchdog alerts [3 of 29 (sigops)]"))

tryCatch(
    {
        # -- Alerts: detector downtime --

        bad_det <- lapply(
            seq(today() %m-% days(180), today() %m-% days(1), by = "1 day"),
            function(date_) {
                key <- glue("mark/bad_detectors/date={date_}/bad_detectors_{date_}.parquet")
                tryCatch(
                    {
                        s3read_using(read_parquet, bucket = conf$bucket, object = key) %>%
                            select(SignalID, Detector) %>%
                            mutate(Date = date_)
                    },
                    error = function(e) {
                        data.frame()
                    }
                )
            }
        ) %>%
            bind_rows() %>%
            mutate(
                SignalID = factor(SignalID),
                Detector = factor(Detector)
            )

        det_config <- lapply(sort(unique(bad_det$Date)), function(date_) {
            get_det_config(date_) %>%
                transmute(
                    SignalID,
                    CallPhase,
                    Detector,
                    ApproachDesc,
                    LaneNumber,
                    Date = date_
                )
        }) %>%
            bind_rows() %>%
            mutate(
                SignalID = factor(SignalID),
                CallPhase = factor(CallPhase),
                Detector = factor(Detector)
            )

        bad_det <- bad_det %>%
            left_join(
                det_config,
                by = c("SignalID", "Detector", "Date"),
                relationship = "many-to-many"
            ) %>%
            left_join(
                dplyr::select(corridors, Zone_Group, Zone, Corridor, SignalID, Name),
                by = c("SignalID"),
                relationship = "many-to-many"
            ) %>%
            filter(!is.na(Corridor)) %>%
            transmute(
                Zone_Group,
                Zone,
                Corridor,
                SignalID = factor(SignalID),
                CallPhase = factor(CallPhase),
                Detector = factor(Detector),
                Date,
                Alert = factor("Bad Vehicle Detection"),
                Name = factor(if_else(Corridor == "Ramp Meter", sub("@", "-", Name), Name)),
                ApproachDesc = if_else(
		    is.na(ApproachDesc),
		    "",
		    as.character(glue("{trimws(ApproachDesc)} Lane {LaneNumber}")))
            )

        # Zone_Group | Zone | Corridor | SignalID/CameraID | CallPhase | DetectorID | Date | Alert | Name

        s3write_using(
            bad_det,
            FUN = write_parquet,
            object = "sigops/watchdog/bad_detectors.parquet",
            bucket = conf$bucket,
            opts = list(multipart = TRUE)
        )
        rm(bad_det)
        rm(det_config)

        # -- Alerts: pedestrian detector downtime --

        bad_ped <- lapply(
            seq(today() %m-% days(90), today() %m-% days(1), by = "1 day"),
            function(date_) {
                key <- glue("mark/bad_ped_detectors/date={date_}/bad_ped_detectors_{date_}.parquet")
                tryCatch(
                    {
                        s3read_using(read_parquet, bucket = conf$bucket, object = key) %>%
                            mutate(Date = date_)
                    },
                    error = function(e) {
                        data.frame()
                    }
                )
            }
        ) %>%
            bind_rows() %>%
            mutate(
                SignalID = factor(SignalID),
                Detector = factor(Detector)
            ) %>%
            left_join(
                dplyr::select(corridors, Zone_Group, Zone, Corridor, SignalID, Name),
                by = c("SignalID"),
                relationship = "many-to-many"
            ) %>%
            transmute(Zone_Group,
                Zone,
                Corridor = factor(Corridor),
                SignalID = factor(SignalID),
                Detector = factor(Detector),
                Date,
                Alert = factor("Bad Ped Pushbuttons"),
                Name = factor(Name)
            )

        s3write_using(
            bad_ped,
            FUN = write_parquet,
            object = "sigops/watchdog/bad_ped_pushbuttons.parquet",
            bucket = conf$bucket
        )
        rm(bad_ped)

        # -- Alerts: CCTV downtime --

        bad_cam <- lapply(
            seq(floor_date(today() %m-% months(6), "month"), today() %m-% days(1), by = "1 month"),
            function(date_) {
                key <- glue("mark/cctv_uptime/month={date_}/cctv_uptime_{date_}.parquet")
                tryCatch(
                    {
                        s3read_using(read_parquet, bucket = conf$bucket, object = key) %>%
                            filter(Size == 0)
                    },
                    error = function(e) {
                        print(e)
                        data.frame()
                    }
                )
            }
        ) %>%
            bind_rows() %>%
            left_join(cam_config, by = c("CameraID"), relationship = "many-to-many") %>%
            filter(Date > As_of_Date) %>%
            transmute(
                Zone_Group,
                Zone,
                Corridor = factor(Corridor),
                SignalID = factor(CameraID),
                CallPhase = factor(0),
                Detector = factor(0),
                Date, Alert = factor("No Camera Image"),
                Name = factor(Location)
            )

        s3write_using(
            bad_cam,
            FUN = write_parquet,
            object = "sigops/watchdog/bad_cameras.parquet",
            bucket = conf$bucket
        )
        rm(bad_cam)

        # -- Watchdog Alerts --

        # Nothing to do here


        # -- --------------- --

        # Zone_Group | Zone | Corridor | SignalID/CameraID | CallPhase | DetectorID | Date | Alert | Name
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)




# DAILY PEDESTRIAN ACTIVATIONS ################################################

print(glue("{Sys.time()} Daily Pedestrian Activations [4 of 29 (sigops)]"))

tryCatch(
    {
        daily_papd <- get_daily_sum(papd, "papd")
        weekly_papd <- get_weekly_papd(papd)
        monthly_papd <- get_monthly_papd(papd)

        # Group into corridors --------------------------------------------------------
        cor_daily_papd <- get_cor_weekly_papd(daily_papd, corridors) %>% select(-Week)
        cor_weekly_papd <- get_cor_weekly_papd(weekly_papd, corridors)
        cor_monthly_papd <- get_cor_monthly_papd(monthly_papd, corridors)

        # Group into subcorridors --------------------------------------------------------
        sub_daily_papd <- get_cor_weekly_papd(daily_papd, subcorridors) %>% select(-Week) %>%
            filter(!is.na(Corridor))
        sub_weekly_papd <- get_cor_weekly_papd(weekly_papd, subcorridors) %>%
            filter(!is.na(Corridor))
        sub_monthly_papd <- get_cor_monthly_papd(monthly_papd, subcorridors) %>%
            filter(!is.na(Corridor))


        addtoRDS(daily_papd, "daily_papd.rds", "papd", report_start_date, wk_calcs_start_date)
        addtoRDS(weekly_papd, "weekly_papd.rds", "papd", report_start_date, wk_calcs_start_date)
        addtoRDS(monthly_papd, "monthly_papd.rds", "papd", report_start_date, calcs_start_date)
        addtoRDS(cor_daily_papd, "cor_daily_papd.rds", "papd", report_start_date, wk_calcs_start_date)
        addtoRDS(cor_weekly_papd, "cor_weekly_papd.rds", "papd", report_start_date, wk_calcs_start_date)
        addtoRDS(cor_monthly_papd, "cor_monthly_papd.rds", "papd", report_start_date, calcs_start_date)
        addtoRDS(sub_daily_papd, "sub_daily_papd.rds", "papd", report_start_date, wk_calcs_start_date)
        addtoRDS(sub_weekly_papd, "sub_weekly_papd.rds", "papd", report_start_date, wk_calcs_start_date)
        addtoRDS(sub_monthly_papd, "sub_monthly_papd.rds", "papd", report_start_date, calcs_start_date)

        rm(papd)
        rm(daily_papd)
        rm(weekly_papd)
        rm(monthly_papd)
        rm(cor_daily_papd)
        rm(cor_weekly_papd)
        rm(cor_monthly_papd)
        rm(sub_daily_papd)
        rm(sub_weekly_papd)
        rm(sub_monthly_papd)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)



# HOURLY PEDESTRIAN ACTIVATIONS ###############################################

print(glue("{Sys.time()} Hourly Pedestrian Activations [5 of 29 (sigops)]"))

if (FALSE) {
tryCatch(
    {
        paph <- paph %>%
            filter(Hour >= wk_calcs_start_date) %>%
            mutate(CallPhase = 0)
        qsave(paph, "paph.qs")

        weekly_paph <- get_weekly_paph(paph)
        monthly_paph <- get_monthly_paph(paph)

        # Group into corridors --------------------------------------------------------
        cor_weekly_paph <- get_cor_weekly_paph(weekly_paph, corridors)
        sub_weekly_paph <- get_cor_weekly_paph(weekly_paph, subcorridors) %>%
            filter(!is.na(Corridor))

        # Hourly volumes by Corridor --------------------------------------------------
        cor_monthly_paph <- get_cor_monthly_paph(monthly_paph, corridors)
        sub_monthly_paph <- get_cor_monthly_paph(monthly_paph, subcorridors) %>%
            filter(!is.na(Corridor))

        addtoRDS(weekly_paph, "weekly_paph.rds", "paph", report_start_date, wk_calcs_start_date)
        addtoRDS(monthly_paph, "monthly_paph.rds", "paph", report_start_date, calcs_start_date)
        addtoRDS(cor_weekly_paph, "cor_weekly_paph.rds", "paph", report_start_date, wk_calcs_start_date)
        addtoRDS(cor_monthly_paph, "cor_monthly_paph.rds", "paph", report_start_date, calcs_start_date)
        addtoRDS(sub_weekly_paph, "sub_weekly_paph.rds", "paph", report_start_date, wk_calcs_start_date)
        addtoRDS(sub_monthly_paph, "sub_monthly_paph.rds", "paph", report_start_date, calcs_start_date)

        rm(paph)
        rm(weekly_paph)
        rm(monthly_paph)
        rm(cor_weekly_paph)
        rm(cor_monthly_paph)
        rm(sub_weekly_paph)
        rm(sub_monthly_paph)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)
}


# GET PEDESTRIAN DELAY ###################################################

print(glue("{Sys.time()} Pedestrian Delay [6 of 29 (sigops)]"))

tryCatch(
    {
        cb <- function(x) {
            if ("Avg.Max.Ped.Delay" %in% names(x)) {
                x <- x %>%
                    rename(pd = Avg.Max.Ped.Delay) %>%
                    mutate(CallPhase = factor(0))
            }
            x %>%
                mutate(
                    DOW = wday(Date),
                    Week = week(Date)
                )
        }

        ped_delay <- s3_read_parquet_parallel(
            bucket = conf$bucket,
            table_name = "ped_delay",
            start_date = wk_calcs_start_date,
            end_date = report_end_date,
            signals_list = signals_list,
            callback = cb
        ) %>%
            mutate(
                SignalID = factor(SignalID),
                CallPhase = factor(CallPhase)
            ) %>%
            replace_na(list(Events = 1))


        daily_pd <- get_daily_avg(ped_delay, "pd", "Events")
        weekly_pd_by_day <- get_weekly_avg_by_day(ped_delay, "pd", "Events", peak_only = FALSE)
        monthly_pd_by_day <- get_monthly_avg_by_day(ped_delay, "pd", "Events", peak_only = FALSE)

        cor_daily_pd <- get_cor_weekly_avg_by_day(daily_pd, corridors, "pd", "Events") %>% select(-Week)
        cor_weekly_pd_by_day <- get_cor_weekly_avg_by_day(weekly_pd_by_day, corridors, "pd", "Events")
        cor_monthly_pd_by_day <- get_cor_monthly_avg_by_day(monthly_pd_by_day, corridors, "pd", "Events")

        sub_daily_pd <- get_cor_weekly_avg_by_day(daily_pd, subcorridors, "pd", "Events") %>% select(-Week) %>%
            filter(!is.na(Corridor))
        sub_weekly_pd_by_day <- get_cor_weekly_avg_by_day(weekly_pd_by_day, subcorridors, "pd", "Events") %>%
            filter(!is.na(Corridor))
        sub_monthly_pd_by_day <- get_cor_monthly_avg_by_day(monthly_pd_by_day, subcorridors, "pd", "Events") %>%
            filter(!is.na(Corridor))

        addtoRDS(daily_pd, "daily_pd.rds", "pd", report_start_date, wk_calcs_start_date)
        addtoRDS(weekly_pd_by_day, "weekly_pd_by_day.rds", "pd", report_start_date, wk_calcs_start_date)
        addtoRDS(monthly_pd_by_day, "monthly_pd_by_day.rds", "pd", report_start_date, calcs_start_date)
        addtoRDS(cor_daily_pd, "cor_daily_pd.rds", "pd", report_start_date, wk_calcs_start_date)
        addtoRDS(cor_weekly_pd_by_day, "cor_weekly_pd_by_day.rds", "pd", report_start_date, wk_calcs_start_date)
        addtoRDS(cor_monthly_pd_by_day, "cor_monthly_pd_by_day.rds", "pd", report_start_date, calcs_start_date)
        addtoRDS(sub_daily_pd, "sub_daily_pd.rds", "pd", report_start_date, wk_calcs_start_date)
        addtoRDS(sub_weekly_pd_by_day, "sub_weekly_pd_by_day.rds", "pd", report_start_date, wk_calcs_start_date)
        addtoRDS(sub_monthly_pd_by_day, "sub_monthly_pd_by_day.rds", "pd", report_start_date, calcs_start_date)

        rm(ped_delay)
        rm(daily_pd)
        rm(weekly_pd_by_day)
        rm(monthly_pd_by_day)
        rm(cor_daily_pd)
        rm(cor_weekly_pd_by_day)
        rm(cor_monthly_pd_by_day)
        rm(sub_daily_pd)
        rm(sub_weekly_pd_by_day)
        rm(sub_monthly_pd_by_day)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)


# GET COMMUNICATIONS UPTIME ###################################################

print(glue("{Sys.time()} Communication Uptime [7 of 29 (sigops)]"))

tryCatch(
    {
        cu <- s3_read_parquet_parallel(
            bucket = conf$bucket,
            table_name = "comm_uptime",
            start_date = wk_calcs_start_date,
            end_date = report_end_date,
            signals_list = signals_list
        ) %>%
            mutate(
                SignalID = factor(SignalID),
                CallPhase = factor(CallPhase),
                Date = date(Date)
            )

        daily_comm_uptime <- get_daily_avg(cu, "uptime", peak_only = FALSE)
        cor_daily_comm_uptime <-
            get_cor_weekly_avg_by_day(daily_comm_uptime, corridors, "uptime")
        sub_daily_comm_uptime <-
            (get_cor_weekly_avg_by_day(daily_comm_uptime, subcorridors, "uptime") %>%
                filter(!is.na(Corridor)))

        weekly_comm_uptime <- get_weekly_avg_by_day(cu, "uptime", peak_only = FALSE)
        cor_weekly_comm_uptime <-
            get_cor_weekly_avg_by_day(weekly_comm_uptime, corridors, "uptime")
        sub_weekly_comm_uptime <-
            (get_cor_weekly_avg_by_day(weekly_comm_uptime, subcorridors, "uptime") %>%
                filter(!is.na(Corridor)))

        monthly_comm_uptime <- get_monthly_avg_by_day(cu, "uptime", peak_only = FALSE)
        cor_monthly_comm_uptime <-
            get_cor_monthly_avg_by_day(monthly_comm_uptime, corridors, "uptime")
        sub_monthly_comm_uptime <-
            (get_cor_monthly_avg_by_day(monthly_comm_uptime, subcorridors, "uptime") %>%
                filter(!is.na(Corridor)))


        addtoRDS(daily_comm_uptime, "daily_comm_uptime.rds", "uptime", report_start_date, calcs_start_date)
        addtoRDS(cor_daily_comm_uptime, "cor_daily_comm_uptime.rds", "uptime", report_start_date, calcs_start_date)
        addtoRDS(sub_daily_comm_uptime, "sub_daily_comm_uptime.rds", "uptime", report_start_date, calcs_start_date)

        addtoRDS(weekly_comm_uptime, "weekly_comm_uptime.rds", "uptime", report_start_date, wk_calcs_start_date)
        addtoRDS(cor_weekly_comm_uptime, "cor_weekly_comm_uptime.rds", "uptime", report_start_date, wk_calcs_start_date)
        addtoRDS(sub_weekly_comm_uptime, "sub_weekly_comm_uptime.rds", "uptime", report_start_date, wk_calcs_start_date)

        addtoRDS(monthly_comm_uptime, "monthly_comm_uptime.rds", "uptime", report_start_date, calcs_start_date)
        addtoRDS(cor_monthly_comm_uptime, "cor_monthly_comm_uptime.rds", "uptime", report_start_date, calcs_start_date)
        addtoRDS(sub_monthly_comm_uptime, "sub_monthly_comm_uptime.rds", "uptime", report_start_date, calcs_start_date)

        rm(cu)
        rm(daily_comm_uptime)
        rm(weekly_comm_uptime)
        rm(monthly_comm_uptime)
        rm(cor_daily_comm_uptime)
        rm(cor_weekly_comm_uptime)
        rm(cor_monthly_comm_uptime)
        rm(sub_daily_comm_uptime)
        rm(sub_weekly_comm_uptime)
        rm(sub_monthly_comm_uptime)
        # gc()
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)



# DAILY VOLUMES ###############################################################

print(glue("{Sys.time()} Daily Volumes [8 of 29 (sigops)]"))

tryCatch(
    {
        vpd <- s3_read_parquet_parallel(
            bucket = conf$bucket,
            table_name = "vehicles_pd",
            start_date = wk_calcs_start_date,
            end_date = report_end_date,
            signals_list = signals_list
        ) %>%
            mutate(
                SignalID = factor(SignalID),
                CallPhase = factor(CallPhase),
                Date = date(Date)
            )

        daily_vpd <- get_daily_sum(vpd, "vpd")
        weekly_vpd <- get_weekly_vpd(vpd)
        monthly_vpd <- get_monthly_vpd(vpd)

        # Group into corridors --------------------------------------------------------
        cor_daily_vpd <- get_cor_weekly_vpd(daily_vpd, corridors) %>% select(-ones, -Week)
        cor_weekly_vpd %<-% get_cor_weekly_vpd(weekly_vpd, corridors)
        cor_monthly_vpd %<-% get_cor_monthly_vpd(monthly_vpd, corridors)

        # Subcorridors
        sub_daily_vpd <- get_cor_weekly_vpd(daily_vpd, subcorridors) %>% select(-ones, -Week) %>%
            filter(!is.na(Corridor))
        sub_weekly_vpd <-
            get_cor_weekly_vpd(weekly_vpd, subcorridors) %>%
                filter(!is.na(Corridor))
        sub_monthly_vpd <-
            get_cor_monthly_vpd(monthly_vpd, subcorridors) %>%
                filter(!is.na(Corridor))

        # Monthly % change from previous month by corridor ----------------------------
        addtoRDS(daily_vpd, "daily_vpd.rds", "vpd", report_start_date, wk_calcs_start_date)
        addtoRDS(weekly_vpd, "weekly_vpd.rds", "vpd", report_start_date, wk_calcs_start_date)
        addtoRDS(monthly_vpd, "monthly_vpd.rds", "vpd", report_start_date, calcs_start_date)
        addtoRDS(cor_daily_vpd, "cor_daily_vpd.rds", "vpd", report_start_date, wk_calcs_start_date)
        addtoRDS(cor_weekly_vpd, "cor_weekly_vpd.rds", "vpd", report_start_date, wk_calcs_start_date)
        addtoRDS(cor_monthly_vpd, "cor_monthly_vpd.rds", "vpd", report_start_date, calcs_start_date)
        addtoRDS(sub_daily_vpd, "sub_daily_vpd.rds", "vpd", report_start_date, wk_calcs_start_date)
        addtoRDS(sub_weekly_vpd, "sub_weekly_vpd.rds", "vpd", report_start_date, wk_calcs_start_date)
        addtoRDS(sub_monthly_vpd, "sub_monthly_vpd.rds", "vpd", report_start_date, calcs_start_date)

        rm(vpd)
        rm(daily_vpd)
        rm(weekly_vpd)
        rm(monthly_vpd)
        rm(cor_daily_vpd)
        rm(cor_weekly_vpd)
        rm(cor_monthly_vpd)
        rm(sub_daily_vpd)
        rm(sub_weekly_vpd)
        rm(sub_monthly_vpd)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)



# HOURLY VOLUMES ##############################################################

print(glue("{Sys.time()} Hourly Volumes [9 of 29 (sigops)]"))

tryCatch(
    {
        vph <- s3_read_parquet_parallel(
            bucket = conf$bucket,
            table_name = "vehicles_ph",
            start_date = wk_calcs_start_date,
            end_date = report_end_date,
            signals_list = signals_list
        ) %>%
            mutate(
                SignalID = factor(SignalID),
                CallPhase = factor(2), # Hack because next function needs a CallPhase
                Date = date(Date)
            )

    	hourly_vol <- get_hourly(vph, "vph", corridors)

    	cor_daily_vph <- get_cor_weekly_vph(hourly_vol, corridors)
    	sub_daily_vph <- get_cor_weekly_vph(hourly_vol, subcorridors) %>%
    	    filter(!is.na(Corridor))

    	daily_vph_peak <- get_weekly_vph_peak(hourly_vol)
    	cor_daily_vph_peak <- get_cor_weekly_vph_peak(cor_daily_vph)
    	sub_daily_vph_peak <- get_cor_weekly_vph_peak(sub_daily_vph) %>%
    	    map(~ filter(., !is.na(Corridor)))

        weekly_vph <- get_weekly_vph(vph)
        cor_weekly_vph <- get_cor_weekly_vph(weekly_vph, corridors)
        sub_weekly_vph <- get_cor_weekly_vph(weekly_vph, subcorridors) %>%
            filter(!is.na(Corridor))

        weekly_vph_peak <- get_weekly_vph_peak(weekly_vph)
        cor_weekly_vph_peak <- get_cor_weekly_vph_peak(cor_weekly_vph)
        sub_weekly_vph_peak <- get_cor_weekly_vph_peak(sub_weekly_vph) %>%
            map(~ filter(., !is.na(Corridor)))

        monthly_vph <- get_monthly_vph(vph)
        cor_monthly_vph <- get_cor_monthly_vph(monthly_vph, corridors)
        sub_monthly_vph <- get_cor_monthly_vph(monthly_vph, subcorridors) %>%
            filter(!is.na(Corridor))

        monthly_vph_peak <- get_monthly_vph_peak(monthly_vph)
        cor_monthly_vph_peak <- get_cor_monthly_vph_peak(cor_monthly_vph)
        sub_monthly_vph_peak <- get_cor_monthly_vph_peak(sub_monthly_vph) %>%
            map(~ filter(., !is.na(Corridor)))


        addtoRDS(weekly_vph, "weekly_vph.rds", "vph", report_start_date, wk_calcs_start_date)
        addtoRDS(monthly_vph, "monthly_vph.rds", "vph", report_start_date, calcs_start_date)
        addtoRDS(cor_daily_vph, "cor_daily_vph.rds", "vph", report_start_date, wk_calcs_start_date)
        addtoRDS(cor_weekly_vph, "cor_weekly_vph.rds", "vph", report_start_date, wk_calcs_start_date)
        addtoRDS(cor_monthly_vph, "cor_monthly_vph.rds", "vph", report_start_date, calcs_start_date)
        addtoRDS(sub_daily_vph, "sub_daily_vph.rds", "vph", report_start_date, wk_calcs_start_date)
        addtoRDS(sub_weekly_vph, "sub_weekly_vph.rds", "vph", report_start_date, wk_calcs_start_date)
        addtoRDS(sub_monthly_vph, "sub_monthly_vph.rds", "vph", report_start_date, calcs_start_date)

        addtoRDS(daily_vph_peak, "daily_vph_peak.rds", "vph", report_start_date, wk_calcs_start_date)
        addtoRDS(weekly_vph_peak, "weekly_vph_peak.rds", "vph", report_start_date, wk_calcs_start_date)
        addtoRDS(monthly_vph_peak, "monthly_vph_peak.rds", "vph", report_start_date, calcs_start_date)
        addtoRDS(cor_daily_vph_peak, "cor_daily_vph_peak.rds", "vph", report_start_date, wk_calcs_start_date)
        addtoRDS(cor_weekly_vph_peak, "cor_weekly_vph_peak.rds", "vph", report_start_date, wk_calcs_start_date)
        addtoRDS(cor_monthly_vph_peak, "cor_monthly_vph_peak.rds", "vph", report_start_date, calcs_start_date)
        addtoRDS(sub_daily_vph_peak, "sub_daily_vph_peak.rds", "vph", report_start_date, wk_calcs_start_date)
        addtoRDS(sub_weekly_vph_peak, "sub_weekly_vph_peak.rds", "vph", report_start_date, wk_calcs_start_date)
        addtoRDS(sub_monthly_vph_peak, "sub_monthly_vph_peak.rds", "vph", report_start_date, calcs_start_date)

        rm(vph)
        rm(hourly_vol)
        rm(cor_daily_vph)
        rm(sub_daily_vph)

        rm(weekly_vph)
        rm(cor_weekly_vph)
        rm(sub_weekly_vph)

        rm(monthly_vph)
        rm(cor_monthly_vph)
        rm(sub_monthly_vph)

        rm(daily_vph_peak)
        rm(cor_daily_vph_peak)
        rm(sub_daily_vph_peak)

        rm(weekly_vph_peak)
        rm(cor_weekly_vph_peak)
        rm(sub_weekly_vph_peak)

        rm(monthly_vph_peak)
        rm(cor_monthly_vph_peak)
        rm(sub_monthly_vph_peak)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)






# DAILY THROUGHPUT ############################################################

print(glue("{Sys.time()} Daily Throughput [10 of 29 (sigops)]"))

tryCatch(
    {
        throughput <- s3_read_parquet_parallel(
            bucket = conf$bucket,
            table_name = "throughput",
            start_date = wk_calcs_start_date,
            end_date = report_end_date,
            signals_list = signals_list
        ) %>%
            mutate(
                SignalID = factor(SignalID),
                CallPhase = factor(as.integer(CallPhase)),
                Date = date(Date)
            )

        # daily_throughput <- readRDS("../production_scripts/daily_throughput.rds") %>%
        #     filter(Date >= calcs_start_date)
        daily_throughput <- get_daily_sum(throughput, "vph")
        weekly_throughput %<-% get_weekly_thruput(throughput)
        monthly_throughput %<-% get_monthly_thruput(throughput)

        # Daily throughput
        cor_daily_throughput <- get_cor_weekly_thruput(daily_throughput, corridors) %>% select(-Week)
        sub_daily_throughput <- get_cor_weekly_thruput(daily_throughput, subcorridors) %>% select(-Week) %>%
            filter(!is.na(Corridor))

        # Weekly throughput - Group into corridors ---------------------------------
        cor_weekly_throughput <- get_cor_weekly_thruput(weekly_throughput, corridors)
        sub_weekly_throughput <- get_cor_weekly_thruput(weekly_throughput, subcorridors) %>%
            filter(!is.na(Corridor))

        # Monthly throughput - Group into corridors
        cor_monthly_throughput <- get_cor_monthly_thruput(monthly_throughput, corridors)
        sub_monthly_throughput <- get_cor_monthly_thruput(monthly_throughput, subcorridors) %>%
            filter(!is.na(Corridor))

        addtoRDS(daily_throughput, "daily_throughput.rds", "vph", report_start_date, wk_calcs_start_date)
        addtoRDS(weekly_throughput, "weekly_throughput.rds", "vph", report_start_date, wk_calcs_start_date)
        addtoRDS(monthly_throughput, "monthly_throughput.rds", "vph", report_start_date, calcs_start_date)
        addtoRDS(cor_daily_throughput, "cor_daily_throughput.rds", "vph", report_start_date, calcs_start_date)
        addtoRDS(cor_weekly_throughput, "cor_weekly_throughput.rds", "vph", report_start_date, wk_calcs_start_date)
        addtoRDS(cor_monthly_throughput, "cor_monthly_throughput.rds", "vph", report_start_date, calcs_start_date)
        addtoRDS(sub_daily_throughput, "sub_daily_throughput.rds", "vph", report_start_date, calcs_start_date)
        addtoRDS(sub_weekly_throughput, "sub_weekly_throughput.rds", "vph", report_start_date, wk_calcs_start_date)
        addtoRDS(sub_monthly_throughput, "sub_monthly_throughput.rds", "vph", report_start_date, calcs_start_date)

        rm(throughput)
        rm(daily_throughput)
        rm(weekly_throughput)
        rm(monthly_throughput)
        rm(cor_daily_throughput)
        rm(cor_weekly_throughput)
        rm(cor_monthly_throughput)
        rm(sub_daily_throughput)
        rm(sub_weekly_throughput)
        rm(sub_monthly_throughput)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)




# DAILY ARRIVALS ON GREEN #####################################################

print(glue("{Sys.time()} Daily AOG [11 of 29 (sigops)]"))

tryCatch(
    {
        aog <- s3_read_parquet_parallel(
            bucket = conf$bucket,
            table_name = "arrivals_on_green",
            start_date = wk_calcs_start_date,
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

        # Alternative design pattern. Uses Arrow datasets on S3.
        # Less memory. Multiple cores. Seems far better all around.
        # Need to assess the stability of Arrow datasets.
        #
        # ds <- arrow::open_dataset(glue("s3://{conf$bucket}/mark/arrivals_on_green"))
        # date_range <- seq(wk_calcs_start_date, as_date(report_end_date), by = "1 day") %>%
        #     as.character()
        # aog <- mclapply(date_range, mc.cores = usable_cores, FUN = function(date_) {
        #     ds %>%
        #         filter(date == date_, SignalID %in% signals_list) %>%
        #         collect() %>%
        #         mutate(
        #             SignalID = factor(SignalID),
        #             CallPhase = factor(CallPhase),
        #             Date = date(date),
        #             DOW = wday(date),
        #             Week = week(date)
        #         ) %>%
        #         get_daily_aog()
        # }) %>% bind_rows()
        #

        daily_aog <- get_daily_aog(aog)
        weekly_aog_by_day <- get_weekly_aog_by_day(aog)
        monthly_aog_by_day <- get_monthly_aog_by_day(aog)

        cor_daily_aog <- get_cor_weekly_aog_by_day(daily_aog, corridors) %>% select(-Week)
        cor_weekly_aog_by_day <- get_cor_weekly_aog_by_day(weekly_aog_by_day, corridors)
        cor_monthly_aog_by_day <- get_cor_monthly_aog_by_day(monthly_aog_by_day, corridors)

        sub_daily_aog <- get_cor_weekly_aog_by_day(daily_aog, subcorridors) %>% select(-Week) %>%
            filter(!is.na(Corridor))
        sub_weekly_aog_by_day <- get_cor_weekly_aog_by_day(weekly_aog_by_day, subcorridors) %>%
            filter(!is.na(Corridor))
        sub_monthly_aog_by_day <- get_cor_monthly_aog_by_day(monthly_aog_by_day, subcorridors) %>%
            filter(!is.na(Corridor))

        addtoRDS(daily_aog, "daily_aog.rds", "aog", report_start_date, wk_calcs_start_date)
        addtoRDS(weekly_aog_by_day, "weekly_aog_by_day.rds", "aog", report_start_date, wk_calcs_start_date)
        addtoRDS(monthly_aog_by_day, "monthly_aog_by_day.rds", "aog", report_start_date, calcs_start_date)
        addtoRDS(cor_daily_aog, "cor_daily_aog.rds", "aog", report_start_date, calcs_start_date)
        addtoRDS(cor_weekly_aog_by_day, "cor_weekly_aog_by_day.rds", "aog", report_start_date, wk_calcs_start_date)
        addtoRDS(cor_monthly_aog_by_day, "cor_monthly_aog_by_day.rds", "aog", report_start_date, calcs_start_date)
        addtoRDS(sub_daily_aog, "sub_daily_aog.rds", "aog", report_start_date, calcs_start_date)
        addtoRDS(sub_weekly_aog_by_day, "sub_weekly_aog_by_day.rds", "aog", report_start_date, wk_calcs_start_date)
        addtoRDS(sub_monthly_aog_by_day, "sub_monthly_aog_by_day.rds", "aog", report_start_date, calcs_start_date)

        rm(daily_aog)
        rm(weekly_aog_by_day)
        rm(monthly_aog_by_day)
        rm(cor_weekly_aog_by_day)
        rm(cor_monthly_aog_by_day)
        rm(sub_weekly_aog_by_day)
        rm(sub_monthly_aog_by_day)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)



# HOURLY ARRIVALS ON GREEN ####################################################

print(glue("{Sys.time()} Hourly AOG [12 of 29 (sigops)]"))

tryCatch(
    {
        aog_by_hr <- get_aog_by_hr(aog)
        monthly_aog_by_hr <- get_monthly_aog_by_hr(aog_by_hr)

        # Hourly volumes by Corridor --------------------------------------------------
        cor_monthly_aog_by_hr <- get_cor_monthly_aog_by_hr(monthly_aog_by_hr, corridors)
        sub_monthly_aog_by_hr <- get_cor_monthly_aog_by_hr(monthly_aog_by_hr, subcorridors) %>%
            filter(!is.na(Corridor))

        # cor_monthly_aog_peak <- get_cor_monthly_aog_peak(cor_monthly_aog_by_hr)

        addtoRDS(monthly_aog_by_hr, "monthly_aog_by_hr.rds", "aog", report_start_date, calcs_start_date)
        addtoRDS(cor_monthly_aog_by_hr, "cor_monthly_aog_by_hr.rds", "aog", report_start_date, calcs_start_date)
        addtoRDS(sub_monthly_aog_by_hr, "sub_monthly_aog_by_hr.rds", "aog", report_start_date, calcs_start_date)

        rm(aog_by_hr)
        # rm(cor_monthly_aog_peak)
        rm(monthly_aog_by_hr)
        rm(cor_monthly_aog_by_hr)
        rm(sub_monthly_aog_by_hr)
        # gc()
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)



# DAILY PROGRESSION RATIO #####################################################

print(glue("{Sys.time()} Daily Progression Ratio [13 of 29 (sigops)]"))

tryCatch(
    {
        daily_pr <- get_daily_avg(aog, "pr", "vol")
        weekly_pr_by_day <- get_weekly_pr_by_day(aog)
        monthly_pr_by_day <- get_monthly_pr_by_day(aog)

        cor_daily_pr <- get_cor_weekly_pr_by_day(daily_pr, corridors) %>% select(-Week)
        cor_weekly_pr_by_day <- get_cor_weekly_pr_by_day(weekly_pr_by_day, corridors)
        cor_monthly_pr_by_day <- get_cor_monthly_pr_by_day(monthly_pr_by_day, corridors)

        sub_daily_pr <- get_cor_weekly_pr_by_day(daily_pr, subcorridors) %>% select(-Week) %>%
            filter(!is.na(Corridor))
        sub_weekly_pr_by_day <- get_cor_weekly_pr_by_day(weekly_pr_by_day, subcorridors) %>%
            filter(!is.na(Corridor))
        sub_monthly_pr_by_day <- get_cor_monthly_pr_by_day(monthly_pr_by_day, subcorridors) %>%
            filter(!is.na(Corridor))

        addtoRDS(daily_pr, "daily_pr.rds", "pr", report_start_date, wk_calcs_start_date)
        addtoRDS(weekly_pr_by_day, "weekly_pr_by_day.rds", "pr", report_start_date, wk_calcs_start_date)
        addtoRDS(monthly_pr_by_day, "monthly_pr_by_day.rds", "pr", report_start_date, calcs_start_date)
        addtoRDS(cor_daily_pr, "cor_daily_pr.rds", "pr", report_start_date, wk_calcs_start_date)
        addtoRDS(cor_weekly_pr_by_day, "cor_weekly_pr_by_day.rds", "pr", report_start_date, wk_calcs_start_date)
        addtoRDS(cor_monthly_pr_by_day, "cor_monthly_pr_by_day.rds", "pr", report_start_date, calcs_start_date)
        addtoRDS(sub_daily_pr, "sub_daily_pr.rds", "pr", report_start_date, wk_calcs_start_date)
        addtoRDS(sub_weekly_pr_by_day, "sub_weekly_pr_by_day.rds", "pr", report_start_date, wk_calcs_start_date)
        addtoRDS(sub_monthly_pr_by_day, "sub_monthly_pr_by_day.rds", "pr", report_start_date, calcs_start_date)

        rm(daily_pr)
        rm(weekly_pr_by_day)
        rm(monthly_pr_by_day)
        rm(cor_daily_pr)
        rm(cor_weekly_pr_by_day)
        rm(cor_monthly_pr_by_day)
        rm(sub_daily_pr)
        rm(sub_weekly_pr_by_day)
        rm(sub_monthly_pr_by_day)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)



# HOURLY PROGESSION RATIO ####################################################

print(glue("{Sys.time()} Hourly Progression Ratio [14 of 29 (sigops)]"))

tryCatch(
    {
        pr_by_hr <- get_pr_by_hr(aog)
        monthly_pr_by_hr <- get_monthly_pr_by_hr(pr_by_hr)

        # Hourly volumes by Corridor --------------------------------------------------
        cor_monthly_pr_by_hr <- get_cor_monthly_pr_by_hr(monthly_pr_by_hr, corridors)
        sub_monthly_pr_by_hr <- get_cor_monthly_pr_by_hr(monthly_pr_by_hr, subcorridors) %>%
            filter(!is.na(Corridor))

        # cor_monthly_pr_peak <- get_cor_monthly_pr_peak(cor_monthly_pr_by_hr)

        addtoRDS(monthly_pr_by_hr, "monthly_pr_by_hr.rds", "pr", report_start_date, calcs_start_date)
        addtoRDS(cor_monthly_pr_by_hr, "cor_monthly_pr_by_hr.rds", "pr", report_start_date, calcs_start_date)
        addtoRDS(sub_monthly_pr_by_hr, "sub_monthly_pr_by_hr.rds", "pr", report_start_date, calcs_start_date)

        rm(aog)
        rm(pr_by_hr)
        rm(monthly_pr_by_hr)
        rm(cor_monthly_pr_by_hr)
        rm(sub_monthly_pr_by_hr)
        # gc()
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)





# DAILY SPLIT FAILURES #####################################################

tryCatch(
    {
        print(glue("{Sys.time()} Daily Split Failures [15 of 29 (sigops)]"))

        sf <- s3_read_parquet_parallel(
            bucket = conf$bucket,
            table_name = "split_failures",
            start_date = wk_calcs_start_date,
            end_date = report_end_date,
            signals_list = signals_list,
            callback = function(x) filter(x, CallPhase == 0)
        ) %>%
            mutate(
                SignalID = factor(SignalID),
                CallPhase = factor(CallPhase),
                Date = date(Date)
            )

        # Divide into peak/off-peak split failures
        # -------------------------------------------------------------------------
        sfo <- sf %>% filter(!hour(Date_Hour) %in% c(AM_PEAK_HOURS, PM_PEAK_HOURS))
        sfp <- sf %>% filter(hour(Date_Hour) %in% c(AM_PEAK_HOURS, PM_PEAK_HOURS))
        # -------------------------------------------------------------------------

        daily_sfp <- get_daily_avg(sfp, "sf_freq", "cycles")
        daily_sfo <- get_daily_avg(sfo, "sf_freq", "cycles")

        weekly_sf_by_day <- get_weekly_avg_by_day(
            sfp, "sf_freq", "cycles",
            peak_only = FALSE
        )
        weekly_sfo_by_day <- get_weekly_avg_by_day(
            sfo, "sf_freq", "cycles",
            peak_only = FALSE
        )
        monthly_sf_by_day <- get_monthly_avg_by_day(
            sfp, "sf_freq", "cycles",
            peak_only = FALSE
        )
        monthly_sfo_by_day <- get_monthly_avg_by_day(
            sfo, "sf_freq", "cycles",
            peak_only = FALSE
        )

        cor_daily_sfp <- get_cor_weekly_sf_by_day(daily_sfp, corridors) %>% select(-Week)
        cor_daily_sfo <- get_cor_weekly_sf_by_day(daily_sfo, corridors) %>% select(-Week)
        cor_weekly_sf_by_day <- get_cor_weekly_sf_by_day(weekly_sf_by_day, corridors)
        cor_weekly_sfo_by_day <- get_cor_weekly_sf_by_day(weekly_sfo_by_day, corridors)
        cor_monthly_sf_by_day <- get_cor_monthly_sf_by_day(monthly_sf_by_day, corridors)
        cor_monthly_sfo_by_day <- get_cor_monthly_sf_by_day(monthly_sfo_by_day, corridors)

        sub_daily_sfp <- get_cor_weekly_sf_by_day(daily_sfp, subcorridors) %>%
            filter(!is.na(Corridor))
        sub_daily_sfo <- get_cor_weekly_sf_by_day(daily_sfo, subcorridors) %>%
            filter(!is.na(Corridor))
        sub_weekly_sf_by_day <- get_cor_weekly_sf_by_day(weekly_sf_by_day, subcorridors) %>%
            filter(!is.na(Corridor))
        sub_weekly_sfo_by_day <- get_cor_weekly_sf_by_day(weekly_sfo_by_day, subcorridors) %>%
            filter(!is.na(Corridor))
        sub_monthly_sf_by_day <- get_cor_monthly_sf_by_day(monthly_sf_by_day, subcorridors) %>%
            filter(!is.na(Corridor))
        sub_monthly_sfo_by_day <- get_cor_monthly_sf_by_day(monthly_sfo_by_day, subcorridors) %>%
            filter(!is.na(Corridor))


        ############################### Ended here 9/13

        addtoRDS(daily_sfp, "daily_sfp.rds", "sf_freq", report_start_date, wk_calcs_start_date)
        addtoRDS(weekly_sf_by_day, "wsf.rds", "sf_freq", report_start_date, wk_calcs_start_date)
        addtoRDS(monthly_sf_by_day, "monthly_sfd.rds", "sf_freq", report_start_date, calcs_start_date)

        addtoRDS(cor_daily_sfp, "cor_daily_sfp.rds", "sf_freq", report_start_date, wk_calcs_start_date)
        addtoRDS(cor_weekly_sf_by_day, "cor_wsf.rds", "sf_freq", report_start_date, wk_calcs_start_date)
        addtoRDS(cor_monthly_sf_by_day, "cor_monthly_sfd.rds", "sf_freq", report_start_date, calcs_start_date)

        addtoRDS(sub_daily_sfp, "sub_daily_sfp.rds", "sf_freq", report_start_date, wk_calcs_start_date)
        addtoRDS(sub_weekly_sf_by_day, "sub_wsf.rds", "sf_freq", report_start_date, wk_calcs_start_date)
        addtoRDS(sub_monthly_sf_by_day, "sub_monthly_sfd.rds", "sf_freq", report_start_date, calcs_start_date)

        addtoRDS(daily_sfo, "daily_sfo.rds", "sf_freq", report_start_date, wk_calcs_start_date)
        addtoRDS(weekly_sfo_by_day, "wsfo.rds", "sf_freq", report_start_date, wk_calcs_start_date)
        addtoRDS(monthly_sfo_by_day, "monthly_sfo.rds", "sf_freq", report_start_date, calcs_start_date)
        addtoRDS(cor_daily_sfo, "cor_daily_sfo.rds", "sf_freq", report_start_date, wk_calcs_start_date)
        addtoRDS(cor_weekly_sfo_by_day, "cor_wsfo.rds", "sf_freq", report_start_date, wk_calcs_start_date)
        addtoRDS(cor_monthly_sfo_by_day, "cor_monthly_sfo.rds", "sf_freq", report_start_date, calcs_start_date)
        addtoRDS(sub_daily_sfo, "sub_daily_sfo.rds", "sf_freq", report_start_date, wk_calcs_start_date)
        addtoRDS(sub_weekly_sfo_by_day, "sub_wsfo.rds", "sf_freq", report_start_date, wk_calcs_start_date)
        addtoRDS(sub_monthly_sfo_by_day, "sub_monthly_sfo.rds", "sf_freq", report_start_date, calcs_start_date)

        rm(sfp)
        rm(sfo)
        rm(daily_sfp)
        rm(weekly_sf_by_day)
        rm(monthly_sf_by_day)
        rm(cor_daily_sfp)
        rm(cor_weekly_sf_by_day)
        rm(cor_monthly_sf_by_day)
        rm(sub_daily_sfp)
        rm(sub_weekly_sf_by_day)
        rm(sub_monthly_sf_by_day)

        rm(daily_sfo)
        rm(weekly_sfo_by_day)
        rm(monthly_sfo_by_day)
        rm(cor_daily_sfo)
        rm(cor_weekly_sfo_by_day)
        rm(cor_monthly_sfo_by_day)
        rm(sub_daily_sfo)
        rm(sub_weekly_sfo_by_day)
        rm(sub_monthly_sfo_by_day)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)

# HOURLY SPLIT FAILURES #######################################################

print(glue("{Sys.time()} Hourly Split Failures [16 of 29 (sigops)]"))

tryCatch(
    {
        sfh <- get_sf_by_hr(sf)
        msfh <- get_monthly_sf_by_hr(sfh)

        # Hourly volumes by Corridor --------------------------------------------------
        cor_msfh <- get_cor_monthly_sf_by_hr(msfh, corridors)
        sub_msfh <- get_cor_monthly_sf_by_hr(msfh, subcorridors) %>%
            filter(!is.na(Corridor))

        addtoRDS(msfh, "msfh.rds", "sf_freq", report_start_date, calcs_start_date)
        addtoRDS(cor_msfh, "cor_msfh.rds", "sf_freq", report_start_date, calcs_start_date)
        addtoRDS(sub_msfh, "sub_msfh.rds", "sf_freq", report_start_date, calcs_start_date)

        rm(sf)
        rm(sfh)
        rm(msfh)
        rm(cor_msfh)
        rm(sub_msfh)

        # gc()
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)



# DAILY QUEUE SPILLBACK #######################################################

print(glue("{Sys.time()} Daily Queue Spillback [17 of 29 (sigops)]"))

tryCatch(
    {
        qs <- s3_read_parquet_parallel(
            bucket = conf$bucket,
            table_name = "queue_spillback",
            start_date = wk_calcs_start_date,
            end_date = report_end_date,
            signals_list = signals_list
        ) %>%
            mutate(
                SignalID = factor(SignalID),
                CallPhase = factor(CallPhase),
                Date = date(Date)
            )

        daily_qs <- get_daily_avg(qs, "qs_freq", "cycles")
        wqs <- get_weekly_qs_by_day(qs)
        monthly_qsd <- get_monthly_qs_by_day(qs)

        cor_daily_qs <- get_cor_weekly_qs_by_day(daily_qs, corridors) %>% select(-Week)
        cor_wqs <- get_cor_weekly_qs_by_day(wqs, corridors)
        cor_monthly_qsd <- get_cor_monthly_qs_by_day(monthly_qsd, corridors)

        sub_daily_qs <- get_cor_weekly_qs_by_day(daily_qs, subcorridors) %>% select(-Week) %>%
            filter(!is.na(Corridor))
        sub_wqs <- get_cor_weekly_qs_by_day(wqs, subcorridors) %>%
            filter(!is.na(Corridor))
        sub_monthly_qsd <- get_cor_monthly_qs_by_day(monthly_qsd, subcorridors) %>%
            filter(!is.na(Corridor))

        addtoRDS(daily_qs, "daily_qsd.rds", "qs_freq", report_start_date, calcs_start_date)
        addtoRDS(wqs, "wqs.rds", "qs_freq", report_start_date, wk_calcs_start_date)
        addtoRDS(monthly_qsd, "monthly_qsd.rds", "qs_freq", report_start_date, calcs_start_date)
        addtoRDS(cor_daily_qs, "cor_daily_qsd.rds", "qs_freq", report_start_date, calcs_start_date)
        addtoRDS(cor_wqs, "cor_wqs.rds", "qs_freq", report_start_date, wk_calcs_start_date)
        addtoRDS(cor_monthly_qsd, "cor_monthly_qsd.rds", "qs_freq", report_start_date, calcs_start_date)
        addtoRDS(sub_daily_qs, "sub_daily_qsd.rds", "qs_freq", report_start_date, calcs_start_date)
        addtoRDS(sub_wqs, "sub_wqs.rds", "qs_freq", report_start_date, wk_calcs_start_date)
        addtoRDS(sub_monthly_qsd, "sub_monthly_qsd.rds", "qs_freq", report_start_date, calcs_start_date)

        rm(daily_qs)
        rm(wqs)
        rm(monthly_qsd)
        rm(cor_daily_qs)
        rm(cor_wqs)
        rm(cor_monthly_qsd)
        rm(sub_daily_qs)
        rm(sub_wqs)
        rm(sub_monthly_qsd)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)




# HOURLY QUEUE SPILLBACK ######################################################

print(glue("{Sys.time()} Hourly Queue Spillback [18 of 29 (sigops)]"))

tryCatch(
    {
        qsh <- get_qs_by_hr(qs)
        mqsh <- get_monthly_qs_by_hr(qsh)

        # Hourly volumes by Corridor --------------------------------------------------
        cor_mqsh <- get_cor_monthly_qs_by_hr(mqsh, corridors)
        sub_mqsh <- get_cor_monthly_qs_by_hr(mqsh, subcorridors) %>%
            filter(!is.na(Corridor))

        addtoRDS(mqsh, "mqsh.rds", "qs_freq", report_start_date, calcs_start_date)
        addtoRDS(cor_mqsh, "cor_mqsh.rds", "qs_freq", report_start_date, calcs_start_date)
        addtoRDS(sub_mqsh, "sub_mqsh.rds", "qs_freq", report_start_date, calcs_start_date)

        rm(qs)
        rm(qsh)
        rm(mqsh)
        rm(cor_mqsh)
        rm(sub_mqsh)
        # gc()
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)



# TRAVEL TIME AND BUFFER TIME INDEXES #########################################

print(glue("{Sys.time()} Travel Time Indexes [19 of 29 (sigops)]"))

tryCatch(
    {

        # ------- Corridor Travel Time Metrics ------- #

        tt <- s3_read_parquet_parallel(
            bucket = conf$bucket,
            table_name = "cor_travel_time_metrics_1hr",
            s3root = 'sigops',
            start_date = calcs_start_date,
            end_date = report_end_date
        ) %>%
            mutate(
                Corridor = factor(Corridor)
            ) %>%
            left_join(distinct(all_corridors, Zone_Group, Zone, Corridor)) %>%
            filter(!is.na(Zone_Group))

        tti <- tt %>%
            dplyr::select(-c(pti, bi, speed_mph))

        pti <- tt %>%
            dplyr::select(-c(tti, bi, speed_mph))

        bi <- tt %>%
            dplyr::select(-c(tti, pti, speed_mph))

        spd <- tt %>%
            dplyr::select(-c(tti, pti, bi))

        cor_monthly_vph <- readRDS("cor_monthly_vph.rds") %>%
            rename(Zone = Zone_Group) %>%
            left_join(distinct(corridors, Zone_Group, Zone), by = ("Zone"), relationship = "many-to-many")


        cor_weekly_vph <- readRDS("cor_weekly_vph.rds") %>%
            rename(Zone = Zone_Group) %>%
            left_join(distinct(corridors, Zone_Group, Zone), by = ("Zone"), relationship = "many-to-many")


        cor_monthly_tti_by_hr <- get_cor_monthly_ti_by_hr(tti, cor_monthly_vph, all_corridors)
        cor_monthly_pti_by_hr <- get_cor_monthly_ti_by_hr(pti, cor_monthly_vph, all_corridors)
        cor_monthly_bi_by_hr <- get_cor_monthly_ti_by_hr(bi, cor_monthly_vph, all_corridors)
        cor_monthly_spd_by_hr <- get_cor_monthly_ti_by_hr(spd, cor_monthly_vph, all_corridors)

        cor_monthly_tti <- get_cor_monthly_ti_by_day(tti, cor_monthly_vph, all_corridors)
        cor_monthly_pti <- get_cor_monthly_ti_by_day(pti, cor_monthly_vph, all_corridors)
        cor_monthly_bi <- get_cor_monthly_ti_by_day(bi, cor_monthly_vph, all_corridors)
        cor_monthly_spd <- get_cor_monthly_ti_by_day(spd, cor_monthly_vph, all_corridors)

        addtoRDS(cor_monthly_tti, "cor_monthly_tti.rds", "tti", report_start_date, calcs_start_date)
        addtoRDS(cor_monthly_tti_by_hr, "cor_monthly_tti_by_hr.rds", "tti", report_start_date, calcs_start_date)

        addtoRDS(cor_monthly_pti, "cor_monthly_pti.rds", "pti", report_start_date, calcs_start_date)
        addtoRDS(cor_monthly_pti_by_hr, "cor_monthly_pti_by_hr.rds", "pti", report_start_date, calcs_start_date)

        addtoRDS(cor_monthly_bi, "cor_monthly_bi.rds", "bi", report_start_date, calcs_start_date)
        addtoRDS(cor_monthly_bi_by_hr, "cor_monthly_bi_by_hr.rds", "bi", report_start_date, calcs_start_date)

        addtoRDS(cor_monthly_spd, "cor_monthly_spd.rds", "speed_mph", report_start_date, calcs_start_date)
        addtoRDS(cor_monthly_spd_by_hr, "cor_monthly_spd_by_hr.rds", "speed_mph", report_start_date, calcs_start_date)

        # ------- Subcorridor Travel Time Metrics ------- #

        tt <- s3_read_parquet_parallel(
            bucket = conf$bucket,
            table_name = "sub_travel_time_metrics_1hr",
            s3root = 'sigops',
            start_date = calcs_start_date,
            end_date = report_end_date
        ) %>%
            mutate(
                Corridor = factor(Corridor),
                Subcorridor = factor(Subcorridor)
            ) %>%
            rename(
                Zone = Corridor,
                Corridor = Subcorridor
            ) %>%
            left_join(distinct(subcorridors, Zone_Group, Zone), relationship = "many-to-many")

        tti <- tt %>%
            dplyr::select(-c(pti, bi, speed_mph))

        pti <- tt %>%
            dplyr::select(-c(tti, bi, speed_mph))

        bi <- tt %>%
            dplyr::select(-c(tti, pti, speed_mph))

        spd <- tt %>%
            dplyr::select(-c(tti, pti, bi))

        sub_monthly_vph <- readRDS("sub_monthly_vph.rds") %>%
            rename(Zone = Zone_Group) %>%
            left_join(distinct(subcorridors, Zone_Group, Zone), by = ("Zone"))

        sub_weekly_vph <- readRDS("sub_weekly_vph.rds") %>%
            rename(Zone = Zone_Group) %>%
            left_join(distinct(subcorridors, Zone_Group, Zone), by = ("Zone"))


        sub_monthly_tti_by_hr <- get_cor_monthly_ti_by_hr(tti, sub_monthly_vph, subcorridors)
        sub_monthly_pti_by_hr <- get_cor_monthly_ti_by_hr(pti, sub_monthly_vph, subcorridors)
        sub_monthly_bi_by_hr <- get_cor_monthly_ti_by_hr(bi, sub_monthly_vph, subcorridors)
        sub_monthly_spd_by_hr <- get_cor_monthly_ti_by_hr(spd, sub_monthly_vph, subcorridors)

        sub_monthly_tti <- get_cor_monthly_ti_by_day(tti, sub_monthly_vph, subcorridors)
        sub_monthly_pti <- get_cor_monthly_ti_by_day(pti, sub_monthly_vph, subcorridors)
        sub_monthly_bi <- get_cor_monthly_ti_by_day(bi, sub_monthly_vph, subcorridors)
        sub_monthly_spd <- get_cor_monthly_ti_by_day(spd, sub_monthly_vph, subcorridors)

        addtoRDS(sub_monthly_tti, "sub_monthly_tti.rds", "tti", report_start_date, calcs_start_date)
        addtoRDS(sub_monthly_tti_by_hr, "sub_monthly_tti_by_hr.rds", "tti", report_start_date, calcs_start_date)

        addtoRDS(sub_monthly_pti, "sub_monthly_pti.rds", "pti", report_start_date, calcs_start_date)
        addtoRDS(sub_monthly_pti_by_hr, "sub_monthly_pti_by_hr.rds", "pti", report_start_date, calcs_start_date)

        addtoRDS(sub_monthly_bi, "sub_monthly_bi.rds", "bi", report_start_date, calcs_start_date)
        addtoRDS(sub_monthly_bi_by_hr, "sub_monthly_bi_by_hr.rds", "bi", report_start_date, calcs_start_date)

        addtoRDS(sub_monthly_spd, "sub_monthly_spd.rds", "speed_mph", report_start_date, calcs_start_date)
        addtoRDS(sub_monthly_spd_by_hr, "sub_monthly_spd_by_hr.rds", "speed_mph", report_start_date, calcs_start_date)

        rm(tt)
        rm(tti)
        rm(pti)
        rm(bi)
        rm(cor_monthly_vph)
        rm(cor_monthly_tti)
        rm(cor_monthly_tti_by_hr)
        rm(cor_monthly_pti)
        rm(cor_monthly_pti_by_hr)
        rm(cor_monthly_bi)
        rm(cor_monthly_bi_by_hr)

        rm(sub_monthly_tti)
        rm(sub_monthly_tti_by_hr)
        rm(sub_monthly_pti)
        rm(sub_monthly_pti_by_hr)
        rm(sub_monthly_bi)
        rm(sub_monthly_bi_by_hr)

        # gc()
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)



# CCTV UPTIME From 511 and Encoders

print(glue("{Sys.time()} CCTV Uptimes [20 of 29 (sigops)]"))

tryCatch(
    {
        daily_cctv_uptime_511 <- get_daily_cctv_uptime(
            conf$athena$database, "cctv_uptime", cam_config, wk_calcs_start_date
        )
        daily_cctv_uptime_encoders <- get_daily_cctv_uptime(
            conf$athena$database, "cctv_uptime_encoders", cam_config, wk_calcs_start_date
        )

        # up:
        #   2-on 511 (dark brown)
        #   1-working at encoder but not on 511 (light brown)
        #   0-not working on either (gray)
        daily_cctv_uptime <- full_join(
            daily_cctv_uptime_511,
            daily_cctv_uptime_encoders,
            by = c(
                "Zone_Group", "Zone", "Corridor", "Subcorridor",
                "CameraID", "Description", "Date"
            ),
            relationship = "many-to-many",
            suffix = c("_511", "_enc")
        ) %>%
            complete(
                nesting(Zone_Group, Zone, Corridor, Subcorridor, CameraID, Description), Date,
            ) %>%
            replace_na(list(
                up_enc = 0, num_enc = 0, uptime_enc = 0,
                up_511 = 0, num_511 = 0, uptime_511 = 0
            )) %>%
            select(
                Zone_Group, Zone, Corridor, Subcorridor,
                CameraID, Description, Date, up_511, up_enc
            ) %>%
            mutate(
                uptime = up_511,
                num = 1,
                up = pmax(up_511 * 2, up_enc),
                Zone_Group = factor(Zone_Group),
                Zone = factor(Zone),
                Corridor = factor(Corridor),
                Subcorridor = factor(Subcorridor),
                CameraID = factor(CameraID),
                Description = factor(Description)
            )


        # Find the days where uptime across the board is very low (close to 0)
        #  This is symptomatic of a problem with the acquisition rather than the cameras themselves
        bad_days <- daily_cctv_uptime %>%
            group_by(Date) %>%
            summarize(
                sup = sum(uptime),
                snum = sum(num),
                suptime = sum(uptime) / sum(num),
                .groups = "drop"
            ) %>%
            filter(suptime < 0.2)

        # Filter out bad days, i.e., those with systemic issues
        daily_cctv_uptime <- daily_cctv_uptime %>%
            filter(!Date %in% bad_days$Date)

        weekly_cctv_uptime <- get_weekly_avg_by_day_cctv(daily_cctv_uptime)

        monthly_cctv_uptime <- daily_cctv_uptime %>%
            group_by(
                Zone_Group, Zone, Corridor, Subcorridor, CameraID, Description,
                Month = floor_date(Date, unit = "months")
            ) %>%
            summarize(
                up = sum(uptime),
                uptime = weighted.mean(uptime, num), num = sum(num),
                .groups = "drop"
            )

        cor_daily_cctv_uptime <- get_cor_weekly_avg_by_day(
            daily_cctv_uptime, all_corridors, "uptime", "num"
        )

        cor_weekly_cctv_uptime <- get_cor_weekly_avg_by_day(
            weekly_cctv_uptime, all_corridors, "uptime", "num"
        )

        cor_monthly_cctv_uptime <- get_cor_monthly_avg_by_day(
            monthly_cctv_uptime, all_corridors, "uptime", "num"
        )


        sub_daily_cctv_uptime <- daily_cctv_uptime %>%
            select(-Zone_Group) %>%
            filter(!is.na(Subcorridor)) %>%
            rename(
                Zone_Group = Zone,
                Zone = Corridor,
                Corridor = Subcorridor
            ) %>%
            get_cor_weekly_avg_by_day(subcorridors, "uptime", "num")

        sub_weekly_cctv_uptime <- weekly_cctv_uptime %>%
            select(-Zone_Group) %>%
            filter(!is.na(Subcorridor)) %>%
            rename(
                Zone_Group = Zone,
                Zone = Corridor,
                Corridor = Subcorridor
            ) %>%
            get_cor_weekly_avg_by_day(subcorridors, "uptime", "num")

        sub_monthly_cctv_uptime <- monthly_cctv_uptime %>%
            select(-Zone_Group) %>%
            filter(!is.na(Subcorridor)) %>%
            rename(
                Zone_Group = Zone,
                Zone = Corridor,
                Corridor = Subcorridor
            ) %>%
            get_cor_monthly_avg_by_day(subcorridors, "uptime", "num")

        addtoRDS(daily_cctv_uptime, "daily_cctv_uptime.rds", "uptime", report_start_date, calcs_start_date)
        addtoRDS(weekly_cctv_uptime, "weekly_cctv_uptime.rds", "uptime", report_start_date, wk_calcs_start_date)
        addtoRDS(monthly_cctv_uptime, "monthly_cctv_uptime.rds", "uptime", report_start_date, calcs_start_date)

        addtoRDS(cor_daily_cctv_uptime, "cor_daily_cctv_uptime.rds", "uptime", report_start_date, calcs_start_date)
        addtoRDS(cor_weekly_cctv_uptime, "cor_weekly_cctv_uptime.rds", "uptime", report_start_date, wk_calcs_start_date)
        addtoRDS(cor_monthly_cctv_uptime, "cor_monthly_cctv_uptime.rds", "uptime", report_start_date, calcs_start_date)

        addtoRDS(sub_daily_cctv_uptime, "sub_daily_cctv_uptime.rds", "uptime", report_start_date, calcs_start_date)
        addtoRDS(sub_weekly_cctv_uptime, "sub_weekly_cctv_uptime.rds", "uptime", report_start_date, wk_calcs_start_date)
        addtoRDS(sub_monthly_cctv_uptime, "sub_monthly_cctv_uptime.rds", "uptime", report_start_date, calcs_start_date)

        rm(daily_cctv_uptime_511)
        rm(daily_cctv_uptime_encoders)
        rm(daily_cctv_uptime)
        rm(bad_days)
        rm(weekly_cctv_uptime)
        rm(monthly_cctv_uptime)
        rm(cor_daily_cctv_uptime)
        rm(cor_weekly_cctv_uptime)
        rm(cor_monthly_cctv_uptime)
        rm(sub_daily_cctv_uptime)
        rm(sub_weekly_cctv_uptime)
        rm(sub_monthly_cctv_uptime)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)

# # RSU UPTIME

if (FALSE) {
    daily_rsu_uptime <- get_rsu_uptime(wk_calcs_start_date)

    # Find the days where uptime across the board is very low (close to 0)
    #  This is symptomatic of a problem with the acquisition rather than the rsus themselves
    bad_days <- daily_rsu_uptime %>%
        group_by(Date) %>%
        summarize(suptime = weighted.mean(uptime, total, na.rm = TRUE), .groups = "drop") %>%
        filter(suptime < 0.2)

    # Filter out bad days, i.e., those with systemic issues
    daily_rsu_uptime <- daily_rsu_uptime %>%
        filter(!Date %in% bad_days$Date)

    cor_daily_rsu_uptime <- get_cor_weekly_avg_by_day(
        daily_rsu_uptime, corridors, "uptime"
    )
    sub_daily_rsu_uptime <- get_cor_weekly_avg_by_day(
        daily_rsu_uptime, subcorridors, "uptime"
    )

    weekly_rsu_uptime <- get_weekly_avg_by_day(
        mutate(daily_rsu_uptime, CallPhase = 0, Week = week(Date)), "uptime",
        peak_only = FALSE
    )
    cor_weekly_rsu_uptime <- get_cor_weekly_avg_by_day(
        weekly_rsu_uptime, corridors, "uptime"
    )
    sub_weekly_rsu_uptime <- get_cor_weekly_avg_by_day(
        weekly_rsu_uptime, subcorridors, "uptime"
    )

    monthly_rsu_uptime <- get_monthly_avg_by_day(
        mutate(daily_rsu_uptime, CallPhase = 0), "uptime",
        peak_only = FALSE
    )
    cor_monthly_rsu_uptime <- get_cor_monthly_avg_by_day(
        monthly_rsu_uptime, corridors, "uptime"
    )
    sub_monthly_rsu_uptime <- get_cor_monthly_avg_by_day(
        monthly_rsu_uptime, subcorridors, "uptime"
    )


    addtoRDS(daily_rsu_uptime, "daily_rsu_uptime.rds", "uptime", report_start_date, calcs_start_date)
    addtoRDS(weekly_rsu_uptime, "weekly_rsu_uptime.rds", "uptime", report_start_date, wk_calcs_start_date)
    addtoRDS(monthly_rsu_uptime, "monthly_rsu_uptime.rds", "uptime", report_start_date, calcs_start_date)

    addtoRDS(cor_daily_rsu_uptime, "cor_daily_rsu_uptime.rds", "uptime", report_start_date, calcs_start_date)
    addtoRDS(cor_weekly_rsu_uptime, "cor_weekly_rsu_uptime.rds", "uptime", report_start_date, wk_calcs_start_date)
    addtoRDS(cor_monthly_rsu_uptime, "cor_monthly_rsu_uptime.rds", "uptime", report_start_date, calcs_start_date)

    addtoRDS(sub_daily_rsu_uptime, "sub_daily_rsu_uptime.rds", "uptime", report_start_date, calcs_start_date)
    addtoRDS(sub_weekly_rsu_uptime, "sub_weekly_rsu_uptime.rds", "uptime", report_start_date, wk_calcs_start_date)
    addtoRDS(sub_monthly_rsu_uptime, "sub_monthly_rsu_uptime.rds", "uptime", report_start_date, calcs_start_date)
}









# ACTIVITIES ##############################

print(glue("{Sys.time()} TEAMS [21 of 29 (sigops)]"))

tryCatch(
    {
        teams <- get_teams_tasks_from_s3(
            bucket = conf$bucket,
            archived_tasks_prefix = "mark/teams/tasks202",
            current_tasks_key = "mark/teams/tasks.csv.zip",
            report_start_date
        )
        teams <- tidy_teams_tasks(
            teams,
            bucket = conf$bucket,
            corridors,
            replicate = TRUE
        )

        tasks_by_type <- get_outstanding_tasks_by_param(
            teams, "Task_Type", report_start_date
        )
        tasks_by_subtype <- get_outstanding_tasks_by_param(
            teams, "Task_Subtype", report_start_date
        )
        tasks_by_priority <- get_outstanding_tasks_by_param(
            teams, "Priority", report_start_date
        )
        tasks_by_source <- get_outstanding_tasks_by_param(
            teams, "Task_Source", report_start_date
        )
        tasks_all <- get_outstanding_tasks_by_param(
            teams, "All", report_start_date
        )

        cor_outstanding_tasks_by_day_range <- lapply(
            dates, function(x) get_outstanding_tasks_by_day_range(teams, report_start_date, x)
        ) %>%
            bind_rows() %>%
            mutate(
                Zone_Group = factor(Zone_Group),
                Corridor = factor(Corridor)
            ) %>%
            arrange(Zone_Group, Corridor, Month) %>%
            group_by(Zone_Group, Corridor) %>%
            mutate(
                delta.over45 = (over45 - lag(over45)) / lag(over45),
                delta.mttr = (mttr - lag(mttr)) / lag(mttr)
            ) %>%
            ungroup()

        sig_outstanding_tasks_by_day_range <- cor_outstanding_tasks_by_day_range %>%
            group_by(Corridor) %>%
            filter(as.character(Zone_Group) == min(as.character(Zone_Group))) %>%
            mutate(Zone_Group = Corridor) %>%
            filter(Corridor %in% all_corridors$Corridor) %>%
            ungroup()

        saveRDS(tasks_by_type, "tasks_by_type.rds")
        saveRDS(tasks_by_subtype, "tasks_by_subtype.rds")
        saveRDS(tasks_by_priority, "tasks_by_priority.rds")
        saveRDS(tasks_by_source, "tasks_by_source.rds")
        saveRDS(tasks_all, "tasks_all.rds")
        saveRDS(cor_outstanding_tasks_by_day_range, "cor_tasks_by_date.rds")
        saveRDS(sig_outstanding_tasks_by_day_range, "sig_tasks_by_date.rds")

        rm(tasks_by_type)
        rm(tasks_by_subtype)
        rm(tasks_by_priority)
        rm(tasks_by_source)
        rm(tasks_all)
        rm(cor_outstanding_tasks_by_day_range)
        rm(sig_outstanding_tasks_by_day_range)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)




# USER DELAY COSTS   ##############################


print(glue("{Sys.time()} User Delay Costs [22 of 29 (sigops)]"))



# Flash Events ###############################################################

print(glue("{Sys.time()} Flash Events [23 of 29 (sigops)]"))

tryCatch(
    {
        fe <- s3_read_parquet_parallel(
            bucket = conf$bucket,
            table_name = "flash_events",
            start_date = wk_calcs_start_date,
            end_date = report_end_date,
            signals_list = signals_list
        ) %>%
            mutate(
                SignalID = factor(SignalID),
                Date = date(Date)
            )

        # No need for Weekly

        # Monthly flash events for bar charts and % change ---------------------------------
        monthly_flash <- get_monthly_flashevent(fe)

        # Group into corridors
        cor_monthly_flash <- get_cor_monthly_flash(monthly_flash, corridors)

        # Subcorridors
        sub_monthly_flash <- get_cor_monthly_flash(monthly_flash, subcorridors) %>%
            filter(!is.na(Corridor))

        # Monthly % change from previous month by corridor ----------------------------

        addtoRDS(monthly_flash, "monthly_flash.rds", "flash", report_start_date, calcs_start_date)
        addtoRDS(cor_monthly_flash, "cor_monthly_flash.rds", "flash", report_start_date, calcs_start_date)
        addtoRDS(sub_monthly_flash, "sub_monthly_flash.rds", "flash", report_start_date, calcs_start_date)

        rm(fe)
        rm(monthly_flash)
        rm(cor_monthly_flash)
        rm(sub_monthly_flash)
        # gc()
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)



# BIKE/PED SAFETY INDEX #######################################################

print(glue("{Sys.time()} Bike/Ped Safety Index [24 of 29 (sigops)]"))

tryCatch(
    {
        date_range <- seq(ymd(calcs_start_date), ymd(report_end_date), by = "month")

        sub_monthly_bpsi <- lapply(
            date_range, function(d) {
                obj <- glue("mark/bike_ped_safety_index/bpsi_sub_{d}.parquet")
                if (length(get_bucket(conf$bucket, obj)) > 0) {
                    s3read_using(read_parquet, bucket = conf$bucket, object = obj) %>%
                        select(-starts_with("__")) %>%
                        mutate(Month = d, .before = "overall_pct") %>%
                        rename(bpsi = overall_pct)
                }
            }
        ) %>% bind_rows() %>%
            mutate(
                Corridor = factor(Corridor),
                Subcorridor = factor(Subcorridor)
            )


        cor_monthly_bpsi <- lapply(
            date_range, function(d) {
                obj <- glue("mark/bike_ped_safety_index/bpsi_cor_{d}.parquet")
                if (length(get_bucket(conf$bucket, obj)) > 0) {
                    s3read_using(read_parquet, bucket = conf$bucket, object = obj) %>%
                        select(-starts_with("__")) %>%
                        mutate(Month = d, .before = "overall_pct") %>%
                        rename(bpsi = overall_pct)
                }
            }
        ) %>% bind_rows() %>%
            mutate(
                Corridor = factor(Corridor)
            )

        sub_monthly_bpsi <- bind_rows(
            sub_monthly_bpsi,
            mutate(cor_monthly_bpsi, Subcorridor = Corridor)
        ) %>%
            rename(Zone_Group = Corridor, Corridor = Subcorridor) %>%
            group_by(Zone_Group, Corridor) %>%
            arrange(Month) %>%
            mutate(
                ones = NA,
                delta = (bpsi - lag(bpsi))/lag(bpsi)) %>%
            ungroup()

        cor_monthly_bpsi <- left_join(
            cor_monthly_bpsi,
            distinct(corridors, Zone_Group, Corridor),
            by = "Corridor",
            relationship = "many-to-many"
        ) %>%
            relocate(Zone_Group, .before = "Corridor") %>%
            group_by(Zone_Group, Corridor) %>%
            arrange(Month) %>%
            mutate(
                ones = NA,
                delta = (bpsi - lag(bpsi))/lag(bpsi)) %>%
            ungroup()

        addtoRDS(cor_monthly_bpsi, "cor_monthly_bpsi.rds", "bpsi", report_start_date, calcs_start_date)
        addtoRDS(sub_monthly_bpsi, "sub_monthly_bpsi.rds", "bpsi", report_start_date, calcs_start_date)

        rm(cor_monthly_bpsi)
        rm(sub_monthly_bpsi)
        # gc()
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)



# RELATIVE SPEED INDEX ########################################################

print(glue("{Sys.time()} Relative Speed Index [25 of 29 (sigops)]"))

tryCatch(
    {
        date_range <- seq(ymd(calcs_start_date), ymd(report_end_date), by = "month")

        sub_monthly_rsi <- lapply(
            date_range, function(d) {
                obj <- glue("mark/relative_speed_index/rsi_sub_{d}.parquet")
                if (length(get_bucket(conf$bucket, obj)) > 0) {
                    s3read_using(read_parquet, bucket = conf$bucket, object = obj) %>%
                        select(-starts_with("__")) %>%
                        mutate(Month = d, .before = "rsi")
                }
            }
        ) %>% bind_rows() %>%
            mutate(
                Corridor = factor(Corridor),
                Subcorridor = factor(Subcorridor)
            )


        cor_monthly_rsi <- lapply(
            date_range, function(d) {
                obj <- glue("mark/relative_speed_index/rsi_cor_{d}.parquet")
                if (length(get_bucket(conf$bucket, obj)) > 0) {
                    s3read_using(read_parquet, bucket = conf$bucket, object = obj) %>%
                        select(-starts_with("__")) %>%
                        mutate(Month = d, .before = "rsi")
                }
            }
        ) %>% bind_rows() %>%
            mutate(
                Corridor = factor(Corridor)
            )

        sub_monthly_rsi <- bind_rows(
            sub_monthly_rsi,
            mutate(cor_monthly_rsi, Subcorridor = Corridor)
        ) %>%
            rename(Zone_Group = Corridor, Corridor = Subcorridor) %>%
            group_by(Zone_Group, Corridor) %>%
            arrange(Month) %>%
            mutate(
                ones = NA,
                delta = (rsi - lag(rsi))/lag(rsi)) %>%
            ungroup()

        cor_monthly_rsi <- left_join(
            cor_monthly_rsi,
            distinct(corridors, Zone_Group, Corridor),
            by = "Corridor",
            relationship = "many-to-many"
        ) %>%
            relocate(Zone_Group, .before = "Corridor") %>%
            group_by(Zone_Group, Corridor) %>%
            arrange(Month) %>%
            mutate(
                ones = NA,
                delta = (rsi - lag(rsi))/lag(rsi)) %>%
            ungroup()

        addtoRDS(cor_monthly_rsi, "cor_monthly_rsi.rds", "rsi", report_start_date, calcs_start_date)
        addtoRDS(sub_monthly_rsi, "sub_monthly_rsi.rds", "rsi", report_start_date, calcs_start_date)

        rm(cor_monthly_rsi)
        rm(sub_monthly_rsi)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)



# CRASH INDICES ###############################################################

print(glue("{Sys.time()} Crash Indices [26 of 29 (sigops)]"))

tryCatch(
    {
        crashes <- s3read_using(
            read_excel,
            bucket = conf$bucket,
            object = "Collisions Dataset 2017-2019.xlsm"
        ) %>%
            transmute(
                SignalID = as.factor(Signal_ID_Clean),
                Month = as.Date(Month),
                crashes_k,
                crashes_a,
                crashes_b,
                crashes_c,
                crashes_o,
                crashes_total,
                cost) %>%
            drop_na() %>%
            arrange(SignalID, Month) %>%
            group_by(SignalID, Month) %>%
            summarise(across(c(starts_with("crashes"), cost), sum), .groups = "drop")


        monthly_vpd <- readRDS("monthly_vpd.rds")

        # Running 12-months of volumes
        monthly_vpd <- monthly_vpd %>%
            complete(SignalID, Month) %>%
            arrange(SignalID, Month) %>%
            group_by(SignalID) %>%
            mutate(vpd12 = runner::mean_run(vpd, lag = 0, k = 12)) %>%
            ungroup()

        # Running 36 months of crashes
        monthly_36mo_crashes <- crashes %>%
            complete(SignalID, Month) %>%
            replace(is.na(.), 0) %>%
            arrange(SignalID, Month) %>%
            group_by(SignalID) %>%

            mutate(across(
                starts_with("crashes"),
                function(x) {runner::sum_run(x, lag = 0, k = 36)}
            )) %>%
            mutate(cost = runner::sum_run(cost, lag = 0, k = 36)) %>%
            ungroup()

        # -- Hack to use a fixed 36 month period for all months in report (which is the same as the vpd months)
        monthly_36mo_crashes <- filter(monthly_36mo_crashes, Month == max(Month))
        monthly_36mo_crashes <- lapply(
            unique(monthly_vpd$Month),
            function(m) mutate(monthly_36mo_crashes, Month = m)
            ) %>%
            bind_rows()
        # -- ---------------------------------------------------

        monthly_crashes <- full_join(
            monthly_36mo_crashes,
            monthly_vpd,
            by = c("SignalID", "Month"),
            relationship = "many-to-many"
            ) %>%
            mutate(
                cri = crashes_total * 1000 / (vpd * 3),
                kabco = cost / (vpd * 3),
                Date = Month,
                CallPhase = 0
            )

        # TODO: Insert a step to handle vpd==NA. Remove? Do nothing?

        monthly_crash_rate_index <- get_monthly_avg_by_day(monthly_crashes, "cri")
        monthly_kabco_index <- get_monthly_avg_by_day(monthly_crashes, "kabco")

        cor_monthly_crash_rate_index <- get_cor_monthly_avg_by_day(
            monthly_crash_rate_index, corridors, "cri")

        cor_monthly_kabco_index <- get_cor_monthly_avg_by_day(
            monthly_kabco_index, corridors, "kabco")


        sub_monthly_crash_rate_index <- get_cor_monthly_avg_by_day(
            monthly_crash_rate_index, subcorridors, "cri")

        sub_monthly_kabco_index <- get_cor_monthly_avg_by_day(
            monthly_kabco_index, subcorridors, "kabco")


        addtoRDS(monthly_crash_rate_index, "monthly_crash_rate_index.rds", "cri", report_start_date, calcs_start_date)
        addtoRDS(monthly_kabco_index, "monthly_kabco_index.rds", "kabco", report_start_date, calcs_start_date)

        addtoRDS(cor_monthly_crash_rate_index, "cor_monthly_crash_rate_index.rds", "cri", report_start_date, calcs_start_date)
        addtoRDS(cor_monthly_kabco_index, "cor_monthly_kabco_index.rds", "kabco", report_start_date, calcs_start_date)

        addtoRDS(sub_monthly_crash_rate_index, "sub_monthly_crash_rate_index.rds", "cri", report_start_date, calcs_start_date)
        addtoRDS(sub_monthly_kabco_index, "sub_monthly_kabco_index.rds", "kabco", report_start_date, calcs_start_date)

	rm(monthly_crash_rate_index)
        rm(monthly_kabco_index)
        rm(cor_monthly_crash_rate_index)
        rm(cor_monthly_kabco_index)
        rm(sub_monthly_crash_rate_index)
        rm(sub_monthly_kabco_index)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)




# Package up for Flexdashboard

print(glue("{Sys.time()} Package for Monthly Report [27 of 29 (sigops)]"))

tryCatch(
    {
        cor <- list()
        cor$dy <- list(
            "vpd" = readRDS("cor_daily_vpd.rds"),
            "vphpa" = readRDS("cor_daily_vph_peak.rds")$am,
            "vphpp" = readRDS("cor_daily_vph_peak.rds")$pm,
            "papd" = readRDS("cor_daily_papd.rds"),
            "tp" = readRDS("cor_daily_throughput.rds"),
            "aogd" = readRDS("cor_daily_aog.rds"),
            "prd" = readRDS("cor_daily_pr.rds"),
            "qsd" = readRDS("cor_daily_qsd.rds"),
            "sfd" = readRDS("cor_daily_sfp.rds"),
            "sfo" = readRDS("cor_daily_sfo.rds"),
            "du" = readRDS("cor_avg_daily_detector_uptime.rds"),
            "cu" = readRDS("cor_daily_comm_uptime.rds"),
            "pau" = readRDS("cor_daily_pa_uptime.rds"),
            "cctv" = readRDS("cor_daily_cctv_uptime.rds"),
            "ttyp" = readRDS("tasks_by_type.rds")$cor_daily,
            "tsub" = readRDS("tasks_by_subtype.rds")$cor_daily,
            "tpri" = readRDS("tasks_by_priority.rds")$cor_daily,
            "tsou" = readRDS("tasks_by_source.rds")$cor_daily,
            "tasks" = readRDS("tasks_all.rds")$cor_daily,
            "reported" = readRDS("tasks_all.rds")$cor_daily %>%
                transmute(Zone_Group, Corridor, Date, Reported, delta = NA),
            "resolved" = readRDS("tasks_all.rds")$cor_daily %>%
                transmute(Zone_Group, Corridor, Date, Resolved, delta = NA),
            "outstanding" = readRDS("tasks_all.rds")$cor_daily %>%
                transmute(Zone_Group, Corridor, Date, Outstanding, delta = NA)
        )
        cor$wk <- list(
            "vpd" = readRDS("cor_weekly_vpd.rds"),
            # "vph" = readRDS("cor_weekly_vph.rds"),
            "vphpa" = readRDS("cor_weekly_vph_peak.rds")$am,
            "vphpp" = readRDS("cor_weekly_vph_peak.rds")$pm,
            "papd" = readRDS("cor_weekly_papd.rds"),
            # "paph" = readRDS("cor_weekly_paph.rds"),
            "pd" = readRDS("cor_weekly_pd_by_day.rds"),
            "tp" = readRDS("cor_weekly_throughput.rds"),
            "aogd" = readRDS("cor_weekly_aog_by_day.rds"),
            "prd" = readRDS("cor_weekly_pr_by_day.rds"),
            "qsd" = readRDS("cor_wqs.rds"),
            "sfd" = readRDS("cor_wsf.rds"),
            "sfo" = readRDS("cor_wsfo.rds"),
            "du" = readRDS("cor_weekly_detector_uptime.rds"),
            "cu" = readRDS("cor_weekly_comm_uptime.rds"),
            "pau" = readRDS("cor_weekly_pa_uptime.rds"),
            "cctv" = readRDS("cor_weekly_cctv_uptime.rds")
        )
        cor$mo <- list(
            "vpd" = readRDS("cor_monthly_vpd.rds"),
            # "vph" = readRDS("cor_monthly_vph.rds"),
            "vphpa" = readRDS("cor_monthly_vph_peak.rds")$am,
            "vphpp" = readRDS("cor_monthly_vph_peak.rds")$pm,
            "papd" = readRDS("cor_monthly_papd.rds"),
            # "paph" = readRDS("cor_monthly_paph.rds"),
            "pd" = readRDS("cor_monthly_pd_by_day.rds"),
            "tp" = readRDS("cor_monthly_throughput.rds"),
            "aogd" = readRDS("cor_monthly_aog_by_day.rds"),
            "aogh" = readRDS("cor_monthly_aog_by_hr.rds"),
            "prd" = readRDS("cor_monthly_pr_by_day.rds"),
            "prh" = readRDS("cor_monthly_pr_by_hr.rds"),
            "qsd" = readRDS("cor_monthly_qsd.rds"),
            "qsh" = readRDS("cor_mqsh.rds"),
            "sfd" = readRDS("cor_monthly_sfd.rds"),
            "sfh" = readRDS("cor_msfh.rds"),
            "sfo" = readRDS("cor_monthly_sfo.rds"),
            "tti" = readRDS("cor_monthly_tti.rds"),
            "ttih" = readRDS("cor_monthly_tti_by_hr.rds"),
            "pti" = readRDS("cor_monthly_pti.rds"),
            "ptih" = readRDS("cor_monthly_pti_by_hr.rds"),
            "bi" = readRDS("cor_monthly_bi.rds"),
            "bih" = readRDS("cor_monthly_bi_by_hr.rds"),
            "spd" = readRDS("cor_monthly_spd.rds"),
            "spdh" = readRDS("cor_monthly_spd_by_hr.rds"),
            "du" = readRDS("cor_monthly_detector_uptime.rds"),
            "cu" = readRDS("cor_monthly_comm_uptime.rds"),
            "pau" = readRDS("cor_monthly_pa_uptime.rds"),
            "cctv" = readRDS("cor_monthly_cctv_uptime.rds"),
            "ttyp" = readRDS("tasks_by_type.rds")$cor_monthly,
            "tsub" = readRDS("tasks_by_subtype.rds")$cor_monthly,
            "tpri" = readRDS("tasks_by_priority.rds")$cor_monthly,
            "tsou" = readRDS("tasks_by_source.rds")$cor_monthly,
            "tasks" = readRDS("tasks_all.rds")$cor_monthly,
            "reported" = readRDS("tasks_all.rds")$cor_monthly %>%
                transmute(Zone_Group, Corridor, Month, Reported, delta = delta.rep),
            "resolved" = readRDS("tasks_all.rds")$cor_monthly %>%
                transmute(Zone_Group, Corridor, Month, Resolved, delta = delta.res),
            "outstanding" = readRDS("tasks_all.rds")$cor_monthly %>%
                transmute(Zone_Group, Corridor, Month, Outstanding, delta = delta.out),
            "over45" = readRDS("cor_tasks_by_date.rds") %>%
                transmute(Zone_Group, Corridor, Month, over45, delta = delta.over45),
            "mttr" = readRDS("cor_tasks_by_date.rds") %>%
                transmute(Zone_Group, Corridor, Month, mttr, delta = delta.mttr),
            "flash" = readRDS("cor_monthly_flash.rds"),
            "bpsi" = readRDS("cor_monthly_bpsi.rds"),
            "rsi" = readRDS("cor_monthly_rsi.rds"),
            "cri" = readRDS("cor_monthly_crash_rate_index.rds"),
            "kabco" = readRDS("cor_monthly_kabco_index.rds")
        )
        cor$qu <- list(
            "vpd" = get_quarterly(cor$mo$vpd, "vpd"),
            # "vph" = data.frame(), # get_quarterly(cor$mo$vph, "vph"),
            "vphpa" = get_quarterly(cor$mo$vphpa, "vph"),
            "vphpp" = get_quarterly(cor$mo$vphpp, "vph"),
            "papd" = get_quarterly(cor$mo$papd, "papd"),
            "pd" = get_quarterly(cor$mo$pd, "pd"),
            "tp" = get_quarterly(cor$mo$tp, "vph"),
            "aogd" = get_quarterly(cor$mo$aogd, "aog", "vol"),
            "prd" = get_quarterly(cor$mo$prd, "pr", "vol"),
            "qsd" = get_quarterly(cor$mo$qsd, "qs_freq"),
            "sfd" = get_quarterly(cor$mo$sfd, "sf_freq"),
            "sfo" = get_quarterly(cor$mo$sfo, "sf_freq"),
            "tti" = get_quarterly(cor$mo$tti, "tti"),
            "pti" = get_quarterly(cor$mo$pti, "pti"),
            "bi" = get_quarterly(cor$mo$bi, "bi"),
            "spd" = get_quarterly(cor$mo$spd, "speed_mph"),
            "du" = get_quarterly(cor$mo$du, "uptime"),
            "cu" = get_quarterly(cor$mo$cu, "uptime"),
            "pau" = get_quarterly(cor$mo$pau, "uptime"),
            "cctv" = get_quarterly(cor$mo$cctv, "uptime", "num"),
            "ttyp" = get_quarterly(cor$mo$ttyp, "Reported"),
            "tsub" = get_quarterly(cor$mo$tsub, "Reported"),
            "tpri" = get_quarterly(cor$mo$tpri, "Reported"),
            "tsou" = get_quarterly(cor$mo$tsou, "Reported"),
            "reported" = get_quarterly(cor$mo$tasks, "Reported"),
            "resolved" = get_quarterly(cor$mo$tasks, "Resolved"),
            "outstanding" = get_quarterly(cor$mo$tasks, "Outstanding", operation = "latest"),
            "over45" = get_quarterly(cor$mo$over45, "over45", operation = "latest"),
            "mttr" = get_quarterly(cor$mo$mttr, "mttr", operation = "latest"),
            "bpsi" = get_quarterly(cor$mo$bpsi, "bpsi"),
            "rsi" = get_quarterly(cor$mo$rsi, "rsi"),
            "cri" = get_quarterly(cor$mo$cri, "cri"),
            "kabco" = get_quarterly(cor$mo$kabco, "kabco")
        )

        for (per in c("mo", "wk")) {
            for (tabl in names(cor[[per]])) {
                if ("data.frame" %in% class(cor[[per]][[tabl]])) {
                    cor[[per]][[tabl]] <- cor[[per]][[tabl]] %>%
                        mutate(Description = Corridor)
                }
            }
        }

        cor$summary_data <- get_corridor_summary_data(cor)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)


tryCatch(
    {
        sub <- list()
        sub$dy <- list(
            "vpd" = readRDS("sub_daily_vpd.rds"),
            "vphpa" = readRDS("sub_daily_vph_peak.rds")$am,
            "vphpp" = readRDS("sub_daily_vph_peak.rds")$pm,
            "papd" = readRDS("sub_daily_papd.rds"),
            "tp" = readRDS("sub_daily_throughput.rds"),
            "aogd" = readRDS("sub_daily_aog.rds"),
            "prd" = readRDS("sub_daily_pr.rds"),
            "qsd" = readRDS("sub_daily_qsd.rds"),
            "sfd" = readRDS("sub_daily_sfp.rds"),
            "sfo" = readRDS("sub_daily_sfo.rds"),
            "du" = readRDS("sub_avg_daily_detector_uptime.rds") %>%
                select(Zone_Group, Corridor, Date, uptime.sb, uptime.pr, uptime),
            "cu" = readRDS("sub_daily_comm_uptime.rds") %>%
                select(Zone_Group, Corridor, Date, uptime),
            "pau" = readRDS("sub_daily_pa_uptime.rds") %>%
                select(Zone_Group, Corridor, Date, uptime),
            "cctv" = readRDS("sub_daily_cctv_uptime.rds") %>%
                select(Zone_Group, Corridor, Date, uptime)
        )
        sub$wk <- list(
            "vpd" = readRDS("sub_weekly_vpd.rds") %>%
                select(Zone_Group, Corridor, Date, vpd, delta),
            "vphpa" = readRDS("sub_weekly_vph_peak.rds")$am %>%
                select(Zone_Group, Corridor, Date, vph, delta),
            "vphpp" = readRDS("sub_weekly_vph_peak.rds")$pm %>%
                select(Zone_Group, Corridor, Date, vph, delta),
            "papd" = readRDS("sub_weekly_papd.rds") %>%
                select(Zone_Group, Corridor, Date, papd, delta),
            # "paph" = readRDS("sub_weekly_paph.rds"),
            "pd" = readRDS("sub_weekly_pd_by_day.rds") %>%
                select(Zone_Group, Corridor, Date, pd, delta),
            "tp" = readRDS("sub_weekly_throughput.rds") %>%
                select(Zone_Group, Corridor, Date, vph, delta),
            "aogd" = readRDS("sub_weekly_aog_by_day.rds") %>%
                select(Zone_Group, Corridor, Date, aog, delta),
            "prd" = readRDS("sub_weekly_pr_by_day.rds") %>%
                select(Zone_Group, Corridor, Date, pr, delta),
            "qsd" = readRDS("sub_wqs.rds") %>%
                select(Zone_Group, Corridor, Date, qs_freq, delta),
            "sfd" = readRDS("sub_wsf.rds") %>%
                select(Zone_Group, Corridor, Date, sf_freq, delta),
            "sfo" = readRDS("sub_wsfo.rds") %>%
                select(Zone_Group, Corridor, Date, sf_freq, delta),
            "du" = readRDS("sub_weekly_detector_uptime.rds") %>%
                select(Zone_Group, Corridor, Date, uptime, delta),
            "cu" = readRDS("sub_weekly_comm_uptime.rds") %>%
                select(Zone_Group, Corridor, Date, uptime, delta),
            "pau" = readRDS("sub_weekly_pa_uptime.rds") %>%
                select(Zone_Group, Corridor, Date, uptime, delta),
            "cctv" = readRDS("sub_weekly_cctv_uptime.rds") %>%
                select(Zone_Group, Corridor, Date, uptime, delta)
        )
        sub$mo <- list(
            "vpd" = readRDS("sub_monthly_vpd.rds"),
            # "vph" = readRDS("sub_monthly_vph.rds"),
            "vphpa" = readRDS("sub_monthly_vph_peak.rds")$am,
            "vphpp" = readRDS("sub_monthly_vph_peak.rds")$pm,
            "papd" = readRDS("sub_monthly_papd.rds"),
            # "paph" = readRDS("sub_monthly_paph.rds"),
            "pd" = readRDS("sub_monthly_pd_by_day.rds"),
            "tp" = readRDS("sub_monthly_throughput.rds"),
            "aogd" = readRDS("sub_monthly_aog_by_day.rds"),
            "aogh" = readRDS("sub_monthly_aog_by_hr.rds"),
            "prd" = readRDS("sub_monthly_pr_by_day.rds"),
            "prh" = readRDS("sub_monthly_pr_by_hr.rds"),
            "qsd" = readRDS("sub_monthly_qsd.rds"),
            "qsh" = readRDS("sub_mqsh.rds"),
            "sfd" = readRDS("sub_monthly_sfd.rds"),
            "sfo" = readRDS("sub_monthly_sfo.rds"),
            "sfh" = readRDS("sub_msfh.rds"),
            "tti" = readRDS("sub_monthly_tti.rds"),
            "ttih" = readRDS("sub_monthly_tti_by_hr.rds"),
            "pti" = readRDS("sub_monthly_pti.rds"),
            "ptih" = readRDS("sub_monthly_pti_by_hr.rds"),
            "bi" = readRDS("sub_monthly_bi.rds"),
            "bih" = readRDS("sub_monthly_bi_by_hr.rds"),
            "spd" = readRDS("sub_monthly_spd.rds"),
            "spdh" = readRDS("sub_monthly_spd_by_hr.rds"),
            "du" = readRDS("sub_monthly_detector_uptime.rds"),
            "cu" = readRDS("sub_monthly_comm_uptime.rds"),
            "pau" = readRDS("sub_monthly_pa_uptime.rds"),
            "cctv" = readRDS("sub_monthly_cctv_uptime.rds"),
            "flash" = readRDS("sub_monthly_flash.rds"),
            "bpsi" = readRDS("sub_monthly_bpsi.rds"),
            "rsi" = readRDS("sub_monthly_rsi.rds"),
            "cri" = readRDS("sub_monthly_crash_rate_index.rds"),
            "kabco" = readRDS("sub_monthly_kabco_index.rds")
        )
        sub$qu <- list(
            "vpd" = get_quarterly(sub$mo$vpd, "vpd"),
            # "vph" = get_quarterly(sub$mo$vph, "vph"),
            "vphpa" = get_quarterly(sub$mo$vphpa, "vph"),
            "vphpp" = get_quarterly(sub$mo$vphpp, "vph"),
            "tp" = get_quarterly(sub$mo$tp, "vph"),
            "aogd" = get_quarterly(sub$mo$aogd, "aog", "vol"),
            "prd" = get_quarterly(sub$mo$prd, "pr", "vol"),
            "qsd" = get_quarterly(sub$mo$qsd, "qs_freq"),
            "sfd" = get_quarterly(sub$mo$sfd, "sf_freq"),
            "sfo" = get_quarterly(sub$mo$sfo, "sf_freq"),
            "du" = get_quarterly(sub$mo$du, "uptime"),
            "cu" = get_quarterly(sub$mo$cu, "uptime"),
            "pau" = get_quarterly(sub$mo$pau, "uptime"),
            "cctv" = get_quarterly(sub$mo$cctv, "uptime")
        )

        for (per in c("mo", "wk")) {
            for (tabl in names(sub[[per]])) {
                if ("data.frame" %in% class(sub[[per]][[tabl]])) {
                    sub[[per]][[tabl]] <- sub[[per]][[tabl]] %>%
                        mutate(Description = Corridor)
                }
            }
        }
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)



tryCatch(
    {
        sig <- list()
        sig$dy <- list(
            "vpd" = sigify(readRDS("daily_vpd.rds"), cor$dy$vpd, corridors) %>%
                select(Zone_Group, Corridor, Date, vpd, delta),
            "vphpa" = sigify(readRDS("daily_vph_peak.rds")$am, cor$dy$vphpa, corridors) %>%
                select(Zone_Group, Corridor, Date, vph, delta),
            "vphpp" = sigify(readRDS("daily_vph_peak.rds")$pm, cor$dy$vphpp, corridors) %>%
                select(Zone_Group, Corridor, Date, vph, delta),
            "papd" = sigify(readRDS("daily_papd.rds"), cor$dy$papd, corridors) %>%
                select(Zone_Group, Corridor, Date, papd, delta),
            "tp" = sigify(readRDS("daily_throughput.rds"), cor$dy$tp, corridors) %>%
                select(Zone_Group, Corridor, Date, vph, delta),
            "aogd" = sigify(readRDS("daily_aog.rds"), cor$dy$aogd, corridors) %>%
              select(Zone_Group, Corridor, Date, aog, delta),
            "prd" = sigify(readRDS("daily_pr.rds"), cor$dy$prd, corridors) %>%
              select(Zone_Group, Corridor, Date, pr, delta),
            "qsd" = sigify(readRDS("daily_qsd.rds"), cor$dy$qsd, corridors) %>%
              select(Zone_Group, Corridor, Date, qs_freq, delta),
            "sfd" = sigify(readRDS("daily_sfp.rds"), cor$dy$sfd, corridors) %>%
              select(Zone_Group, Corridor, Date, sf_freq, delta),
            "sfo" = sigify(readRDS("daily_sfo.rds"), cor$dy$sfo, corridors) %>%
              select(Zone_Group, Corridor, Date, sf_freq, delta),
            "du" = sigify(readRDS("avg_daily_detector_uptime.rds"), cor$dy$du, corridors) %>%
                select(Zone_Group, Corridor, Date, uptime, uptime.sb, uptime.pr),
            "cu" = sigify(readRDS("daily_comm_uptime.rds"), cor$dy$cu, corridors) %>%
                select(Zone_Group, Corridor, Date, uptime),
            "pau" = sigify(readRDS("daily_pa_uptime.rds"), cor$dy$pau, corridors) %>%
                select(Zone_Group, Corridor, Date, uptime),
            "cctv" = sigify(readRDS("daily_cctv_uptime.rds"), cor$dy$cctv, cam_config, identifier = "CameraID") %>%
                select(Zone_Group, Corridor, Description, Date, uptime, up),
            "ttyp" = readRDS("tasks_by_type.rds")$sig_daily,
            "tsub" = readRDS("tasks_by_subtype.rds")$sig_daily,
            "tpri" = readRDS("tasks_by_priority.rds")$sig_daily,
            "tsou" = readRDS("tasks_by_source.rds")$sig_daily,
            "tasks" = readRDS("tasks_all.rds")$sig_daily,
            "reported" = readRDS("tasks_all.rds")$sig_daily %>%
                transmute(Zone_Group, Corridor, Date, Reported, delta = NA),
            "resolved" = readRDS("tasks_all.rds")$sig_daily %>%
                transmute(Zone_Group, Corridor, Date, Resolved, delta = NA),
            "outstanding" = readRDS("tasks_all.rds")$sig_daily %>%
                transmute(Zone_Group, Corridor, Date, Outstanding, delta = NA)
        )
        sig$wk <- list(
            "vpd" = sigify(readRDS("weekly_vpd.rds"), cor$wk$vpd, corridors) %>%
                select(Zone_Group, Corridor, Date, vpd, delta),
            "vphpa" = sigify(readRDS("weekly_vph_peak.rds")$am, cor$wk$vphpa, corridors) %>%
                select(Zone_Group, Corridor, Date, vph, delta),
            "vphpp" = sigify(readRDS("weekly_vph_peak.rds")$pm, cor$wk$vphpp, corridors) %>%
                select(Zone_Group, Corridor, Date, vph, delta),
            "papd" = sigify(readRDS("weekly_papd.rds"), cor$wk$papd, corridors) %>%
                select(Zone_Group, Corridor, Date, papd, delta),
            # "paph" = sigify(readRDS("weekly_paph.rds"), cor$wk$paph, corridors),
            "pd" = sigify(readRDS("weekly_pd_by_day.rds"), cor$wk$pd, corridors) %>%
                select(Zone_Group, Corridor, Date, pd, delta),
            "tp" = sigify(readRDS("weekly_throughput.rds"), cor$wk$tp, corridors) %>%
                select(Zone_Group, Corridor, Date, vph, delta),
            "aogd" = sigify(readRDS("weekly_aog_by_day.rds"), cor$wk$aogd, corridors) %>%
                select(Zone_Group, Corridor, Date, aog, delta),
            "prd" = sigify(readRDS("weekly_pr_by_day.rds"), cor$wk$prd, corridors) %>%
                select(Zone_Group, Corridor, Date, pr, delta),
            "qsd" = sigify(readRDS("wqs.rds"), cor$wk$qsd, corridors) %>%
                select(Zone_Group, Corridor, Date, qs_freq, delta),
            "sfd" = sigify(readRDS("wsf.rds"), cor$wk$sfd, corridors) %>%
                select(Zone_Group, Corridor, Date, sf_freq, delta),
            "sfo" = sigify(readRDS("wsfo.rds"), cor$wk$sfo, corridors) %>%
                select(Zone_Group, Corridor, Date, sf_freq, delta),
            "du" = sigify(readRDS("weekly_detector_uptime.rds"), cor$wk$du, corridors) %>%
                select(Zone_Group, Corridor, Date, uptime, delta),
            "cu" = sigify(readRDS("weekly_comm_uptime.rds"), cor$wk$cu, corridors) %>%
                select(Zone_Group, Corridor, Date, uptime, delta),
            "pau" = sigify(readRDS("weekly_pa_uptime.rds"), cor$wk$pau, corridors) %>%
                select(Zone_Group, Corridor, Date, uptime, delta),
            "cctv" = sigify(readRDS("weekly_cctv_uptime.rds"), cor$wk$cctv, cam_config, identifier = "CameraID") %>%
                select(Zone_Group, Corridor, Description, Date, uptime, delta)
        )
        sig$mo <- list(
            "vpd" = sigify(readRDS("monthly_vpd.rds"), cor$mo$vpd, corridors) %>%
                select(-c(Name, ones)),
            "vphpa" = sigify(readRDS("monthly_vph_peak.rds")$am, cor$mo$vphpa, corridors) %>%
                select(-c(Name, ones)),
            "vphpp" = sigify(readRDS("monthly_vph_peak.rds")$pm, cor$mo$vphpp, corridors) %>%
                select(-c(Name, ones)),
            "papd" = sigify(readRDS("monthly_papd.rds"), cor$mo$papd, corridors) %>%
                select(-c(Name, ones)),
            # "paph" = sigify(readRDS("monthly_paph.rds"), cor$mo$paph, corridors) %>%
            #    select(-c(Name, ones)),
            "pd" = sigify(readRDS("monthly_pd_by_day.rds"), cor$mo$pd, corridors) %>%
                select(-c(Name, Events)),
            "tp" = sigify(readRDS("monthly_throughput.rds"), cor$mo$tp, corridors) %>%
                select(-c(Name, ones)),
            "aogd" = sigify(readRDS("monthly_aog_by_day.rds"), cor$mo$aogd, corridors) %>%
                select(-c(Name, vol)),
            "aogh" = sigify(readRDS("monthly_aog_by_hr.rds"), cor$mo$aogh, corridors) %>%
                select(-c(Name, vol)),
            "prd" = sigify(readRDS("monthly_pr_by_day.rds"), cor$mo$prd, corridors) %>%
                select(-c(Name, vol)),
            "prh" = sigify(readRDS("monthly_pr_by_hr.rds"), cor$mo$prh, corridors) %>%
                select(-c(Name, vol)),
            "qsd" = sigify(readRDS("monthly_qsd.rds"), cor$mo$qsd, corridors) %>%
                select(-c(Name, cycles)),
            "qsh" = sigify(readRDS("mqsh.rds"), cor$mo$qsh, corridors) %>%
                select(-c(Name, cycles)),
            "sfd" = sigify(readRDS("monthly_sfd.rds"), cor$mo$sfd, corridors) %>%
                select(-c(Name, cycles)),
            "sfh" = sigify(readRDS("msfh.rds"), cor$mo$sfh, corridors) %>%
                select(-c(Name, cycles)),
            "sfo" = sigify(readRDS("monthly_sfo.rds"), cor$mo$sfo, corridors) %>%
                select(-c(Name, cycles)),
            "tti" = data.frame(),
            "pti" = data.frame(),
            "bi" = data.frame(),
            "spd" = data.frame(),
            "du" = sigify(readRDS("monthly_detector_uptime.rds"), cor$mo$du, corridors) %>%
                select(Zone_Group, Corridor, Month, uptime, uptime.sb, uptime.pr, delta),
            "cu" = sigify(readRDS("monthly_comm_uptime.rds"), cor$mo$cu, corridors) %>%
                select(Zone_Group, Corridor, Month, uptime, delta),
            "pau" = sigify(readRDS("monthly_pa_uptime.rds"), cor$mo$pau, corridors) %>%
                select(Zone_Group, Corridor, Month, uptime, delta),
            "cctv" = sigify(readRDS("monthly_cctv_uptime.rds"), cor$mo$cctv, cam_config, identifier = "CameraID") %>%
                select(Zone_Group, Corridor, Description, Month, uptime, delta),
            "ttyp" = readRDS("tasks_by_type.rds")$sig_monthly,
            "tsub" = readRDS("tasks_by_subtype.rds")$sig_monthly,
            "tpri" = readRDS("tasks_by_priority.rds")$sig_monthly,
            "tsou" = readRDS("tasks_by_source.rds")$sig_monthly,
            "tasks" = readRDS("tasks_all.rds")$sig_monthly,
            "reported" = readRDS("tasks_all.rds")$sig_monthly %>%
                transmute(Zone_Group, Corridor, Month, Reported, delta = delta.rep),
            "resolved" = readRDS("tasks_all.rds")$sig_monthly %>%
                transmute(Zone_Group, Corridor, Month, Resolved, delta = delta.res),
            "outstanding" = readRDS("tasks_all.rds")$sig_monthly %>%
                transmute(Zone_Group, Corridor, Month, Outstanding, delta = delta.out),
            "over45" = readRDS("sig_tasks_by_date.rds") %>%
                transmute(Zone_Group, Corridor, Month, over45, delta = delta.over45),
            "mttr" = readRDS("sig_tasks_by_date.rds") %>%
                transmute(Zone_Group, Corridor, Month, mttr, delta = delta.mttr),
            "flash" = sigify(readRDS("monthly_flash.rds"), cor$mo$flash, corridors) %>%
                select(-c(Name, ones)),
            "cri" = sigify(readRDS("monthly_crash_rate_index.rds"), cor$mo$cri, corridors) %>%
                select(Zone_Group, Corridor, Month, cri, delta),
            "kabco" = sigify(readRDS("monthly_kabco_index.rds"), cor$mo$kabco, corridors) %>%
                select(Zone_Group, Corridor, Month, kabco, delta)
        )
        sig$qu <- list(
            "vpd" = get_quarterly(sig$mo$vpd, "vpd"),
            # "vph" = data.frame(), # get_quarterly(sig$mo$vph, "vph"),
            "vphpa" = get_quarterly(sig$mo$vphpa, "vph"),
            "vphpp" = get_quarterly(sig$mo$vphpp, "vph"),
            "papd" = get_quarterly(sig$mo$papd, "papd"),
            "pd" = get_quarterly(sig$mo$pd, "pd"),
            "tp" = get_quarterly(sig$mo$tp, "vph"),
            "aogd" = get_quarterly(sig$mo$aogd, "aog"),
            "prd" = get_quarterly(sig$mo$prd, "pr"),
            "qsd" = get_quarterly(sig$mo$qsd, "qs_freq"),
            "sfd" = get_quarterly(sig$mo$sfd, "sf_freq"),
            "sfo" = get_quarterly(sig$mo$sfo, "sf_freq"),
            "du" = get_quarterly(sig$mo$du, "uptime"),
            "cu" = get_quarterly(sig$mo$cu, "uptime"),
            "pau" = get_quarterly(sig$mo$pau, "uptime"),
            "cctv" = get_quarterly(sig$mo$cctv, "uptime"),
            "ttyp" = get_quarterly(sig$mo$ttyp, "Reported"),
            "tsub" = get_quarterly(sig$mo$tsub, "Reported"),
            "tpri" = get_quarterly(sig$mo$tpri, "Reported"),
            "tsou" = get_quarterly(sig$mo$tsou, "Reported"),
            "reported" = get_quarterly(sig$mo$tasks, "Reported"),
            "resolved" = get_quarterly(sig$mo$tasks, "Resolved"),
            "outstanding" = get_quarterly(sig$mo$tasks, "Outstanding", operation = "latest"),
            "over45" = get_quarterly(sig$mo$over45, "over45", operation = "latest"),
            "mttr" = get_quarterly(sig$mo$mttr, "mttr", operation = "latest")
        )

    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)




source("Health_Metrics.R")






print(glue("{Sys.time()} Upload to AWS [28 of 29 (sigops)]"))


print(glue("{Sys.time()} Write to Database [29 of 29 (sigops)]"))

source("write_sigops_to_db.R")

# Update Aurora Nightly
conn <- keep_trying(func = get_aurora_connection, n_tries = 5)
# recreate_database(conn)

tryCatch(
{
    append_to_database(
        conn, cor, "cor", calcs_start_date, report_start_date, report_end_date = NULL)
    append_to_database(
        conn, sub, "sub", calcs_start_date, report_start_date, report_end_date = NULL)
    append_to_database(
        conn, sig, "sig", calcs_start_date, report_start_date, report_end_date = NULL)
},
finally = {
    dbDisconnect(conn)
})



