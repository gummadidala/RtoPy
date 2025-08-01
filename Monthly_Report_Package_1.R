
# Monthly_Report_Package.R

source("Monthly_Report_Package_init.R")

# options(warn = 2)

# # DETECTOR UPTIME ###########################################################

print(glue("{Sys.time()} Vehicle Detector Uptime [1 of 29 (mark1)]"))

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
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)

# DAILY PEDESTRIAN PUSHBUTTON UPTIME ###############################################

print(glue("{Sys.time()} Ped Pushbutton Uptime [2 of 29 (mark1)]"))

tryCatch(
    {
        pau_start_date <- pmin(
            as_date(calcs_start_date),
            floor_date(as_date(report_end_date), unit = "months") - months(6)
        ) %>%
            format("%F")

        counts_ped_hourly <- s3_read_parquet_parallel(
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

        counts_ped_daily <- counts_ped_hourly %>%
            group_by(SignalID, Date, DOW, Week, Detector, CallPhase) %>%
            summarize(papd = sum(vol, na.rm = TRUE), .groups = "drop")

        papd <- counts_ped_daily
        paph <- counts_ped_hourly %>%
            rename(Hour = Timeperiod, paph = vol)
        rm(counts_ped_daily)
        rm(counts_ped_hourly)

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
        bad_detectors <- get_bad_ped_detectors(pau) %>%
            filter(Date >= calcs_start_date)

        if (nrow(bad_detectors)) {
            s3_upload_parquet_date_split(
                bad_detectors,
                bucket = conf$bucket,
                prefix = "bad_ped_detectors",
                table_name = "bad_ped_detectors",
                conf_athena = conf$athena,
                parallel = FALSE
            )
        }

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

print(glue("{Sys.time()} watchdog alerts [3 of 29 (mark1)]"))

tryCatch(
    {
        # -- Alerts: detector downtime --
        bad_det <- s3_read_parquet_parallel(
            "bad_detectors",
            start_date = today() - days(90),
            end_date = today() - days(1),
            bucket = conf$bucket
        ) %>%
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
                    as.character(glue("{trimws(ApproachDesc)} Lane {LaneNumber}"))
                )
            )

        # Zone_Group | Zone | Corridor | SignalID/CameraID | CallPhase | DetectorID | Date | Alert | Name

        s3write_using(
            FUN = write_parquet,
            bad_det,
            object = "mark/watchdog/bad_detectors.parquet",
            bucket = conf$bucket,
            opts = list(multipart = TRUE)
        )
        rm(bad_det)
        rm(det_config)

        # -- Alerts: pedestrian detector downtime --
        bad_ped <- s3_read_parquet_parallel(
            "bad_ped_detectors",
            start_date = today() - days(90),
            end_date = today() - days(1),
            bucket = conf$bucket
        )

        if (nrow(bad_ped)) {
            bad_ped %>%
                mutate(SignalID = factor(SignalID),
                       Detector = factor(Detector)) %>%

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
                          Alert = factor("Bad Ped Detection"),
                          Name = factor(Name)
                )
            s3write_using(
                FUN = write_parquet,
                bad_ped,
                object = "mark/watchdog/bad_ped_pushbuttons.parquet",
                bucket = conf$bucket
            )
        }
        rm(bad_ped)

        # -- Alerts: CCTV downtime --

        bad_cam <- lapply(
            seq(floor_date(today(), unit = "months") - months(6), today() - days(1), by = "1 month"),
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
            object = "mark/watchdog/bad_cameras.parquet",
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

print(glue("{Sys.time()} Daily Pedestrian Activations [4 of 29 (mark1)]"))

tryCatch(
    {
        weekly_papd <- get_weekly_papd(papd)

        # Group into corridors --------------------------------------------------------
        cor_weekly_papd <- get_cor_weekly_papd(weekly_papd, corridors)

        # Group into subcorridors --------------------------------------------------------
        sub_weekly_papd <- get_cor_weekly_papd(weekly_papd, subcorridors) %>%
            filter(!is.na(Corridor))

        # Monthly volumes for bar charts and % change ---------------------------------
        monthly_papd <- get_monthly_papd(papd)

        # Group into corridors
        cor_monthly_papd <- get_cor_monthly_papd(monthly_papd, corridors)

        # Group into subcorridors
        sub_monthly_papd <- get_cor_monthly_papd(monthly_papd, subcorridors) %>%
            filter(!is.na(Corridor))

        # Monthly % change from previous month by corridor ----------------------------
        addtoRDS(weekly_papd, "weekly_papd.rds", "papd", report_start_date, wk_calcs_start_date)
        addtoRDS(monthly_papd, "monthly_papd.rds", "papd", report_start_date, calcs_start_date)
        addtoRDS(cor_weekly_papd, "cor_weekly_papd.rds", "papd", report_start_date, wk_calcs_start_date)
        addtoRDS(cor_monthly_papd, "cor_monthly_papd.rds", "papd", report_start_date, calcs_start_date)
        addtoRDS(sub_weekly_papd, "sub_weekly_papd.rds", "papd", report_start_date, wk_calcs_start_date)
        addtoRDS(sub_monthly_papd, "sub_monthly_papd.rds", "papd", report_start_date, calcs_start_date)

        rm(papd)
        rm(weekly_papd)
        rm(monthly_papd)
        rm(cor_weekly_papd)
        rm(cor_monthly_papd)
        rm(sub_weekly_papd)
        rm(sub_monthly_papd)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)



# HOURLY PEDESTRIAN ACTIVATIONS ###############################################

print(glue("{Sys.time()} Hourly Pedestrian Activations [5 of 29 (mark1)]"))

tryCatch(
    {
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



# GET PEDESTRIAN DELAY ###################################################

print(glue("{Sys.time()} Pedestrian Delay [6 of 29 (mark1)]"))

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

        cor_weekly_pd_by_day <- get_cor_weekly_avg_by_day(weekly_pd_by_day, corridors, "pd", "Events")
        cor_monthly_pd_by_day <- get_cor_monthly_avg_by_day(monthly_pd_by_day, corridors, "pd", "Events")

        sub_weekly_pd_by_day <- get_cor_weekly_avg_by_day(weekly_pd_by_day, subcorridors, "pd", "Events") %>%
            filter(!is.na(Corridor))
        sub_monthly_pd_by_day <- get_cor_monthly_avg_by_day(monthly_pd_by_day, subcorridors, "pd", "Events") %>%
            filter(!is.na(Corridor))

        addtoRDS(weekly_pd_by_day, "weekly_pd_by_day.rds", "pd", report_start_date, wk_calcs_start_date)
        addtoRDS(monthly_pd_by_day, "monthly_pd_by_day.rds", "pd", report_start_date, calcs_start_date)
        addtoRDS(cor_weekly_pd_by_day, "cor_weekly_pd_by_day.rds", "pd", report_start_date, wk_calcs_start_date)
        addtoRDS(cor_monthly_pd_by_day, "cor_monthly_pd_by_day.rds", "pd", report_start_date, calcs_start_date)
        addtoRDS(sub_weekly_pd_by_day, "sub_weekly_pd_by_day.rds", "pd", report_start_date, wk_calcs_start_date)
        addtoRDS(sub_monthly_pd_by_day, "sub_monthly_pd_by_day.rds", "pd", report_start_date, calcs_start_date)

        rm(ped_delay)
        rm(daily_pd)
        rm(weekly_pd_by_day)
        rm(monthly_pd_by_day)
        rm(cor_weekly_pd_by_day)
        rm(cor_monthly_pd_by_day)
        rm(sub_weekly_pd_by_day)
        rm(sub_monthly_pd_by_day)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)


# GET COMMUNICATIONS UPTIME ###################################################

print(glue("{Sys.time()} Communication Uptime [7 of 29 (mark1)]"))

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
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)



# DAILY VOLUMES ###############################################################

print(glue("{Sys.time()} Daily Volumes [8 of 29 (mark1)]"))

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

        weekly_vpd <- get_weekly_vpd(vpd)

        # Group into corridors --------------------------------------------------------
        cor_weekly_vpd <- get_cor_weekly_vpd(weekly_vpd, corridors)
        # Subcorridors
        sub_weekly_vpd <-
            (get_cor_weekly_vpd(weekly_vpd, subcorridors) %>%
                filter(!is.na(Corridor)))

        # Monthly volumes for bar charts and % change ---------------------------------
        monthly_vpd <- get_monthly_vpd(vpd)

        # Group into corridors --------------------------------------------------------
        cor_monthly_vpd <- get_cor_monthly_vpd(monthly_vpd, corridors)
        cor_weekly_vpd <- get_cor_weekly_vpd(weekly_vpd, corridors)

        # Subcorridors
        sub_weekly_vpd <-
            get_cor_weekly_vpd(weekly_vpd, subcorridors) %>%
                filter(!is.na(Corridor))
        sub_monthly_vpd <-
            get_cor_monthly_vpd(monthly_vpd, subcorridors) %>%
                filter(!is.na(Corridor))

        # Monthly % change from previous month by corridor ----------------------------
        addtoRDS(weekly_vpd, "weekly_vpd.rds", "vpd", report_start_date, wk_calcs_start_date)
        addtoRDS(monthly_vpd, "monthly_vpd.rds", "vpd", report_start_date, calcs_start_date)
        addtoRDS(cor_weekly_vpd, "cor_weekly_vpd.rds", "vpd", report_start_date, wk_calcs_start_date)
        addtoRDS(cor_monthly_vpd, "cor_monthly_vpd.rds", "vpd", report_start_date, calcs_start_date)
        addtoRDS(sub_weekly_vpd, "sub_weekly_vpd.rds", "vpd", report_start_date, wk_calcs_start_date)
        addtoRDS(sub_monthly_vpd, "sub_monthly_vpd.rds", "vpd", report_start_date, calcs_start_date)

        rm(vpd)
        rm(weekly_vpd)
        rm(monthly_vpd)
        rm(cor_weekly_vpd)
        rm(cor_monthly_vpd)
        rm(sub_weekly_vpd)
        rm(sub_monthly_vpd)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)



# HOURLY VOLUMES ##############################################################

print(glue("{Sys.time()} Hourly Volumes [9 of 29 (mark1)]"))

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

        weekly_vph <- get_weekly_vph(vph)
        weekly_vph_peak <- get_weekly_vph_peak(weekly_vph)

        # Group into corridors --------------------------------------------------------
        cor_weekly_vph <- get_cor_weekly_vph(weekly_vph, corridors)
        cor_weekly_vph_peak <- get_cor_weekly_vph_peak(cor_weekly_vph)

        # Group into Subcorridors --------------------------------------------------------
        sub_weekly_vph <- get_cor_weekly_vph(weekly_vph, subcorridors) %>%
            filter(!is.na(Corridor))
        sub_weekly_vph_peak <- get_cor_weekly_vph_peak(sub_weekly_vph) %>%
            map(~ filter(., !is.na(Corridor)))

        monthly_vph <- get_monthly_vph(vph)
        monthly_vph_peak <- get_monthly_vph_peak(monthly_vph)

        # Hourly volumes by Corridor --------------------------------------------------
        cor_monthly_vph <- get_cor_monthly_vph(monthly_vph, corridors)
        cor_monthly_vph_peak <- get_cor_monthly_vph_peak(cor_monthly_vph)

        # Hourly volumes by Subcorridor --------------------------------------------------
        sub_monthly_vph <- get_cor_monthly_vph(monthly_vph, subcorridors) %>%
            filter(!is.na(Corridor))
        sub_monthly_vph_peak <- get_cor_monthly_vph_peak(sub_monthly_vph) %>%
            map(~ filter(., !is.na(Corridor)))

        addtoRDS(weekly_vph, "weekly_vph.rds", "vph", report_start_date, wk_calcs_start_date)
        addtoRDS(monthly_vph, "monthly_vph.rds", "vph", report_start_date, calcs_start_date)
        addtoRDS(cor_weekly_vph, "cor_weekly_vph.rds", "vph", report_start_date, wk_calcs_start_date)
        addtoRDS(cor_monthly_vph, "cor_monthly_vph.rds", "vph", report_start_date, calcs_start_date)
        addtoRDS(sub_weekly_vph, "sub_weekly_vph.rds", "vph", report_start_date, wk_calcs_start_date)
        addtoRDS(sub_monthly_vph, "sub_monthly_vph.rds", "vph", report_start_date, calcs_start_date)

        addtoRDS(weekly_vph_peak, "weekly_vph_peak.rds", "vph", report_start_date, wk_calcs_start_date)
        addtoRDS(monthly_vph_peak, "monthly_vph_peak.rds", "vph", report_start_date, calcs_start_date)
        addtoRDS(cor_weekly_vph_peak, "cor_weekly_vph_peak.rds", "vph", report_start_date, wk_calcs_start_date)
        addtoRDS(cor_monthly_vph_peak, "cor_monthly_vph_peak.rds", "vph", report_start_date, calcs_start_date)
        addtoRDS(sub_weekly_vph_peak, "sub_weekly_vph_peak.rds", "vph", report_start_date, wk_calcs_start_date)
        addtoRDS(sub_monthly_vph_peak, "sub_monthly_vph_peak.rds", "vph", report_start_date, calcs_start_date)

        rm(vph)
        rm(weekly_vph)
        rm(monthly_vph)
        rm(cor_weekly_vph)
        rm(sub_weekly_vph)
        rm(cor_monthly_vph)
        rm(sub_monthly_vph)
        rm(weekly_vph_peak)
        rm(monthly_vph_peak)
        rm(cor_weekly_vph_peak)
        rm(cor_monthly_vph_peak)
        rm(sub_weekly_vph_peak)
        rm(sub_monthly_vph_peak)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)






# DAILY THROUGHPUT ############################################################

print(glue("{Sys.time()} Daily Throughput [10 of 29 (mark1)]"))

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

        weekly_throughput <- get_weekly_thruput(throughput)
        monthly_throughput <- get_monthly_thruput(throughput)

        # Weekly throughput - Group into corridors ---------------------------------
        cor_weekly_throughput <- get_cor_weekly_thruput(weekly_throughput, corridors)
        sub_weekly_throughput <- get_cor_weekly_thruput(weekly_throughput, subcorridors) %>%
            filter(!is.na(Corridor))

        # Monthly throughput - Group into corridors
        cor_monthly_throughput <- get_cor_monthly_thruput(monthly_throughput, corridors)
        sub_monthly_throughput <- get_cor_monthly_thruput(monthly_throughput, subcorridors) %>%
            filter(!is.na(Corridor))

        addtoRDS(weekly_throughput, "weekly_throughput.rds", "vph", report_start_date, wk_calcs_start_date)
        addtoRDS(monthly_throughput, "monthly_throughput.rds", "vph", report_start_date, calcs_start_date)
        addtoRDS(cor_weekly_throughput, "cor_weekly_throughput.rds", "vph", report_start_date, wk_calcs_start_date)
        addtoRDS(cor_monthly_throughput, "cor_monthly_throughput.rds", "vph", report_start_date, calcs_start_date)
        addtoRDS(sub_weekly_throughput, "sub_weekly_throughput.rds", "vph", report_start_date, wk_calcs_start_date)
        addtoRDS(sub_monthly_throughput, "sub_monthly_throughput.rds", "vph", report_start_date, calcs_start_date)

        rm(throughput)
        rm(weekly_throughput)
        rm(monthly_throughput)
        rm(cor_weekly_throughput)
        rm(cor_monthly_throughput)
        rm(sub_weekly_throughput)
        rm(sub_monthly_throughput)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)




# DAILY ARRIVALS ON GREEN #####################################################

print(glue("{Sys.time()} Daily AOG [11 of 29 (mark1)]"))

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

        daily_aog <- get_daily_aog(aog)
        weekly_aog_by_day <- get_weekly_aog_by_day(aog)
        monthly_aog_by_day <- get_monthly_aog_by_day(aog)

        cor_weekly_aog_by_day <- get_cor_weekly_aog_by_day(weekly_aog_by_day, corridors)
        cor_monthly_aog_by_day <- get_cor_monthly_aog_by_day(monthly_aog_by_day, corridors)

        sub_weekly_aog_by_day <- get_cor_weekly_aog_by_day(weekly_aog_by_day, subcorridors) %>%
            filter(!is.na(Corridor))
        sub_monthly_aog_by_day <- get_cor_monthly_aog_by_day(monthly_aog_by_day, subcorridors) %>%
            filter(!is.na(Corridor))

        addtoRDS(weekly_aog_by_day, "weekly_aog_by_day.rds", "aog", report_start_date, wk_calcs_start_date)
        addtoRDS(monthly_aog_by_day, "monthly_aog_by_day.rds", "aog", report_start_date, calcs_start_date)
        addtoRDS(cor_weekly_aog_by_day, "cor_weekly_aog_by_day.rds", "aog", report_start_date, wk_calcs_start_date)
        addtoRDS(cor_monthly_aog_by_day, "cor_monthly_aog_by_day.rds", "aog", report_start_date, calcs_start_date)
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

print(glue("{Sys.time()} Hourly AOG [12 of 29 (mark1)]"))

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
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)



# DAILY PROGRESSION RATIO #####################################################

print(glue("{Sys.time()} Daily Progression Ratio [13 of 29 (mark1)]"))

tryCatch(
    {
        weekly_pr_by_day <- get_weekly_pr_by_day(aog)
        monthly_pr_by_day <- get_monthly_pr_by_day(aog)

        cor_weekly_pr_by_day <- get_cor_weekly_pr_by_day(weekly_pr_by_day, corridors)
        cor_monthly_pr_by_day <- get_cor_monthly_pr_by_day(monthly_pr_by_day, corridors)

        sub_weekly_pr_by_day <- get_cor_weekly_pr_by_day(weekly_pr_by_day, subcorridors) %>%
            filter(!is.na(Corridor))
        sub_monthly_pr_by_day <- get_cor_monthly_pr_by_day(monthly_pr_by_day, subcorridors) %>%
            filter(!is.na(Corridor))

        addtoRDS(weekly_pr_by_day, "weekly_pr_by_day.rds", "pr", report_start_date, wk_calcs_start_date)
        addtoRDS(monthly_pr_by_day, "monthly_pr_by_day.rds", "pr", report_start_date, calcs_start_date)
        addtoRDS(cor_weekly_pr_by_day, "cor_weekly_pr_by_day.rds", "pr", report_start_date, wk_calcs_start_date)
        addtoRDS(cor_monthly_pr_by_day, "cor_monthly_pr_by_day.rds", "pr", report_start_date, calcs_start_date)
        addtoRDS(sub_weekly_pr_by_day, "sub_weekly_pr_by_day.rds", "pr", report_start_date, wk_calcs_start_date)
        addtoRDS(sub_monthly_pr_by_day, "sub_monthly_pr_by_day.rds", "pr", report_start_date, calcs_start_date)

        rm(weekly_pr_by_day)
        rm(monthly_pr_by_day)
        rm(cor_weekly_pr_by_day)
        rm(cor_monthly_pr_by_day)
        rm(sub_weekly_pr_by_day)
        rm(sub_monthly_pr_by_day)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)



# HOURLY PROGESSION RATIO ####################################################

print(glue("{Sys.time()} Hourly Progression Ratio [14 of 29 (mark1)]"))

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
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)





# DAILY SPLIT FAILURES #####################################################

tryCatch(
    {
        print(glue("{Sys.time()} Daily Split Failures [15 of 29 (mark1)]"))

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

        weekly_sf_by_day <- get_weekly_avg_by_day(
            sfp, "sf_freq", "cycles",
            peak_only = FALSE
        )
        weekly_sfo_by_day <- get_weekly_avg_by_day(
            sfo, "sf_freq", "cycles",
            peak_only = FALSE
        )

        cor_weekly_sf_by_day <- get_cor_weekly_sf_by_day(weekly_sf_by_day, corridors)
        cor_weekly_sfo_by_day <- get_cor_weekly_sf_by_day(weekly_sfo_by_day, corridors)

        sub_weekly_sf_by_day <- get_cor_weekly_sf_by_day(weekly_sf_by_day, subcorridors) %>%
            filter(!is.na(Corridor))
        sub_weekly_sfo_by_day <- get_cor_weekly_sf_by_day(weekly_sfo_by_day, subcorridors) %>%
            filter(!is.na(Corridor))

        monthly_sf_by_day <- get_monthly_avg_by_day(
            sfp, "sf_freq", "cycles",
            peak_only = FALSE
        )
        monthly_sfo_by_day <- get_monthly_avg_by_day(
            sfo, "sf_freq", "cycles",
            peak_only = FALSE
        )

        cor_monthly_sf_by_day <- get_cor_monthly_sf_by_day(monthly_sf_by_day, corridors)
        cor_monthly_sfo_by_day <- get_cor_monthly_sf_by_day(monthly_sfo_by_day, corridors)

        sub_monthly_sf_by_day <- get_cor_monthly_sf_by_day(monthly_sf_by_day, subcorridors) %>%
            filter(!is.na(Corridor))
        sub_monthly_sfo_by_day <- get_cor_monthly_sf_by_day(monthly_sfo_by_day, subcorridors) %>%
            filter(!is.na(Corridor))


        addtoRDS(weekly_sf_by_day, "wsf.rds", "sf_freq", report_start_date, wk_calcs_start_date)
        addtoRDS(monthly_sf_by_day, "monthly_sfd.rds", "sf_freq", report_start_date, calcs_start_date)
        addtoRDS(cor_weekly_sf_by_day, "cor_wsf.rds", "sf_freq", report_start_date, wk_calcs_start_date)
        addtoRDS(cor_monthly_sf_by_day, "cor_monthly_sfd.rds", "sf_freq", report_start_date, calcs_start_date)
        addtoRDS(sub_weekly_sf_by_day, "sub_wsf.rds", "sf_freq", report_start_date, wk_calcs_start_date)
        addtoRDS(sub_monthly_sf_by_day, "sub_monthly_sfd.rds", "sf_freq", report_start_date, calcs_start_date)

        addtoRDS(weekly_sfo_by_day, "wsfo.rds", "sf_freq", report_start_date, wk_calcs_start_date)
        addtoRDS(monthly_sfo_by_day, "monthly_sfo.rds", "sf_freq", report_start_date, calcs_start_date)
        addtoRDS(cor_weekly_sfo_by_day, "cor_wsfo.rds", "sf_freq", report_start_date, wk_calcs_start_date)
        addtoRDS(cor_monthly_sfo_by_day, "cor_monthly_sfo.rds", "sf_freq", report_start_date, calcs_start_date)
        addtoRDS(sub_weekly_sfo_by_day, "sub_wsfo.rds", "sf_freq", report_start_date, wk_calcs_start_date)
        addtoRDS(sub_monthly_sfo_by_day, "sub_monthly_sfo.rds", "sf_freq", report_start_date, calcs_start_date)

        rm(sfp)
        rm(sfo)
        rm(weekly_sf_by_day)
        rm(monthly_sf_by_day)
        rm(cor_weekly_sf_by_day)
        rm(cor_monthly_sf_by_day)
        rm(sub_weekly_sf_by_day)
        rm(sub_monthly_sf_by_day)

        rm(weekly_sfo_by_day)
        rm(monthly_sfo_by_day)
        rm(cor_weekly_sfo_by_day)
        rm(cor_monthly_sfo_by_day)
        rm(sub_weekly_sfo_by_day)
        rm(sub_monthly_sfo_by_day)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)

# HOURLY SPLIT FAILURES #######################################################

print(glue("{Sys.time()} Hourly Split Failures [16 of 29 (mark1)]"))

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
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)



# DAILY QUEUE SPILLBACK #######################################################

print(glue("{Sys.time()} Daily Queue Spillback [17 of 29 (mark1)]"))

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

        wqs <- get_weekly_qs_by_day(qs)
        cor_wqs <- get_cor_weekly_qs_by_day(wqs, corridors)
        sub_wqs <- get_cor_weekly_qs_by_day(wqs, subcorridors) %>%
            filter(!is.na(Corridor))

        monthly_qsd <- get_monthly_qs_by_day(qs)
        cor_monthly_qsd <- get_cor_monthly_qs_by_day(monthly_qsd, corridors)
        sub_monthly_qsd <- get_cor_monthly_qs_by_day(monthly_qsd, subcorridors) %>%
            filter(!is.na(Corridor))

        addtoRDS(wqs, "wqs.rds", "qs_freq", report_start_date, wk_calcs_start_date)
        addtoRDS(monthly_qsd, "monthly_qsd.rds", "qs_freq", report_start_date, calcs_start_date)
        addtoRDS(cor_wqs, "cor_wqs.rds", "qs_freq", report_start_date, wk_calcs_start_date)
        addtoRDS(cor_monthly_qsd, "cor_monthly_qsd.rds", "qs_freq", report_start_date, calcs_start_date)
        addtoRDS(sub_wqs, "sub_wqs.rds", "qs_freq", report_start_date, wk_calcs_start_date)
        addtoRDS(sub_monthly_qsd, "sub_monthly_qsd.rds", "qs_freq", report_start_date, calcs_start_date)

        rm(wqs)
        rm(monthly_qsd)
        rm(cor_wqs)
        rm(cor_monthly_qsd)
        rm(sub_wqs)
        rm(sub_monthly_qsd)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)




# HOURLY QUEUE SPILLBACK ######################################################

print(glue("{Sys.time()} Hourly Queue Spillback [18 of 29 (mark1)]"))

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
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)



# TRAVEL TIME AND BUFFER TIME INDEXES #########################################

print(glue("{Sys.time()} Travel Time Indexes [19 of 29 (mark1)]"))

tryCatch(
    {

        # ------- Corridor Travel Time Metrics ------- #

        tt <- s3_read_parquet_parallel(
            bucket = conf$bucket,
            table_name = "cor_travel_time_metrics_1hr",
            start_date = calcs_start_date,
            end_date = report_end_date
        ) %>%
            mutate(
                Corridor = factor(Corridor)
            ) %>%
            left_join(
                distinct(all_corridors, Zone_Group, Zone, Corridor), relationship = "many-to-many"
	    ) %>%
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
            left_join(
                distinct(corridors, Zone_Group, Zone), by = ("Zone"), relationship = "many-to-many"
            )

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
            left_join(distinct(subcorridors, Zone_Group, Zone))

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
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)



# CCTV UPTIME From 511 and Encoders

print(glue("{Sys.time()} CCTV Uptimes [20 of 29 (mark1)]"))

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

print(glue("{Sys.time()} TEAMS [21 of 29 (mark1)]"))

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


print(glue("{Sys.time()} User Delay Costs [22 of 29 (mark1)]"))


tryCatch(
    {
        months <- seq(ymd(report_start_date), ymd(report_end_date), by = "1 month")
        udc <- lapply(months, function(yyyymmdd) {
            obj <- glue("mark/user_delay_costs/date={yyyymmdd}/user_delay_costs_{yyyymmdd}.parquet")
            if (nrow(aws.s3::get_bucket_df(conf$bucket, prefix = obj)) > 0) {
                s3read_using(
                    read_parquet,
                    bucket = conf$bucket,
                    object = obj
                ) %>%
                    filter(!is.na(date)) %>%
                    convert_to_utc() %>%
                    transmute(
                        Zone = zone,
                        Corridor = corridor,
                        analysis_month = ymd(yyyymmdd),
                        month_hour = as_date(date),
                        month_hour = month_hour - days(day(month_hour)) + days(1),
                        Month = date(floor_date(month_hour, unit = "months")),
                        delay_cost = combined.delay_cost
                    )
            }
        }) %>% bind_rows()

        hourly_udc <- udc %>%
            group_by(Zone, Corridor, Month, month_hour) %>% # year_month
            summarize(delay_cost = sum(delay_cost, na.rm = TRUE), .groups = "drop")


        months <- unique(udc$analysis_month)
        udc_trend_table_list <- lapply(months[2:length(months)], function(current_month) {
            last_month <- current_month - months(1)
            last_year <- current_month - years(1)

            current_month_col <- as.name(format(current_month, "%B %Y"))
            last_month_col <- as.name(format(last_month, "%B %Y"))
            last_year_col <- as.name(format(last_year, "%B %Y"))


            udc_trend_table_list <- udc %>%
                filter(
                    analysis_month <= current_month,
                    Month %in% c(current_month, last_month, last_year)
                ) %>%
                group_by(
                    Zone, Corridor, Month
                ) %>%
                summarize(
                    delay_cost = sum(delay_cost, na.rm = TRUE),
                    .groups = "drop"
                ) %>%
                mutate(
                    Month = format(Month, "%B %Y")
                ) %>%
                spread(
                    Month, delay_cost
                ) %>%
                mutate(
                    Month = current_month,
                    `Month-over-Month` = (!!current_month_col - !!last_month_col) / !!last_month_col,
                    `Year-over-Year` = (!!current_month_col - !!last_year_col) / !!last_year_col
                ) %>%
                select(
                    Zone, Corridor, Month,
                    !!last_year_col, `Year-over-Year`,
                    !!last_month_col, `Month-over-Month`,
                    !!current_month_col
                )
        })
        names(udc_trend_table_list) <- months[2:length(months)]


        saveRDS(hourly_udc, "hourly_udc.rds")
        saveRDS(udc_trend_table_list, "udc_trend_table_list.rds")
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)



# Flash Events ###############################################################

print(glue("{Sys.time()} Flash Events [23 of 29 (mark1)]"))

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
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)



# BIKE/PED SAFETY INDEX #######################################################

print(glue("{Sys.time()} Bike/Ped Safety Index [24 of 29 (mark1)]"))

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
            by = "Corridor"
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
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)



# RELATIVE SPEED INDEX ########################################################

print(glue("{Sys.time()} Relative Speed Index [25 of 29 (mark1)]"))

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
            by = "Corridor"
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

print(glue("{Sys.time()} Crash Indices [26 of 29 (mark1)]"))

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
            by = c("SignalID", "Month")
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

        rm(crashes)
        rm(monthly_36mo_crashes)
        rm(monthly_crashes)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)
closeAllConnections()
