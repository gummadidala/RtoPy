
get_counts <- function(df, det_config, units = "hours", date_, event_code = 82, TWR_only = FALSE) {

    if (lubridate::wday(date_, label = TRUE) %in% c("Tue", "Wed", "Thu") || (TWR_only == FALSE)) {

        df <- df %>%
            filter(eventcode == event_code)

        # Group by hour using Athena/Presto SQL
        if (units == "hours") {
            df <- df %>%
                group_by(timeperiod = date_trunc('hour', timestamp),
                         signalid,
                         eventparam)

            # Group by 15 minute interval using Athena/Presto SQL
        } else if (units == "15min") {
            df <- df %>%
                mutate(timeperiod = date_trunc('minute', timestamp)) %>%
                group_by(timeperiod = date_add('second',
                                               as.integer(-1 * mod(to_unixtime(timeperiod), 15*60)),
                                               timeperiod),
                         signalid,
                         eventparam)
        }

        df <- df %>%
            count() %>%
            ungroup() %>%
            collect() %>%
            transmute(Timeperiod = as_datetime(timeperiod),
                      SignalID = factor(signalid),
                      Detector = factor(eventparam),
                      vol = as.integer(n)) %>%
            left_join(det_config, by = c("SignalID", "Detector")) %>%

            dplyr::select(SignalID, Timeperiod, Detector, CallPhase, vol) %>%
            mutate(SignalID = factor(SignalID),
                   Detector = factor(Detector))
        df
    } else {
        data.frame()
    }
}


get_counts2 <- function(date_, bucket, conf_athena, uptime = TRUE, counts = TRUE) {

    atspm_query <- sql(glue(paste(
        "select distinct timestamp, signalid, eventcode, eventparam",
        "from {conf_athena$database}.{conf_athena$atspm_table}",
        "where date = '{date_}'")))

    if (counts == TRUE) {
        det_config <- get_det_config(date_) %>%
            transmute(SignalID = factor(SignalID),
                      Detector = factor(Detector),
                      CallPhase = factor(CallPhase))

        ped_config <- get_ped_config(date_) %>%
            transmute(SignalID = factor(SignalID),
                      Detector = factor(Detector),
                      CallPhase = factor(CallPhase))
    }

#     athena <- get_athena_connection()
#     df <- tbl(athena, "atspm2") %>%
#         filter(date == date_) %>%
#         select(signalid, timestamp, eventcode, eventparam)
#
#     mssql <- get_atspm_connection(conf$atspm)
#     df <- tbl(mssql, "Controller_Event_Log") %>%
#         filter(timestamp >= date_, timestamp < date_ + days(1)) %>%
#     select(signalid=SignalID, timestamp=Timestamp, eventcode=EventCode, eventparam=EventParam)

    print(paste("-- Get Counts for:", date_, "-----------"))

    if (uptime == TRUE) {


        # Uptime is updated as of July 2020
        # Takes comm codes from MaxView EventLog table (500,501,502,503,504)
        # These codes show comm attempts, failures and percent failures every 5 minutes.
        # If failure rate is less than 100% we say we have comm at that time period
        # We average up the periods with comms to get the uptime for the day.
        # Typically (# periods with some comm)/(total number of periods) = % uptime
        #
        # When we start pulling from the ATSPM database for events, we'll have
        # to pull these codes separately since they only appear in MaxView's EventLog
        #
        conn <- get_athena_connection(conf_athena)

        print(glue("Communications uptime {date_}"))
        comm_uptime <- tbl(conn, atspm_query) %>%
            filter(eventcode %in% c(502,503)) %>%
            collect() %>%
            group_by(signalid) %>%
            summarize(
                success_rate = sum(eventparam[eventcode==502]<100, na.rm=TRUE),
                denom = sum(eventparam[eventcode==502]>-1, na.rm=TRUE),
                response_ms = mean(eventparam[eventcode==503], na.rm=TRUE),
                .groups = "drop") %>%
            mutate(SignalID = factor(signalid),
                   CallPhase = factor(0),
                   uptime = success_rate/denom,
                   Date_Hour = as_datetime(date_),
                   Date = date(date_),
                   DOW = wday(date_),
                   Week = week(date_)) %>%
            dplyr::select(SignalID, CallPhase, Date, Date_Hour, DOW, Week, uptime, response_ms)

        dbDisconnect(conn)

        s3_upload_parquet(comm_uptime, date_,
                          fn = glue("cu_{date_}"),
                          bucket = bucket,
                          table_name = "comm_uptime",
                          conf_athena = conf_athena)
    }

    if (counts == TRUE) {

        counts_1hr_fn <- glue("counts_1hr_{date_}")
        counts_15min_fn <- glue("counts_15min_TWR_{date_}")

        counts_ped_1hr_fn <- glue("counts_ped_1hr_{date_}")
        counts_ped_15min_fn <- glue("counts_ped_15min_{date_}")

        filtered_counts_1hr_fn <- glue("filtered_counts_1hr_{date_}")
        filtered_counts_15min_fn <- glue("filtered_counts_15min_{date_}")

        conn <- get_athena_connection(conf_athena)

        # get 1hr counts
        print("1-hour counts")
        counts_1hr <- get_counts(
            tbl(conn, atspm_query),
            det_config,
            "hours",
            date_,
            event_code = 82,
            TWR_only = FALSE
        ) %>%
            arrange(SignalID, Detector, Timeperiod)

        dbDisconnect(conn)

        s3_upload_parquet(counts_1hr, date_,
                          fn = counts_1hr_fn,
                          bucket = bucket,
                          table_name = "counts_1hr",
                          conf_athena = conf_athena)

        conn <- get_athena_connection(conf_athena)

        print("1-hour filtered counts")
        filtered_counts_1hr <- get_filtered_counts_3stream(
            date_,
            counts_1hr,
            interval = "1 hour")

        filtered_counts_1hr <- filtered_counts_hack(filtered_counts_1hr)

        s3_upload_parquet(filtered_counts_1hr, date_,
                          fn = filtered_counts_1hr_fn,
                          bucket = bucket,
                          table_name = "filtered_counts_1hr",
                          conf_athena = conf_athena)
        rm(counts_1hr)

        # BAD DETECTORS
        print(glue("detectors: {date_}"))
        get_bad_detectors(filtered_counts_1hr) %>%
            s3_upload_parquet_date_split(
                bucket = conf$bucket,
                prefix = "bad_detectors",
                table_name = "bad_detectors",
                conf_athena = conf$athena
            )

        # # DAILY DETECTOR UPTIME
        print(glue("ddu: {date_}"))
        get_daily_detector_uptime(filtered_counts_1hr) %>%
            bind_rows() %>%
            s3_upload_parquet_date_split(
                bucket = conf$bucket,
                prefix = "ddu",
                table_name = "detector_uptime_pd",
                conf_athena = conf$athena
            )

        dbDisconnect(conn)

        conn <- get_athena_connection(conf_athena)

        # get 1hr ped counts
        print("1-hour pedestrian actuation counts")
        counts_ped_1hr <- get_counts(
            tbl(conn, atspm_query),
            ped_config,
            "hours",
            date_,
            event_code = 90,
            TWR_only = FALSE
        ) %>%
            arrange(SignalID, Detector, Timeperiod)

        dbDisconnect(conn)

        s3_upload_parquet(counts_ped_1hr, date_,
                          fn = counts_ped_1hr_fn,
                          bucket = bucket,
                          table_name = "counts_ped_1hr",
                          conf_athena = conf_athena)

        # PAPD - pedestrian activations per day
        print("papd")
        get_vpd(counts_ped_1hr, mainline_only = FALSE) %>%
            ungroup() %>%
            rename(papd = vpd) %>%
            s3_upload_parquet_date_split(
                bucket = conf$bucket,
                prefix = "papd",
                table_name = "ped_actuations_pd",
                conf_athena = conf$athena
        )

        # PAPH - pedestrian activations per hour
        print("paph")
        get_vph(counts_ped_1hr, interval = "1 hour", mainline_only = FALSE) %>%
            rename(paph = vph) %>%
            s3_upload_parquet_date_split(
                bucket = conf$bucket,
                prefix = "paph",
                table_name = "ped_actuations_ph",
                conf_athena = conf$athena
        )

        rm(counts_ped_1hr)

        conn <- get_athena_connection(conf_athena)

        # get 15min counts
        print("15-minute counts")
        counts_15min <- get_counts(
            tbl(conn, atspm_query),
            det_config,
            "15min",
            date_,
            event_code = 82,
            TWR_only = FALSE
        ) %>%
            arrange(SignalID, Detector, Timeperiod)

        dbDisconnect(conn)

        s3_upload_parquet(counts_15min, date_,
                          fn = counts_15min_fn,
                          bucket = bucket,
                          table_name = "counts_15min",
                          conf_athena = conf_athena)

        # get 15min filtered counts
        print("15-minute filtered counts")
        get_filtered_counts_3stream(
            date_,
            counts_15min,
            interval = "15 min") %>%
            s3_upload_parquet(
                date_,
                fn = filtered_counts_15min_fn,
                bucket = bucket,
                table_name = "filtered_counts_15min",
                conf_athena = conf_athena)

        conn <- get_athena_connection(conf_athena)

        # get 15min ped counts
        print("15-minute pedestrian activation counts")
        counts_ped_15min <- get_counts(
            tbl(conn, atspm_query),
            ped_config,
            "15min",
            date_,
            event_code = 90,
            TWR_only = FALSE
        ) %>%
            arrange(SignalID, Detector, Timeperiod)

        dbDisconnect(conn)

        s3_upload_parquet(counts_ped_15min, date_,
                          fn = counts_ped_15min_fn,
                          bucket = bucket,
                          table_name = "counts_ped_15min",
                          conf_athena = conf_athena)

        # PA15 - pedestrian activations per 15min
        print("pa15")
        get_vph(counts_ped_15min, interval = "15 min", mainline_only = FALSE) %>%
            rename(pa15 = vph) %>%
            s3_upload_parquet_date_split(
                bucket = conf$bucket,
                prefix = "pa15",
                table_name = "ped_actuations_15min",
                conf_athena = conf$athena
            )

        rm(counts_ped_15min)
        return(TRUE)
    }
}



# New version of get_filtered_counts from 4/2/2020.
#  Considers three factors separately and fails a detector if any are flagged:
#  Streak of 5 "flatlined" hours,
#  Five hours exceeding max volume,
#  Mean Absolute Deviation greater than a threshold
get_filtered_counts_3stream <- function(date_, counts, interval = "1 hour") { # interval (e.g., "1 hour", "15 min")

    if (interval == "1 hour") {
        max_volume <- 1200  # 1000 - increased on 3/19/2020 (down to 2000 on 3/31) to accommodate mainline ramp meters
        max_volume_mainline <- 3000 # New on 5/21/2020
        max_delta <- 500
        max_abs_delta <- 200  # 200 - increased on 3/24 to make less stringent
        max_abs_delta_mainline <- 500 # New on 5/21/2020
        max_flat <- 5
        hi_vol_pers <- 5
    } else if (interval == "15 min") {
        max_volume <- 300  #250 - increased on 3/19/2020 (down to 500 on 3/31) to accommodate mainline ramp meter detectors
        max_volume_mainline <- 750 # New on 5/21/2020
        max_delta <- 125
        max_abs_delta <- 50  # 50 - increased on 3/24 to make less stringent
        max_abs_delta_mainline <- 125
        max_flat <- 20
        hi_vol_pers <- 20
    } else {
        stop("interval must be '1 hour' or '15 min'")
    }

    counts <- counts %>%
        mutate(SignalID = factor(SignalID),
               Detector = factor(Detector),
               CallPhase = factor(CallPhase)) %>%
        filter(!is.na(CallPhase))

    # Identify detectors/phases from detector config file. Expand.
    #  This ensures all detectors are included in the bad detectors calculation.
    all_timeperiods <- seq(as_datetime(date_), as_datetime(date_) + days(1) - seconds(1), by = interval)
    det_config <- get_det_config(date_) %>%
        expand(nesting(SignalID, Detector, CallPhase, ApproachDesc), ## ApproachDesc is new
                   Timeperiod = all_timeperiods) %>%
        transmute(SignalID = factor(SignalID),
                  Timeperiod = Timeperiod,
                  Detector = factor(Detector),
                  CallPhase = factor(CallPhase),
                  Mainline = grepl("Mainline", ApproachDesc)) ## New

    expanded_counts <- full_join(
        det_config,
        counts,
        by = c("SignalID", "Timeperiod", "Detector", "CallPhase"),
        relationship = "many-to-many"
    ) %>%
        transmute(SignalID = factor(SignalID),
                  Date = date(Timeperiod),
                  Timeperiod = Timeperiod,
                  Detector = factor(Detector),
                  CallPhase = factor(CallPhase),
                  Mainline,
                  vol = as.double(vol)) %>%
        replace_na(list(vol = 0)) %>%
        arrange(SignalID, CallPhase, Detector, Timeperiod) %>%

        group_by(SignalID, Date, CallPhase, Detector) %>%
        mutate(delta_vol = vol - lag(vol),
               mean_abs_delta = as.integer(ceiling(mean(abs(delta_vol), na.rm = TRUE))),
               vol_streak = if_else(hour(Timeperiod) < 5, -1, vol),
               flatlined = streak_run(vol_streak),
               flatlined = if_else(hour(Timeperiod) < 5, 0, as.double(flatlined)),
               flat_flag = max(flatlined, na.rm = TRUE) > max_flat,
               maxvol_flag = if_else(Mainline,
                                     sum(vol > max_volume_mainline) > hi_vol_pers,
                                     sum(vol > max_volume) > hi_vol_pers),
               mad_flag = if_else(Mainline,
                                  mean_abs_delta > max_abs_delta_mainline,
                                  mean_abs_delta > max_abs_delta)) %>%

        ungroup() %>%
        select(-Mainline, -vol_streak)

    # bad day = any of the following:
    #    flatlined for at least 5 hours (starting at 5am hour)
    #    vol exceeds maximum allowed over 5 different hours
    #    mean absolute delta exceeds 500
    #  - or -
    #    flatlined for at least 20 15-min periods (starting at 5am)
    #    vol exceeds maximum over 20 different 15-min periods
    #    mean absolute delta exeeds 125

    expanded_counts %>%
        group_by(
            SignalID, Date, Detector, CallPhase) %>%
        mutate(
            flat_strk = as.integer(max(flatlined)),
            max_vol = as.integer(max(vol, na.rm = TRUE)),
            flat_flag = max(flat_flag),
            maxvol_flag = max(maxvol_flag),
            mad_flag = max(mad_flag)) %>%
        ungroup() %>%
        mutate(Good_Day = if_else(
            flat_flag > 0 | maxvol_flag > 0 | mad_flag > 0,
            as.integer(0),
            as.integer(1))) %>%
        select(
            SignalID:flatlined, flat_strk, flat_flag, maxvol_flag, mad_flag, Good_Day) %>%
        mutate(
            Month_Hour = Timeperiod - days(day(Timeperiod) - 1),
            Hour = Month_Hour - months(month(Month_Hour) - 1),
            vol = if_else(Good_Day==1, vol, as.double(NA)))
}


filtered_counts_hack <- function(filtered_counts) {

    # This is an unabashed hack to temporarily deal with one problematic phase

    # 21 - max(vol) > 1000 --> bad

    hack_slice <- (filtered_counts$SignalID == 7242 & filtered_counts$Detector == 21)

    fc_7242 <- filtered_counts[hack_slice,]
    replacement_values <- fc_7242 %>%
        mutate(vol = ifelse(max(vol) > 1000, NA, vol)) %>%
        mutate(Good_Day = ifelse(is.na(vol), 0, 1))

    filtered_counts[hack_slice, "vol"] <- replacement_values$vol
    filtered_counts[hack_slice, "Good_Day"] <- replacement_values$Good_Day

    # 22 - max(vol) > 1000 --> bad

    hack_slice <- (filtered_counts$SignalID == 7242 & filtered_counts$Detector == 22)

    fc_7242 <- filtered_counts[hack_slice,]
    replacement_values <- fc_7242 %>%
        mutate(vol = ifelse(max(vol) > 1000, NA, vol)) %>%
        mutate(Good_Day = ifelse(is.na(vol), 0, 1))

    filtered_counts[hack_slice, "vol"] <- replacement_values$vol
    filtered_counts[hack_slice, "Good_Day"] <- replacement_values$Good_Day

    # 23 - max(vol) > 500 --> bad

    hack_slice <- (filtered_counts$SignalID == 7242 & filtered_counts$Detector == 23)

    fc_7242 <- filtered_counts[hack_slice,]
    replacement_values <- fc_7242 %>%
        mutate(vol = ifelse(max(vol) > 500, NA, vol)) %>%
        mutate(Good_Day = ifelse(is.na(vol), 0, 1))

    filtered_counts[hack_slice, "vol"] <- replacement_values$vol
    filtered_counts[hack_slice, "Good_Day"] <- replacement_values$Good_Day

    filtered_counts
}



get_adjusted_counts <- function(df) {
    fc <- df %>%
        mutate(DOW = wday(Timeperiod),
               vol = as.double(vol))

    ph_contr <- fc %>%
        group_by(SignalID, CallPhase, Timeperiod) %>%
        mutate(na.vol = sum(is.na(vol))) %>%
        ungroup() %>%
        filter(na.vol == 0) %>%
        dplyr::select(-na.vol) %>%

        # phase contribution factors--fraction of phase volume a detector contributes
        group_by(SignalID, CallPhase, Detector) %>%
        summarize(vol = sum(vol, na.rm = TRUE),
                  .groups = "drop_last") %>%
        mutate(sum_vol = sum(vol, na.rm = TRUE),
               Ph_Contr = vol/sum_vol) %>%
        ungroup() %>%
        dplyr::select(-vol, -sum_vol)

    # fill in missing detectors from other detectors on that phase
    fc_phc <- left_join(fc, ph_contr, by = c("SignalID", "CallPhase", "Detector")) %>%
        # fill in missing detectors from other detectors on that phase
        group_by(SignalID, Timeperiod, CallPhase) %>%
        mutate(mvol = mean(vol/Ph_Contr, na.rm = TRUE)) %>% ungroup()

    fc_phc$mvol[fc_phc$mvol > 3000] <- NA  # Prevent ridiculously high interpolated values

    fc_phc$vol[is.na(fc_phc$vol)] <- as.integer(fc_phc$mvol[is.na(fc_phc$vol)] * fc_phc$Ph_Contr[is.na(fc_phc$vol)])

    #hourly volumes over the month to fill in missing data for all detectors in a phase
    mo_hrly_vols <- fc_phc %>%
        group_by(SignalID, CallPhase, Detector, DOW, Month_Hour) %>%
        summarize(Hourly_Volume = median(vol, na.rm = TRUE), .groups = "drop")

    # fill in missing detectors by hour and day of week volume in the month
    left_join(fc_phc,
              mo_hrly_vols,
              by = (c("SignalID", "CallPhase", "Detector", "DOW", "Month_Hour"))) %>%
        ungroup() %>%
        mutate(vol = if_else(is.na(vol), as.integer(Hourly_Volume), as.integer(vol))) %>%

        dplyr::select(SignalID, CallPhase, Detector, Timeperiod, vol) %>%

        filter(!is.na(vol))
}




# ==============================================================================
# Arrow implementation on local drive

prep_db_for_adjusted_counts_arrow <- function(table, conf, date_range) {

    fc_ds <- arrow::open_dataset(sources = glue("s3://{conf$bucket}/mark/{table}/")) %>%
        filter(date %in% format(date_range, "%F"))
    chunks <- get_signals_chunks_arrow(fc_ds, rows = 1e6)
    groups <- tibble(group = names(chunks), SignalID = chunks) %>%
        unnest(SignalID) %>%
        mutate(SignalID = as.integer(SignalID))

    if (dir.exists(table)) unlink(table, recursive = TRUE)
    dir.create(table)
    usable_cores <- 1
    mclapply(date_range, mc.cores = usable_cores, mc.preschedule = FALSE, FUN = function(date_) {
        date_str <- format(date_, "%F")
        cat('.')

        fc <- s3_read_parquet_parallel(table, date_, date_, bucket = conf$bucket) %>%
            transmute(
                SignalID = as.integer(as.character(SignalID)),
                Date, Timeperiod, Month_Hour,
                Detector = as.integer(as.character(Detector)),
                CallPhase = as.integer(as.character(CallPhase)),
                Good_Day,
                vol)
        dc <- get_det_config_vol(date_) %>%
            transmute(
                SignalID = as.integer(as.character(SignalID)),
                Date,
                Detector = as.integer(as.character(Detector)),
                CallPhase = as.integer(as.character(CallPhase)),
                CountPriority) %>%
            filter(!is.na(CountPriority))

        fc <- left_join(fc, dc, by = c("SignalID", "Detector", "CallPhase", "Date")) %>%
            filter(!is.na(CountPriority)) %>%
            left_join(groups, by = c("SignalID"))
        for (grp in names(chunks)) {
            folder_location <- glue("{table}/group={grp}/date={date_str}")
            if (!dir.exists(folder_location)) dir.create(folder_location, recursive = TRUE)
            fc %>% filter(group == grp) %>% select(-group) %>%
                write_parquet(glue("{folder_location}/{table}.parquet"))
        }
    })
    cat('\n')
    return (TRUE)
}




get_adjusted_counts_arrow <- function(fc_table, ac_table, conf, callback = function(x) {x}) {
    # callback would be for get_thruput

    fc_ds <- arrow::open_dataset(fc_table)

    if (dir.exists(ac_table)) unlink(ac_table, recursive = TRUE)
    dir.create(ac_table)

    groups <- (fc_ds %>% select(group) %>% collect() %>% distinct())$group
    usable_cores <- 1
    mclapply(groups, mc.cores = usable_cores, mc.preschedule = FALSE, FUN = function(grp) {
        cat('.')
        ac <- arrow::open_dataset(fc_table) %>%
            filter(group == grp) %>%
            collect() %>%
            get_adjusted_counts() %>%
            mutate(Date = as_date(Timeperiod)) %>%
            callback()
        for (date_ in unique(ac$Date)) {
            date_str <- format(as_date(date_), "%F")
            ac_dir <- glue("{ac_table}/date={date_str}")
            if (!dir.exists(ac_dir)) dir.create(ac_dir)
            filename <- tempfile(
                tmpdir = ac_dir,
                pattern = "ac_",
                fileext = ".parquet")
            ac %>% filter(Date == date_) %>% write_parquet(filename)
        }
    })
    cat('\n')
}

