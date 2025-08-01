
weighted_mean_by_corridor_ <- function(df, per_, corridors, var_, wt_ = NULL) {

    per_ <- as.name(per_)

    gdf <- left_join(df, corridors, relationship = "many-to-many") %>%
        filter(!is.na(Corridor)) %>%
        mutate(Corridor = factor(Corridor)) %>%
        group_by(Zone_Group, Zone, Corridor, !!per_)

    if (is.null(wt_)) {
        gdf %>%
            summarize(!!var_ := mean(!!var_, na.rm = TRUE),
                      .groups = "drop_last") %>%
            mutate(lag_ = lag(!!var_),
                   delta = ((!!var_) - lag_)/lag_) %>%
            ungroup() %>%
            dplyr::select(Zone_Group, Zone, Corridor, !!per_, !!var_, delta)
    } else {
        gdf %>%
            summarize(!!var_ := weighted.mean(!!var_, !!wt_, na.rm = TRUE),
                      !!wt_ := sum(!!wt_, na.rm = TRUE),
                      .groups = "drop_last") %>%
            mutate(lag_ = lag(!!var_),
                   delta = ((!!var_) - lag_)/lag_) %>%
            ungroup() %>%
            dplyr::select(Zone_Group, Zone, Corridor, !!per_, !!var_, !!wt_, delta)
    }
}


group_corridor_by_ <- function(df, per_, var_, wt_, corr_grp) {
    df %>%
        group_by(!!per_) %>%
        summarize(!!var_ := weighted.mean(!!var_, !!wt_, na.rm = TRUE),
                  !!wt_ := sum(!!wt_, na.rm = TRUE),
                  .groups = "drop_last") %>%
        mutate(Corridor = factor(corr_grp)) %>%
        mutate(lag_ = lag(!!var_),
               delta = ((!!var_) - lag_)/lag_) %>%
        mutate(Zone_Group = corr_grp) %>%
        dplyr::select(Zone_Group, Corridor, !!per_, !!var_, !!wt_, delta)
}


group_corridors_ <- function(df, per_, var_, wt_, gr_ = group_corridor_by_) {

    per_ <- as.name(per_)


    # Get average for each Zone or District, according to corridors$Zone
    df_ <- df %>%
        split(df$Zone)
    all_zones <- lapply(names(df_), function(x) { gr_(df_[[x]], per_, var_, wt_, x) })

    # Get average for All RTOP (RTOP1 and RTOP2)
    all_rtop <- df %>%
        filter(Zone_Group %in% c("RTOP1", "RTOP2")) %>%
        gr_(per_, var_, wt_, "All RTOP")

    # Get average for RTOP1
    all_rtop1 <- df %>%
        filter(Zone_Group == "RTOP1") %>%
        gr_(per_, var_, wt_, "RTOP1")

    # Get average for RTOP2
    all_rtop2 <- df %>%
        filter(Zone_Group == "RTOP2") %>%
        gr_(per_, var_, wt_, "RTOP2")


    # Get average for All Zone 7 (Zone 7m and Zone 7d)
    all_zone7 <- df %>%
        filter(Zone %in% c("Zone 7m", "Zone 7d")) %>%
        gr_(per_, var_, wt_, "Zone 7")

    # concatenate all summaries with corridor averages
    dplyr::bind_rows(select(df, "Corridor", Zone_Group = "Zone", !!per_, !!var_, !!wt_, "delta"),
                     all_zones,
                     all_rtop,
                     all_rtop1,
                     all_rtop2,
                     all_zone7) %>%
        distinct() %>%
        mutate(Zone_Group = factor(Zone_Group),
               Corridor = factor(Corridor))
}



get_hourly <- function(df, var_, corridors) {
    full_join(df, corridors, by = "SignalID", relationship = "many-to-many") %>%
        filter(!is.na(Corridor)) %>%
        group_by(SignalID) %>%
        mutate(lag_ = lag(!!as.name(var_)),
               delta = ((!!as.name(var_)) - lag_)/lag_) %>%
        ungroup() %>%
        select(SignalID, Hour, !!as.name(var_), delta, Zone_Group, Zone, Corridor, Subcorridor)
}


get_period_avg <- function(df, var_, per_, wt_ = "ones") {

    if (wt_ == "ones") {
        wt_ <- as.name(wt_)
        df <- mutate(df, !!wt_ := 1)
    } else {
        wt_ <- as.name(wt_)
    }
    var_ <- as.name(var_)
    per_ <- as.name(per_)

    df %>% group_by(SignalID, !!per_) %>%
        summarize(!!var_ := weighted.mean(!!var_, !!wt_, na.rm = TRUE), # Mean of phases
                  !!wt_ := sum(!!wt_, na.rm = TRUE),
                  .groups = "drop_last") %>%
        mutate(lag_ = lag(!!var_),
               delta = ((!!var_) - lag_)/lag_) %>%
        ungroup() %>%
        select(SignalID, !!per_, !!var_, !!wt_, delta)
}


get_period_sum <- function(df, var_, per_) {

    var_ <- as.name(var_)
    per_ <- as.name(per_)

    df %>% group_by(SignalID, !!per_) %>%
        summarize(!!var_ := sum(!!var_, na.rm = TRUE), # Sum of phases
                  .groups = "drop_last") %>%
        mutate(lag_ = lag(!!var_),
               delta = ((!!var_) - lag_)/lag_) %>%
        ungroup() %>%
        select(SignalID, !!per_, !!var_, delta)
}


get_daily_avg <- function(df, var_, wt_ = "ones", peak_only = FALSE) {

    var_ <- as.name(var_)
    if (wt_ == "ones") {
        wt_ <- as.name(wt_)
        df <- mutate(df, !!wt_ := 1)
    } else {
        wt_ <- as.name(wt_)
    }

    if (peak_only == TRUE) {
        df <- df %>%
            filter(hour(Date_Hour) %in% c(AM_PEAK_HOURS, PM_PEAK_HOURS))
    }

    df %>%
        group_by(SignalID, Date) %>%
        summarize(!!var_ := weighted.mean(!!var_, !!wt_, na.rm = TRUE), # Mean of phases 2,6
                  !!wt_ := sum(!!wt_, na.rm = TRUE),
                  .groups = "drop_last") %>% # Sum of phases 2,6
        mutate(lag_ = lag(!!var_),
               delta = ((!!var_) - lag_)/lag_) %>%
        ungroup() %>%
        dplyr::select(SignalID, Date, !!var_, !!wt_, delta)

    # SignalID | Date | var_ | wt_ | delta
}


get_daily_avg_cctv <- function(df, var_ = "uptime", wt_ = "num", peak_only = FALSE) {

    var_ <- as.name(var_)
    wt_ <- as.name(wt_)

    df %>%
        group_by(CameraID, Date) %>%
        summarize(!!var_ := weighted.mean(!!var_, !!wt_, na.rm = TRUE), # Mean of phases 2,6
                  !!wt_ := sum(!!wt_, na.rm = TRUE),
                  .groups = "drop_last") %>% # Sum of phases 2,6
        mutate(lag_ = lag(!!var_),
               delta = ((!!var_) - lag_)/lag_) %>%
        ungroup() %>%
        dplyr::select(CameraID, Date, !!var_, !!wt_, delta)

    # CameraID | Date | var_ | wt_ | delta
}


get_daily_sum <- function(df, var_) {

    var_ <- as.name(var_)
    # leave this in for possible generalization in the future
    # to different time periods besides daily
    per_ <- "Date"
    per_ <- as.name(per_)


    df %>%
        group_by(SignalID, !!per_) %>%
        summarize(!!var_ := sum(!!var_, na.rm = TRUE),
                  .groups = "drop_last") %>%
        mutate(lag_ = lag(!!var_),
               delta = ((!!var_) - lag_)/lag_) %>%
        ungroup() %>%
        dplyr::select(SignalID, !!per_, !!var_, delta)
}


get_weekly_sum_by_day <- function(df, var_) {

    var_ <- as.name(var_)

    Tuesdays <- get_Tuesdays(df)

    df %>%
        group_by(SignalID, Week, CallPhase) %>%
        summarize(!!var_ := mean(!!var_, na.rm = TRUE),
                  .groups = "drop_last") %>% # Mean over 3 days in the week

        summarize(!!var_ := sum(!!var_, na.rm = TRUE),
                  .groups = "drop_last") %>% # Sum of phases 2,6

        mutate(lag_ = lag(!!var_),
               delta = ((!!var_) - lag_)/lag_) %>%
        ungroup() %>%
        left_join(Tuesdays, by = c("Week")) %>%
        dplyr::select(SignalID, Date, Week, !!var_, delta)
}


get_weekly_avg_by_day <- function(df, var_, wt_ = "ones", peak_only = TRUE) {

    var_ <- as.name(var_)
    if (wt_ == "ones") {
        wt_ <- as.name(wt_)
        df <- mutate(df, !!wt_ := 1)
    } else {
        wt_ <- as.name(wt_)
    }

    Tuesdays <- get_Tuesdays(df)

    if (peak_only == TRUE) {
        df <- df %>%
            filter((hour(Date_Hour) %in% c(AM_PEAK_HOURS, PM_PEAK_HOURS)))
    }

    df %>%
        complete(nesting(SignalID, CallPhase), Week = full_seq(Week, 1)) %>%
        group_by(SignalID, CallPhase, Week) %>%
        summarize(!!var_ := weighted.mean(!!var_, !!wt_, na.rm = TRUE),
                  !!wt_ := sum(!!wt_, na.rm = TRUE),
                  .groups = "drop") %>% # Mean over 3 days in the week

        group_by(SignalID, Week) %>%
        summarize(!!var_ := weighted.mean(!!var_, !!wt_, na.rm = TRUE), # Mean of phases 2,6
                  !!wt_ := sum(!!wt_, na.rm = TRUE),
                  .groups = "drop_last") %>% # Sum of phases 2,6

        mutate(lag_ = lag(!!var_),
               delta = ((!!var_) - lag_)/lag_) %>%
        ungroup() %>%
        left_join(Tuesdays, by = c("Week")) %>%
        dplyr::select(SignalID, Date, Week, !!var_, !!wt_, delta) %>%
        ungroup()
}


get_weekly_avg_by_day_cctv <- function(df, var_ = "uptime", wt_ = "num") {

    var_ <- as.name(var_)
    wt_ <- as.name(wt_)

    Tuesdays <- get_Tuesdays(df)

    df %>% mutate(Week = week(Date)) %>%
        dplyr::select(-Date) %>%
        left_join(Tuesdays, by = c("Week")) %>%
        group_by(CameraID, Zone_Group, Zone, Corridor, Subcorridor, Description, Week) %>%
        summarize(!!var_ := weighted.mean(!!var_, !!wt_, na.rm = TRUE),
                  !!wt_ := sum(!!wt_, na.rm = TRUE),
                  .groups = "keep") %>% # Mean over 3 days in the week

        summarize(!!var_ := weighted.mean(!!var_, !!wt_, na.rm = TRUE), # Mean of phases 2,6
                  !!wt_ := sum(!!wt_, na.rm = TRUE),
                  .groups = "drop_last") %>% # Sum of phases 2,6

        mutate(lag_ = lag(!!var_),
               delta = ((!!var_) - lag_)/lag_) %>%
        ungroup() %>%
        left_join(Tuesdays) %>%
        dplyr::select(Zone_Group, Zone, Corridor, Subcorridor, CameraID,
                      Description, Date, Week, !!var_, !!wt_, delta)
}




get_cor_weekly_avg_by_period <- function(df, corridors, var_, per_, wt_ = "ones") {

    if (wt_ == "ones") {
        wt_ <- as.name(wt_)
        df <- mutate(df, !!wt_ := 1)
    } else {
        wt_ <- as.name(wt_)
    }
    var_ <- as.name(var_)
    per_ <- as.name(per_)

    cor_df_out <- weighted_mean_by_corridor_(df, "Date", corridors, var_, wt_) %>%
        filter(!is.nan(!!var_))

    group_corridors_(cor_df_out, "Date", var_, wt_) %>%
        mutate(Week = week(Date))
}

get_cor_monthly_avg_by_period <- function(df, corridors, var_, per_, wt_="ones") {

    if (wt_ == "ones") {
        wt_ <- as.name(wt_)
        df <- mutate(df, !!wt_ := 1)
    } else {
        wt_ <- as.name(wt_)
    }
    var_ <- as.name(var_)

    cor_df_out <- weighted_mean_by_corridor_(df, per_, corridors, var_, wt_) %>%
        filter(!is.nan(!!var_))

    group_corridors_(cor_df_out, per_, var_, wt_)
}




get_cor_weekly_avg_by_day <- function(df, corridors, var_, wt_ = "ones") {

    if (wt_ == "ones") {
        wt_ <- as.name(wt_)
        df <- mutate(df, !!wt_ := 1)
    } else {
        wt_ <- as.name(wt_)
    }
    var_ <- as.name(var_)

    cor_df_out <- weighted_mean_by_corridor_(df, "Date", corridors, var_, wt_) %>%
        filter(!is.nan(!!var_))

    group_corridors_(cor_df_out, "Date", var_, wt_) %>%
        mutate(Week = week(Date))
}


get_monthly_avg_by_day <- function(df, var_, wt_ = NULL, peak_only = FALSE) {

    var_ <- as.name(var_)
    # if (wt_ == "ones") { ##--- this needs to be revisited. this should be added and this function should look like the weekly avg function above
    #     wt_ <- as.name(wt_)
    #     df <- mutate(df, !!wt_ := 1)
    # } else {
    #     wt_ <- as.name(wt_)
    # }

    if (peak_only == TRUE) {
        df <- df %>%
            filter((hour(Date_Hour) %in% c(AM_PEAK_HOURS, PM_PEAK_HOURS)))
    }

    df$Month <- df$Date
    day(df$Month) <- 1

    current_month <- max(df$Month)

    gdf <- df %>%
        complete(nesting(SignalID, CallPhase),
                 Month = seq(min(Month), current_month, by = "1 month")) %>%
        group_by(SignalID, Month, CallPhase)

    if (is.null(wt_)) {
        gdf %>%
            summarize(!!var_ := mean(!!var_, na.rm = TRUE),
                      .groups = "drop_last") %>%
            summarize(!!var_ := sum(!!var_, na.rm = TRUE),
                      .groups = "drop_last") %>% # Sum over Phases (2,6)
            mutate(lag_ = lag(!!var_),
                   delta = ((!!var_) - lag_)/lag_) %>%
            ungroup() %>%
            dplyr::select(-lag_) %>%
            filter(!is.na(Month))
    } else {
        wt_ <- as.name(wt_)
        gdf %>%
            summarize(!!var_ := weighted.mean(!!var_, !!wt_, na.rm = TRUE),
                      !!wt_ := sum(!!wt_, na.rm = TRUE),
                      .groups = "drop_last") %>%
            summarize(!!var_ := weighted.mean(!!var_, !!wt_, na.rm = TRUE), # Mean over Phases(2,6)
                      !!wt_ := sum(!!wt_, na.rm = TRUE),
                      .groups = "drop_last") %>%
            mutate(lag_ = lag(!!var_),
                   delta = ((!!var_) - lag_)/lag_) %>%
            ungroup() %>%
            dplyr::select(-lag_) %>%
            filter(!is.na(Month))
    }
}


get_cor_monthly_avg_by_day <- function(df, corridors, var_, wt_="ones") {

    if (wt_ == "ones") {
        wt_ <- as.name(wt_)
        df <- mutate(df, !!wt_ := 1)
    } else {
        wt_ <- as.name(wt_)
    }
    var_ <- as.name(var_)

    cor_df_out <- weighted_mean_by_corridor_(df, "Month", corridors, var_, wt_) %>%
        filter(!is.nan(!!var_))

    group_corridors_(cor_df_out, "Month", var_, wt_)
}


get_weekly_avg_by_hr <- function(df, var_, wt_ = NULL) {

    var_ <- as.name(var_)

    Tuesdays <- df %>%
        mutate(Date = date(Hour)) %>%
        get_Tuesdays()

    df_ <- df %>%
        select(-Date) %>%
        left_join(Tuesdays, by = c("Week")) %>%
        filter(!is.na(Date))
    year(df_$Hour) <- year(df_$Date)
    month(df_$Hour) <- month(df_$Date)
    day(df_$Hour) <- day(df_$Date)

    if (is.null(wt_)) {
        df_ %>%
            group_by(SignalID, Week, Hour, CallPhase) %>%
            summarize(!!var_ := mean(!!var_, na.rm = TRUE),
                      .groups = "drop_last") %>% # Mean over 3 days in the week
            summarize(!!var_ := mean(!!var_, na.rm = TRUE),
                      .groups = "drop_last") %>% # Mean of phases 2,6
            mutate(lag_ = lag(!!var_),
                   delta = ((!!var_) - lag_)/lag_) %>%
            ungroup() %>%
            dplyr::select(SignalID, Hour, Week, !!var_, delta)
    } else {
        wt_ <- as.name(wt_)
        df_ %>%
            group_by(SignalID, Week, Hour, CallPhase) %>%
            summarize(!!var_ := weighted.mean(!!var_, !!wt_, na.rm = TRUE),
                      !!wt_ := sum(!!wt_, na.rm = TRUE),
                      .groups = "drop_last") %>% # Mean over 3 days in the week
            summarize(!!var_ := weighted.mean(!!var_, !!wt_, na.rm = TRUE), # Mean of phases 2,6
                      !!wt_ := sum(!!wt_, na.rm = TRUE),
                      .groups = "drop_last") %>% # Sum of phases 2,6
            mutate(lag_ = lag(!!var_),
                   delta = ((!!var_) - lag_)/lag_) %>%
            ungroup() %>%
            dplyr::select(SignalID, Hour, Week, !!var_, !!wt_, delta)
    }
}


get_cor_weekly_avg_by_hr <- function(df, corridors, var_, wt_="ones") {

    if (wt_ == "ones") {
        wt_ <- as.name(wt_)
        df <- mutate(df, !!wt_ := 1)
    } else {
        wt_ <- as.name(wt_)
    }
    var_ <- as.name(var_)

    cor_df_out <- weighted_mean_by_corridor_(df, "Hour", corridors, var_, wt_)

    group_corridors_(cor_df_out, "Hour", var_, wt_)
}


get_sum_by_period <- function(df, var_, interval) {

    var_ <- as.name(var_)

    df %>%
        mutate(DOW = wday(Timeperiod),
               Week = week(date(Timeperiod)),
               Timeperiod = floor_date(Timeperiod, unit = interval)) %>%
        group_by(SignalID, CallPhase, Week, DOW, Timeperiod) %>%
        summarize(!!var_ := sum(!!var_, na.rm = TRUE),
                  .groups = "drop_last") %>%
        mutate(lag_ = lag(!!var_),
               delta = ((!!var_) - lag_)/lag_) %>%
        ungroup() %>%
        dplyr::select(-lag_)
}


get_avg_by_hr <- function(df, var_, wt_ = NULL) {

    df_ <- as.data.table(df)

    df_[, c("DOW", "Week", "Hour") := list(wday(Date_Hour),
                                           week(date(Date_Hour)),
                                           floor_date(Date_Hour, unit = '1 hour'))]
    if (is.null(wt_)) {
        ret <- df_[, .(mean(get(var_)), 1),
                   by = .(SignalID, CallPhase, Week, DOW, Hour)]
        ret <- ret[, c((var_), (wt_)) := .(V1, V2)][, -(V1:V2)]
        as_tibble(ret)
    } else {
        ret <- df_[, .(weighted.mean(get(var_), get(wt_), na.rm = TRUE),
                       sum(get(wt_), na.rm = TRUE)),
                   by = .(SignalID, CallPhase, Week, DOW, Hour)]
        ret <- ret[, c((var_), (wt_)) := .(V1, V2)][, -(V1:V2)]
        ret[, delta := (get(var_) - shift(get(var_), 1, type = "lag"))/shift(get(var_), 1, type = "lag"),
            by = .(SignalID, CallPhase)]
        as_tibble(ret)
    }
}


get_monthly_avg_by_hr <- function(df, var_, wt_ = "ones") {

    if (wt_ == "ones") {
        wt_ <- as.name(wt_)
        df <- mutate(df, !!wt_ := 1)
    } else {
        wt_ <- as.name(wt_)
    }
    var_ <- as.name(var_)

    df %>%
        group_by(SignalID, Hour) %>%
        summarize(!!var_ := weighted.mean(!!var_, !!wt_, na.rm = TRUE),
                  !!wt_ := sum(!!wt_, na.rm = TRUE),
                  .groups = "drop_last") %>%
        mutate(Hour = Hour - days(day(Hour)) + days(1)) %>%
        group_by(SignalID, Hour) %>%
        summarize(!!var_ := weighted.mean(!!var_, !!wt_, na.rm = TRUE),
                  !!wt_ := sum(!!wt_, na.rm = TRUE),
                  .groups = "drop_last") %>%
        mutate(lag_ = lag(!!var_),
               delta = ((!!var_) - lag_)/lag_) %>%
        ungroup() %>%
        dplyr::select(-lag_)
}


get_cor_monthly_avg_by_hr <- function(df, corridors, var_, wt_ = "ones") {

    if (wt_ == "ones") {
        wt_ <- as.name(wt_)
        df <- mutate(df, !!wt_ := 1)
    } else {
        wt_ <- as.name(wt_)
    }
    var_ <- as.name(var_)

    cor_df_out <- weighted_mean_by_corridor_(df, "Hour", corridors, var_, wt_)
    group_corridors_(cor_df_out, "Hour", var_, wt_)
}


# Used to get peak period metrics from hourly results, i.e., peak/off-peak split failures
summarize_by_peak <- function(df, date_col) {

    date_col_ <- as.name(date_col)

    df %>%
        mutate(Peak = if_else(hour(!!date_col_) %in% AM_PEAK_HOURS | hour(!!date_col_) %in% PM_PEAK_HOURS,
                              "Peak",
                              "Off_Peak"),
               Peak = factor(Peak)) %>%
        split(.$Peak) %>%
        lapply(function(x) {
            x %>%
                group_by(SignalID, Period = date(!!date_col_), Peak) %>%
                summarize(sf_freq = weighted.mean(sf_freq, cycles),
                          cycles = sum(cycles),
                          .groups = "drop") %>%
                select(-Peak)
        })
}



# -- end Generic Aggregation Functions


get_daily_detector_uptime <- function(filtered_counts) {

    # Remove time periods with no volume on all detectors - the source of much back-and-forth
    bad_comms <- filtered_counts %>%
        group_by(SignalID, Timeperiod) %>%
        summarize(vol = sum(vol, na.rm = TRUE),
                  .groups = "drop") %>%
        dplyr::filter(vol == 0) %>%
        dplyr::select(-vol)
    fc <- anti_join(filtered_counts, bad_comms, by = c("SignalID", "Timeperiod")) %>%
        ungroup()

    ddu <- fc %>%
    # ddu <- filtered_counts %>%   # If reverting this change back, delete the above and uncomment this line.
        mutate(Date_Hour = Timeperiod,
               Date = date(Date_Hour)) %>%
        dplyr::select(SignalID, CallPhase, Detector, Date, Date_Hour, Good_Day) %>%
        ungroup()
    if (nrow(ddu) > 0) {
        ddu <- ddu %>%
        mutate(setback = ifelse(CallPhase %in% c(2,6), "Setback", "Presence"),
               setback = factor(setback),
               SignalID = factor(SignalID)) %>%
        split(.$setback) %>% lapply(function(x) {
            x %>%
                group_by(SignalID, CallPhase, Date, Date_Hour, setback) %>%
                summarize(uptime = sum(Good_Day, na.rm = TRUE),
                          all = n(),
                          .groups = "drop") %>%
                mutate(uptime = uptime/all,
                       all = as.double(all))
        })
    }
    ddu
}


get_avg_daily_detector_uptime <- function(ddu) {

    sb_daily_uptime <- get_daily_avg(filter(ddu, setback == "Setback"),
                                     "uptime", "all",
                                     peak_only = FALSE)
    pr_daily_uptime <- get_daily_avg(filter(ddu, setback == "Presence"),
                                     "uptime", "all",
                                     peak_only = FALSE)
    all_daily_uptime <- get_daily_avg(ddu,
                                      "uptime", "all",
                                      peak_only = FALSE)

    sb_pr <- full_join(sb_daily_uptime, pr_daily_uptime,
                       by = c("SignalID", "Date"),
                       suffix = c(".sb", ".pr"))

    full_join(all_daily_uptime, sb_pr,
              by = c("SignalID", "Date")) %>%
        dplyr::select(-starts_with("delta.")) %>%
        rename(uptime = uptime)
}


get_daily_aog <- function(df) {
    get_daily_avg(df, var_ = "aog", wt_ = "vol", peak_only = TRUE)
}


get_cor_avg_daily_detector_uptime <- function(avg_daily_detector_uptime, corridors) {

    cor_daily_sb_uptime %<-% (avg_daily_detector_uptime %>%
                                  filter(!is.na(uptime.sb)) %>%
                                  get_cor_weekly_avg_by_day(corridors, "uptime.sb", "all.sb"))

    cor_daily_pr_uptime %<-% (avg_daily_detector_uptime %>%
                                  filter(!is.na(uptime.pr)) %>%
                                  get_cor_weekly_avg_by_day(corridors, "uptime.pr", "all.pr"))

    cor_daily_all_uptime %<-% (avg_daily_detector_uptime %>%
                                   filter(!is.na(uptime)) %>%
                                   # average corridor by ones instead of all:
                                   #  to treat all signals equally instead of weighted by # of detectors
                                   get_cor_weekly_avg_by_day(corridors, "uptime", "ones"))

    full_join(dplyr::select(cor_daily_sb_uptime, -c(all.sb, delta)),
              dplyr::select(cor_daily_pr_uptime, -c(all.pr, delta)),
              relationship = "many-to-many") %>%
        left_join(dplyr::select(cor_daily_all_uptime, -c(ones, delta)), relationship = "many-to-many") %>%
        mutate(Zone_Group = factor(Zone_Group))
}


get_weekly_vpd <- function(vpd) {
    vpd <- filter(vpd, DOW %in% c(TUE,WED,THU))
    get_weekly_sum_by_day(vpd, "vpd")
}


get_weekly_papd <- function(papd) {
    papd <- filter(papd, DOW %in% c(TUE,WED,THU))
    get_weekly_sum_by_day(papd, "papd")
}


get_weekly_thruput <- function(throughput) {
    get_weekly_sum_by_day(throughput, "vph")
}


get_weekly_aog_by_day <- function(daily_aog) {
    get_weekly_avg_by_day(daily_aog, "aog", "vol", peak_only = TRUE)
}


get_weekly_pr_by_day <- function(daily_pr) {
    get_weekly_avg_by_day(daily_pr, "pr", "vol", peak_only = TRUE)
}


get_weekly_sf_by_day <- function(sf) {
    get_weekly_avg_by_day(sf, "sf_freq", "cycles", peak_only = TRUE)
}


get_weekly_qs_by_day <- function(qs) {
    get_weekly_avg_by_day(qs, "qs_freq", "cycles", peak_only = TRUE)
}


get_weekly_detector_uptime <- function(avg_daily_detector_uptime) {
    avg_daily_detector_uptime %>%
        mutate(CallPhase = 0, Week = week(Date)) %>%
        get_weekly_avg_by_day("uptime", "all", peak_only = FALSE) %>%
        replace_na(list(uptime = 0)) %>%
        arrange(SignalID, Date)
}


get_cor_weekly_vpd <- function(weekly_vpd, corridors) {
    get_cor_weekly_avg_by_day(weekly_vpd, corridors, "vpd")
}


get_cor_weekly_papd <- function(weekly_papd, corridors) {
    get_cor_weekly_avg_by_day(weekly_papd, corridors, "papd")
}


get_cor_weekly_thruput <- function(weekly_throughput, corridors) {
    get_cor_weekly_avg_by_day(weekly_throughput, corridors, "vph")
}


get_cor_weekly_aog_by_day <- function(weekly_aog, corridors) {
    get_cor_weekly_avg_by_day(weekly_aog, corridors, "aog", "vol")
}


get_cor_weekly_pr_by_day <- function(weekly_pr, corridors) {
    get_cor_weekly_avg_by_day(weekly_pr, corridors, "pr", "vol")
}


get_cor_weekly_sf_by_day <- function(weekly_sf, corridors) {
    get_cor_weekly_avg_by_day(weekly_sf, corridors, "sf_freq", "cycles")
}


get_cor_weekly_qs_by_day <- function(weekly_qs, corridors) {
    get_cor_weekly_avg_by_day(weekly_qs, corridors, "qs_freq", "cycles")
}


get_cor_weekly_detector_uptime <- function(avg_weekly_detector_uptime, corridors) {
    get_cor_weekly_avg_by_day(avg_weekly_detector_uptime, corridors, "uptime", "all")
}


get_monthly_vpd <- function(vpd) {
    vpd <- filter(vpd, DOW %in% c(TUE,WED,THU))
    get_monthly_avg_by_day(vpd, "vpd", peak_only = FALSE)
}


get_monthly_flashevent <- function(flash) {
    flash <- flash %>%
        select(SignalID, Date)

    day(flash$Date) <- 1

    flash <- flash %>%
        group_by(SignalID, Date) %>%
        summarize(flash = n(), .groups = "drop")

    flash$CallPhase = 0 # set the dummy, 'CallPhase' is used in get_monthly_avg_by_day() function
    get_monthly_avg_by_day(flash, "flash", peak_only = FALSE)
}


get_monthly_papd <- function(papd) {
    papd <- filter(papd, DOW %in% c(TUE,WED,THU))
    get_monthly_avg_by_day(papd, "papd", peak_only = FALSE)
}


get_monthly_thruput <- function(throughput) {
    get_monthly_avg_by_day(throughput, "vph", peak_only = FALSE)
}


get_monthly_aog_by_day <- function(daily_aog) {
    get_monthly_avg_by_day(daily_aog, "aog", "vol", peak_only = TRUE)
}


get_monthly_pr_by_day <- function(daily_pr) {
    get_monthly_avg_by_day(daily_pr, "pr", "vol", peak_only = TRUE)
}


get_monthly_sf_by_day <- function(sf) {
    get_monthly_avg_by_day(sf, "sf_freq", "cycles", peak_only = TRUE)
}


get_monthly_qs_by_day <- function(qs) {
    get_monthly_avg_by_day(qs, "qs_freq", "cycles", peak_only = TRUE)
}


get_monthly_detector_uptime <- function(avg_daily_detector_uptime) {
    avg_daily_detector_uptime %>%
        mutate(CallPhase = 0) %>%
        get_monthly_avg_by_day("uptime", "all") %>%
        arrange(SignalID, Month)
}


get_cor_monthly_vpd <- function(monthly_vpd, corridors) {
    get_cor_monthly_avg_by_day(monthly_vpd, corridors, "vpd")
}

get_cor_monthly_flash <- function(monthly_flash, corridors) {
    get_cor_monthly_avg_by_day(monthly_flash, corridors, "flash")
}


get_cor_monthly_papd <- function(monthly_papd, corridors) {
    get_cor_monthly_avg_by_day(monthly_papd, corridors, "papd")
}


get_cor_monthly_thruput <- function(monthly_throughput, corridors) {
    get_cor_monthly_avg_by_day(monthly_throughput, corridors, "vph")
}


get_cor_monthly_aog_by_day <- function(monthly_aog_by_day, corridors) {
    get_cor_monthly_avg_by_day(monthly_aog_by_day, corridors, "aog", "vol")
}


get_cor_monthly_pr_by_day <- function(monthly_pr_by_day, corridors) {
    get_cor_monthly_avg_by_day(monthly_pr_by_day, corridors, "pr", "vol")
}


get_cor_monthly_sf_by_day <- function(monthly_sf_by_day, corridors) {
    get_cor_monthly_avg_by_day(monthly_sf_by_day, corridors, "sf_freq", "cycles")
}


get_cor_monthly_qs_by_day <- function(monthly_qs_by_day, corridors) {
    get_cor_monthly_avg_by_day(monthly_qs_by_day, corridors, "qs_freq", "cycles")
}


get_cor_monthly_detector_uptime <- function(avg_daily_detector_uptime, corridors) {

    cor_daily_sb_uptime %<-% (avg_daily_detector_uptime %>%
                                  filter(!is.na(uptime.sb)) %>%
                                  mutate(Month = Date - days(day(Date) - 1)) %>%
                                  get_cor_monthly_avg_by_day(corridors, "uptime.sb", "all.sb"))
    cor_daily_pr_uptime %<-% (avg_daily_detector_uptime %>%
                                  filter(!is.na(uptime.pr)) %>%
                                  mutate(Month = Date - days(day(Date) - 1)) %>%
                                  get_cor_monthly_avg_by_day(corridors, "uptime.pr", "all.pr"))
    cor_daily_all_uptime %<-% (avg_daily_detector_uptime %>%
                                   filter(!is.na(uptime)) %>%
                                   mutate(Month = Date - days(day(Date) - 1)) %>%
                                   get_cor_monthly_avg_by_day(corridors, "uptime", "all"))

    full_join(dplyr::select(cor_daily_sb_uptime, -c(all.sb, delta)),
              dplyr::select(cor_daily_pr_uptime, -c(all.pr, delta)),
              relationship = "many-to-many") %>%
        left_join(dplyr::select(cor_daily_all_uptime, -c(all)), relationship = "many-to-many") %>%
        mutate(Corridor = factor(Corridor),
               Zone_Group = factor(Zone_Group))
}


get_vph <- function(counts, interval = "1 hour", mainline_only = TRUE) {

    if (mainline_only == TRUE) {
        counts <- counts %>%
            filter(CallPhase %in% c(2,6)) # sum over Phases 2,6
    }
    # Determine the Timeperiod column by its data type (POSIXct)
    # for force it to be named Timeperiod
    per_col <- names(Filter(function(x) "POSIXct" %in% x, sapply(counts, class)))[1]
    if (per_col != "Timeperiod") {
        df <- rename(counts, Timeperiod = !!as.name(per_col))
    } else {
        df <- counts
    }
    df <- get_sum_by_period(df, "vol", interval) %>%
        group_by(SignalID, Week, DOW, Timeperiod) %>%
        summarize(vph = sum(vol, na.rm = TRUE),
                  .groups = "drop")
    if (interval == "1 hour") {
        df <- rename(df, Hour = Timeperiod)
    }
    df
}


get_aog_by_hr <- function(aog) {
    get_avg_by_hr(aog, "aog", "vol")
}


get_pr_by_hr <- function(aog) {
    get_avg_by_hr(aog, "pr", "vol")
}


get_sf_by_hr <- function(sf) {
    get_avg_by_hr(sf, "sf_freq", "cycles")
}


get_qs_by_hr <- function(qs) {
    get_avg_by_hr(qs, "qs_freq", "cycles")
}


get_monthly_vph <- function(vph) {

    vph %>%
        ungroup() %>%
        filter(DOW %in% c(TUE,WED,THU)) %>%
        group_by(SignalID, Hour) %>%
        summarize(vph = sum(vph, na.rm = TRUE),
                  .groups = "drop_last") %>%
        mutate(Hour = Hour - days(day(Hour)) + days(1)) %>%
        group_by(SignalID, Hour) %>%
        summarize(vph = mean(vph, na.rm = TRUE),
                  .groups = "drop")
}


get_monthly_paph <- function(paph) {
    paph %>%
        rename(vph = paph) %>%
        get_monthly_vph() %>%
        rename(paph = vph)
}


get_monthly_aog_by_hr <- function(aog_by_hr) {
    aog_by_hr %>%
        group_by(SignalID, Hour) %>%
        summarize(aog = weighted.mean(aog, vol, na.rm = TRUE),
                  vol = sum(vol, na.rm = TRUE),
                  .groups = "drop_last") %>%
        mutate(Hour = Hour - days(day(Hour)) + days(1)) %>%
        group_by(SignalID, Hour) %>%
        summarize(aog = weighted.mean(aog, vol, na.rm = TRUE),
                  vol = sum(vol, na.rm = TRUE),
                  .groups = "drop")
}


get_monthly_pr_by_hr <- function(pr_by_hr) {
    get_monthly_aog_by_hr(rename(pr_by_hr, aog = pr)) %>%
        rename(pr = aog)
}


get_monthly_sf_by_hr <- function(sf_by_hr) {
    get_monthly_aog_by_hr(rename(sf_by_hr, aog = sf_freq, vol = cycles)) %>%
        rename(sf_freq = aog, cycles = vol)
}


get_monthly_qs_by_hr <- function(qs_by_hr) {
    get_monthly_aog_by_hr(rename(qs_by_hr, aog = qs_freq, vol = cycles)) %>%
        rename(qs_freq = aog, cycles = vol)
}


get_cor_monthly_vph <- function(monthly_vph, corridors) {
    get_cor_monthly_avg_by_hr(monthly_vph, corridors, "vph")
}


get_cor_monthly_paph <- function(monthly_paph, corridors) {
    get_cor_monthly_avg_by_hr(monthly_paph, corridors, "paph")
}


get_cor_monthly_aog_by_hr <- function(monthly_aog_by_hr, corridors) {
    get_cor_monthly_avg_by_hr(monthly_aog_by_hr, corridors, "aog", "vol")
}


get_cor_monthly_pr_by_hr <- function(monthly_pr_by_hr, corridors) {
    get_cor_monthly_avg_by_hr(monthly_pr_by_hr, corridors, "pr", "vol")
}


get_cor_monthly_sf_by_hr <- function(monthly_sf_by_hr, corridors) {
    get_cor_monthly_avg_by_hr(monthly_sf_by_hr, corridors, "sf_freq", "cycles")
}


get_cor_monthly_qs_by_hr <- function(monthly_qs_by_hr, corridors) {
    get_cor_monthly_avg_by_hr(monthly_qs_by_hr, corridors, "qs_freq", "cycles")
}


get_cor_monthly_ti <- function(ti, cor_monthly_vph, corridors) {

    # Get share of volume (as pct) by hour over the day, for whole dataset
    day_dist <- cor_monthly_vph %>%
        group_by(Zone_Group, Zone, Corridor, month(Hour)) %>%
        mutate(pct = vph/sum(vph, na.rm = TRUE)) %>%
        group_by(Zone_Group, Zone, Corridor, hr = hour(Hour)) %>%
        summarize(pct = mean(pct, na.rm = TRUE),
                  .groups = "drop_last")

    left_join(ti,
              distinct(corridors, Zone_Group, Zone, Corridor),
              by = c("Zone_Group", "Zone", "Corridor"),
              relationship = "many-to-many") %>%
        mutate(hr = hour(Hour)) %>%
        left_join(day_dist) %>%
        ungroup() %>%
        tidyr::replace_na(list(pct = 1))
}


get_cor_weekly_ti <- function(ti, cor_weekly_vph, corridors) {

    tuesdays <- get_Tuesdays(mutate(cor_weekly_vph, Date = as_date(Hour)))
    cor_weekly_vph <- cor_weekly_vph %>% mutate(Week = week(as_date(Hour))) %>% left_join(tuesdays, by = "Week")

    # Get share of volume (as pct) by hour over the day, for whole dataset
    day_dist <- cor_weekly_vph %>%
        group_by(Zone_Group, Zone, Corridor, Date) %>%
        mutate(pct = vph/sum(vph, na.rm = TRUE)) %>%
        group_by(Zone_Group, Zone, Corridor, hr = hour(Hour)) %>%
        summarize(pct = mean(pct, na.rm = TRUE),
                  .groups = "drop_last")

    # this is problematic because we aggregate the travel time metrics by month
    # in python when we pull the data

    left_join(ti,
              distinct(corridors, Zone_Group, Zone, Corridor),
              by = c("Zone_Group", "Zone", "Corridor"),
              relationship = "many-to-many") %>%
        mutate(hr = hour(Hour)) %>%
        left_join(day_dist) %>%
        ungroup() %>%
        tidyr::replace_na(list(pct = 1))
}


get_cor_monthly_ti_by_hr <- function(ti, cor_monthly_vph, corridors) {

    df <- get_cor_monthly_ti(ti, cor_monthly_vph, corridors)

    if ("tti" %in% colnames(ti)) {
        tindx = "tti"
    } else if ("pti" %in% colnames(ti)) {
        tindx = "pti"
    } else if ("bi" %in% colnames(ti)) {
        tindx = "bi"
    } else if ("speed_mph" %in% colnames(ti)) {
        tindx = "speed_mph"
    } else {
        tindx = "oops"
    }

    get_cor_monthly_avg_by_hr(df, corridors, tindx, "pct")
}


get_cor_monthly_ti_by_day <- function(ti, cor_monthly_vph, corridors) {

    df <- get_cor_monthly_ti(ti, cor_monthly_vph, corridors)

    if ("tti" %in% colnames(ti)) {
        tindx = "tti"
    } else if ("pti" %in% colnames(ti)) {
        tindx = "pti"
    } else if ("bi" %in% colnames(ti)) {
        tindx = "bi"
    } else if ("speed_mph" %in% colnames(ti)) {
        tindx = "speed_mph"
    } else {
        tindx = "oops"
    }

    df %>%
        mutate(Month = as_date(Hour)) %>%
        get_cor_monthly_avg_by_day(corridors, tindx, "pct")
}


get_cor_weekly_ti_by_day <- function(ti, cor_weekly_vph, corridors) {

    df <- get_cor_weekly_ti(ti, cor_weekly_vph, corridors)

    if ("tti" %in% colnames(ti)) {
        tindx = "tti"
    } else if ("pti" %in% colnames(ti)) {
        tindx = "pti"
    } else if ("bi" %in% colnames(ti)) {
        tindx = "bi"
    } else if ("speed_mph" %in% colnames(ti)) {
        tindx = "speed_mph"
    } else {
        tindx = "oops"
    }

    df %>%
        mutate(Date = as_date(Hour)) %>%
        get_cor_weekly_avg_by_day(corridors, tindx, "pct")
}


get_weekly_vph <- function(vph) {
    vph <- filter(vph, DOW %in% c(TUE,WED,THU))
    get_weekly_avg_by_hr(vph, "vph")
}


get_weekly_paph <- function(paph) {
    paph <- filter(paph, DOW %in% c(TUE,WED,THU))
    get_weekly_avg_by_hr(paph, "paph")
}


get_cor_weekly_vph <- function(weekly_vph, corridors) {
    get_cor_weekly_avg_by_hr(weekly_vph, corridors, "vph")
}


get_cor_weekly_paph <- function(weekly_paph, corridors) {
    get_cor_weekly_avg_by_hr(weekly_paph, corridors, "paph")
}


get_cor_weekly_vph_peak <- function(cor_weekly_vph) {
    dfs <- get_cor_monthly_vph_peak(cor_weekly_vph)
    lapply(dfs, function(x) {dplyr::rename(x, Date = Month)})
}


get_weekly_vph_peak <- function(weekly_vph) {
    dfs <- get_monthly_vph_peak(weekly_vph)
    lapply(dfs, function(x) {dplyr::rename(x, Date = Month)})
}


get_monthly_vph_peak <- function(monthly_vph) {

    am <- dplyr::filter(monthly_vph, hour(Hour) %in% AM_PEAK_HOURS) %>%
        mutate(Date = date(Hour)) %>%
        get_daily_avg("vph") %>%
        rename(Month = Date)

    pm <- dplyr::filter(monthly_vph, hour(Hour) %in% PM_PEAK_HOURS) %>%
        mutate(Date = date(Hour)) %>%
        get_daily_avg("vph") %>%
        rename(Month = Date)

    list("am" = as_tibble(am), "pm" = as_tibble(pm))
}


# vph during peak periods
get_cor_monthly_vph_peak <- function(cor_monthly_vph) {

    am <- dplyr::filter(cor_monthly_vph, hour(Hour) %in% AM_PEAK_HOURS) %>%
        mutate(Month = date(Hour)) %>%
        weighted_mean_by_corridor_("Month", corridors, as.name("vph")) %>%
        select(-Zone)

    pm <- dplyr::filter(cor_monthly_vph, hour(Hour) %in% PM_PEAK_HOURS) %>%
        mutate(Month = date(Hour)) %>%
        weighted_mean_by_corridor_("Month", corridors, as.name("vph")) %>%
        select(-Zone)

    list("am" = as_tibble(am), "pm" = as_tibble(pm))
}


# aog during peak periods -- unused
get_cor_monthly_aog_peak <- function(cor_monthly_aog_by_hr) {

    am <- dplyr::filter(cor_monthly_aog_by_hr, hour(Hour) %in% AM_PEAK_HOURS) %>%
        mutate(Date = date(Hour)) %>%
        weighted_mean_by_corridor_("Date", corridors, as.name("aog"), as.name("vol"))

    pm <- dplyr::filter(cor_monthly_aog_by_hr, hour(Hour) %in% PM_PEAK_HOURS) %>%
        mutate(Date = date(Hour)) %>%
        weighted_mean_by_corridor_("Date", corridors, as.name("aog"), as.name("vol"))

    list("am" = as_tibble(am), "pm" = as_tibble(pm))
}


get_cor_weekly_cctv_uptime <- function(daily_cctv_uptime) {

    df <- daily_cctv_uptime %>%
        mutate(DOW = wday(Date),
               Week = week(Date))

    Tuesdays <- get_Tuesdays(df)

    df %>%
        dplyr::select(-Date) %>%
        left_join(Tuesdays) %>%
        group_by(Date, Corridor, Zone_Group) %>%
        summarize(up = sum(up, na.rm = TRUE),
                  num = sum(num, na.rm = TRUE),
                  uptime = sum(up, na.rm = TRUE)/sum(num, na.rm = TRUE),
                  .groups = "drop") %>%
        filter(!is.na(Corridor))
}


get_cor_monthly_cctv_uptime <- function(daily_cctv_uptime) {

    daily_cctv_uptime %>%
        mutate(Month = Date - days(day(Date) - 1)) %>%
        group_by(Month, Corridor, Zone_Group) %>%
        summarize(up = sum(up, na.rm = TRUE),
                  num = sum(num, na.rm = TRUE),
                  uptime = sum(up, na.rm = TRUE)/sum(num, na.rm = TRUE),
                  .groups = "drop") %>%
        filter(!is.na(Corridor))
}


# Convert Monthly data to quarterly for Quarterly Report
get_quarterly <- function(monthly_df, var_, wt_="ones", operation = "avg") {

    if (wt_ == "ones" & !"ones" %in% names(monthly_df)) {
        monthly_df <- monthly_df %>% mutate(ones = 1)
    }

    if (sapply(monthly_df, class)[["Month"]] != "Date") {
        monthly_df <- mutate(monthly_df, Month = as_date(Month))
    }
    var_ <- as.name(var_)
    wt_ <- as.name(wt_)

    quarterly_df <- monthly_df %>%
        group_by(Corridor,
                 Zone_Group,
                 Quarter = as.character(lubridate::quarter(Month, with_year = TRUE)))
    if (operation == "avg") {
        quarterly_df <- quarterly_df %>%
            summarize(!!var_ := weighted.mean(!!var_, !!wt_, na.rm = TRUE),
                      !!wt_ := sum(!!wt_, na.rm = TRUE),
                      .groups = "drop_last")
    } else if (operation == "sum") {
        quarterly_df <- quarterly_df %>%
            summarize(!!var_ := sum(!!var_, na.rm = TRUE),
                      .groups = "drop_last")
    } else if (operation == "latest") {
        quarterly_df <- monthly_df %>%
            group_by(Corridor,
                     Zone_Group,
                     Quarter = as.character(lubridate::quarter(Month, with_year = TRUE))) %>%
            filter(Month == max(Month)) %>%
            dplyr::select(Corridor,
                          Zone_Group,
                          Quarter,
                          !!var_,
                          !!wt_) %>%
            group_by(Corridor, Zone_Group) %>%
            arrange(Zone_Group, Corridor, Quarter)
    }

    quarterly_df %>%
        mutate(lag_ = lag(!!var_),
               delta = ((!!var_) - lag_)/lag_) %>%
        ungroup() %>%
        dplyr::select(-lag_)
}



sigify <- function(df, cor_df, corridors, identifier = "SignalID") {

    per <- intersect(names(df), c("Date", "Month"))

    descs <- corridors %>%
        select(SignalID, Corridor, Description) %>%
        group_by(SignalID, Corridor) %>%
        filter(Description == first(Description)) %>%
        ungroup()

    if (identifier == "SignalID") {
        df_ <- df %>%
            left_join(distinct(corridors, SignalID, Corridor, Name), by = c("SignalID"), relationship = "many-to-many") %>%
            rename(Zone_Group = Corridor, Corridor = SignalID) %>%
            ungroup() %>%
            mutate(Corridor = factor(Corridor)) %>%
            left_join(descs, by = c("Corridor" = "SignalID", "Zone_Group" = "Corridor")) %>%
            mutate(
                Description = coalesce(Description, Corridor),
                Corridor = factor(Corridor),
                Description = factor(Description)
            )
    } else if (identifier == "CameraID") {
        corridors <- rename(corridors, Name = Location)
        df_ <- df %>%
            select(
                -matches("Subcorridor"),
                -matches("Zone_Group")
            ) %>%
            left_join(distinct(corridors, CameraID, Corridor, Name), by = c("Corridor", "CameraID"), relationship = "many-to-many") %>%
            rename(
                Zone_Group = Corridor,
                Corridor = CameraID
            ) %>%
            ungroup() %>%
            mutate(
                Description = coalesce(Description, Corridor))
    } else {
        stop("bad identifier. Must be SignalID (default) or CameraID")
    }

    cor_df_ <- cor_df %>%
        filter(Corridor %in% unique(df_$Zone_Group)) %>%
        mutate(
            Zone_Group = Corridor,
            Description = Corridor) %>%
        select(-matches("Subcorridor"))

    br <- bind_rows(df_, cor_df_) %>%
        mutate(
            Corridor = factor(Corridor),
            Description = factor(Description))

    if ("Zone_Group" %in% names(br)) {
        br <- br %>%
            mutate(Zone_Group = factor(Zone_Group))
    }

    if ("Month" %in% names(br)) {
        br %>% arrange(Zone_Group, Corridor, Month)
    } else if ("Hour" %in% names(br)) {
        br %>% arrange(Zone_Group, Corridor, Hour)
    } else if ("Timeperiod" %in% names(br)) {
        br %>% arrange(Zone_Group, Corridor, Timeperiod)
    } else if ("Date" %in% names(br)) {
        br %>% arrange(Zone_Group, Corridor, Date)
    }
}

