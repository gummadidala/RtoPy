
get_teams_locations <- function(locs, conf) {

    # conn <- get_atspm_connection()

    last_key <- max(aws.s3::get_bucket_df(
        bucket = conf$bucket,
        prefix = "config/maxv_atspm_intersections")$Key)
    sigs <- s3read_using(
        read_csv,
        bucket = conf$bucket,
        object = last_key
    ) %>%
        mutate(SignalID = factor(SignalID),
               Latitude = as.numeric(Latitude),
               Longitude = as.numeric(Longitude)) %>%
        filter(Latitude != 0)

    corridors <- read_corridors(conf$corridors_filename_s3)

    # sf (spatial - point) objects
    locs.sp <- sf::st_as_sf(locs, coords = c("Latitude", "Longitude")) %>%
        sf::st_set_crs(4326)
    sigs.sp <- sf::st_as_sf(sigs, coords = c("Latitude", "Longitude")) %>%
        sf::st_set_crs(4326)

    # Join TEAMS Locations to Tasks via lat/long -- get closest, only if within 100 m

    # Get the row index in locs.sp that is closest to each row in ints.sp
    idx <- apply(st_distance(sigs.sp, locs.sp, byid = TRUE), 1, which.min)
    # Reorder locs.sp so each row is the one corresponding to each ints.sp row
    locs.sp <- locs.sp[idx,]

    # Get vector of distances between closest items (row-wise)
    dist_vector <- sf::st_distance(sigs.sp, locs.sp, by_element = TRUE)
    # Make into a data frame
    dist_dataframe <- data.frame(m = as.integer(dist_vector))

    # Bind data frames to map Locationid (TEAMS) to SignalID (ATSPM) with distance
    bind_cols(sigs.sp, locs.sp, dist_dataframe) %>%
        dplyr::select(
            SignalID,
            PrimaryName,
            SecondaryName,
            m,
            LocationId = `DB Id`,
            `Maintained By`,
            `Custom Identifier`,
            City,
            County) %>%
        filter(m < 100) %>%
        group_by(LocationId) %>%
        filter(m == min(m)) %>%
        mutate(
            guessID = map(`Custom Identifier`, ~str_split(.x, " ")[[1]]),
            guessID = map_chr(guessID, ~unlist(.x[1])),
            good_guess = if_else(guessID==SignalID, 1, 0)) %>%
        filter(good_guess == max(good_guess)) %>%
        filter(row_number() == 1) %>%
        ungroup()
}



tidy_teams_tasks <- function(tasks, bucket, corridors, replicate = FALSE) {

    corridors <- corridors %>%
        select(SignalID, Zone_Group, Zone, Corridor, Subcorridor, Latitude, Longitude, TeamsLocationID)

    # Get tasks and join with corridors
    tasks <- tasks %>%
        dplyr::select(-c(Latitude, Longitude)) %>%
        filter(`Date Reported` < today(),
              (`Date Resolved` < today() | is.na(`Date Resolved`))) %>%
        left_join(corridors, by = c("LocationId" = "TeamsLocationID"), relationship = "many-to-many")

    all_tasks <- tasks %>%
        mutate(
            `Time To Resolve In Days` = floor((`Date Resolved` - `Date Reported`)/ddays(1)),
            `Time To Resolve In Hours` = floor((`Date Resolved` - `Date Reported`)/dhours(1)),

            SignalID = factor(SignalID),
            Task_Type = fct_explicit_na(`Task Type`, na_level = "Unspecified"),
            Task_Subtype = fct_explicit_na(`Task Subtype`, na_level = "Unspecified"),
            Task_Source = fct_explicit_na(`Task Source`, na_level = "Unspecified"),
            Priority = fct_explicit_na(Priority, na_level = "Unspecified"),
            Status = fct_explicit_na(Status, na_level = "Unspecified"),

            `Date Reported` = date(`Date Reported`),
            `Date Resolved` = date(`Date Resolved`),

            All = factor("all")) %>%

        dplyr::select(Due_Date = `Due Date`,
                      Task_Type,  # = `Task Type`,
                      Task_Subtype,  # = `Task Subtype`,
                      Task_Source,  # = `Task Source`,
                      Priority,
                      Status,
                      `Date Reported`,
                      `Date Resolved`,
                      `Time To Resolve In Days`,
                      `Time To Resolve In Hours`,
                      Maintained_by = `Maintained by`,
                      Owned_by = `Owned by`,
                      `Custom Identifier`,
                      `Primary Route`,
                      `Secondary Route`,
                      Created_on = `Created on`,
                      Created_by = `Created by`,
                      Modified_on = `Modified on`,
                      Modified_by = `Modified by`,
                      Latitude,
                      Longitude,
                      SignalID,
                      Zone,
                      Zone_Group,
                      Corridor,
                      All) %>%

        filter(!is.na(`Date Reported`),
               !is.na(Corridor),
               !(Zone_Group == "Zone 7" & `Date Reported` < "2018-05-01"),
               `Date Reported` < today())

    if (replicate) {
        # Replicate RTOP1 and RTOP2 as "All RTOP" and add to tasks
        # Replicate Zones so that Zone_Group == Zone
        # Replicate Zone 7m, 7d tasks as Zone 7
        all_tasks <- all_tasks %>%
            bind_rows(all_tasks %>%
                          filter(Zone_Group == "RTOP1") %>%
                          mutate(Zone_Group = "All RTOP"),
                      all_tasks %>%
                          filter(Zone_Group == "RTOP2") %>%
                          mutate(Zone_Group = "All RTOP"),
                      all_tasks %>%
                          filter(grepl("^Zone", Zone)) %>%
                          mutate(Zone_Group = Zone),
                      all_tasks %>%
                          filter(Zone %in% c("Zone 7m", "Zone 7d")) %>%
                          mutate(Zone_Group = "Zone 7")) %>%
            mutate(Zone_Group = factor(Zone_Group))
    }
    all_tasks
}


get_teams_tasks_from_s3 <- function(
    bucket, archived_tasks_prefix, current_tasks_key, report_start_date) {

    # Keys for past years' tasks. Not the one actively updated.
    teams_keys <- aws.s3::get_bucket_df(
        bucket = bucket,
        prefix = archived_tasks_prefix
    ) %>%
        select(Key) %>%
        unlist(as.list(.))

    col_spec <- cols(
        .default = col_character(),
        `Due Date` = col_datetime(format = "%m/%d/%Y %H:%M:%S %p"),
        `Date Reported` = col_datetime(format = "%m/%d/%Y %H:%M:%S %p"),
        `Date Resolved` = col_datetime(format = "%m/%d/%Y %H:%M:%S %p"),
        `Created on` = col_datetime(format = "%m/%d/%Y %H:%M:%S %p"),
        `Modified on` = col_datetime(format = "%m/%d/%Y %H:%M:%S %p"),
        Latitude = col_double(),
        Longitude = col_double()
    )

    if (length(teams_keys)) {
        archived_tasks <- lapply(teams_keys,
                                 function(key) {
                                     s3read_using(
                                         function(x) read_delim(
                                             x,
                                             delim = ",",
                                             col_types = col_spec,
                                             escape_double = FALSE),
                                         bucket = bucket,
                                         object = key)
                                 }) %>%
            bind_rows() %>%
            select(-X1) %>%
            filter(`Date Reported` >= report_start_date)
    } else {
        archived_tasks <- data.frame()
    }


    # Read the file with current teams tasks
    current_tasks <- s3read_using(
        function(x) read_delim(
            x,
            delim = ",",
            col_types = col_spec,
            escape_double = FALSE),
        bucket = bucket,
        object = current_tasks_key)  %>%
        filter(`Date Reported` >= report_start_date)

    bind_rows(archived_tasks, current_tasks) %>%
        distinct()
}


get_daily_tasks_status <- function(daily_tasks_status, groupings) {

    #' Get number of reported and resolved tasks for each date
    #' plus cumulative reported and resolved, and tasks outstanding as of the date
    #' of a reported or resolved task, grouped by 'groupings'
    #' based on `Reported on` and `Resolved on` date fields.
    #'
    #' @param daily_tasks_status Number reported and resolved by date.
    #'     [Zone_Group|Zone|Corridor|SignalID|Date|n_reported|n_resolved]
    #' @param groupings One of: SignalID, Zone, Zone_Group.
    #' @return A data frame [(grouping)|Date|n_reported|n_resolved].
    #' @examples
    #' get_daily_tasks_status(daily_tasks_status, "SignalID")

    groupings <- sapply(groupings, as.name)
    daily_tasks_status %>%
        group_by(!!!groupings, Date) %>%
        arrange(!!!groupings, Date) %>%
        summarize(
            Reported = sum(n_reported),
            Resolved = sum(n_resolved),
            .groups = "drop_last") %>%
        mutate(
            cum_Reported = cumsum(Reported),
            cum_Resolved = cumsum(Resolved),
            Outstanding = cum_Reported - cum_Resolved) %>%
        ungroup()
}


### Need to expand Zone_Group, Corridor, !!task_param_, seq(...months...) and
### fill reported and resolved with 0, and outstanding with fill_down
### because for some of these types, there may not be any in a month

get_outstanding_tasks_by_month <- function(df, task_param_) {

    #' Aggregate daily outstanding tasks by month by taking the last row
    #' in each month (cumulative numbers for that month)
    #' and assigning it to the first of the month
    #'
    #' @param df data frame of daily data.
    #' @return A data frame [Zone_Group, Corridor|Month|...].
    #' @examples
    #' get_outstanding_tasks_by_month(cor_daily_outstanding_tasks)

    df %>%
        group_by(
            Zone_Group, Corridor, !!task_param_, Month = floor_date(Date, unit = "month")
        ) %>%
        summarize(
            Reported = sum(Reported),
            Resolved = sum(Resolved),
            cum_Reported = cum_Reported[which.max(Date)],
            cum_Resolved = cum_Resolved[which.max(Date)],
            Outstanding = Outstanding[which.max(Date)],
            .groups = "drop") %>%

        # Complete because not all Zone Group, Corridor, Month, param combinations
        # may be represented and we need to fill in zeros for reported and resolved
        # and the cumulative counts filled in
        complete(nesting(Zone_Group, Corridor), !!task_param_, Month,
                 fill = list(Reported = 0, Resolved = 0)) %>%
        arrange(Corridor, Zone_Group, Month) %>%
        group_by(Zone_Group, Corridor, !!task_param_) %>%
        mutate(
            cum_Reported = ifelse(row_number()==1 & is.na(cum_Reported), 0, cum_Reported),
            cum_Reported = runner::fill_run(cum_Reported,
                                               run_for_first = TRUE,
                                               only_within = FALSE)) %>%

        # Fill_run will leave NAs where the Zone Group, Corridor, Month, param combination has all NAs.
        # Need to replace all those with zeros.
        replace_na(list(cum_Reported = 0, cum_Resolved = 0, Outstanding = 0)) %>%

        # Calculate deltas from month to month within
        # Zone Group, Corridor, Month, param combinations
        group_by(Corridor, Zone_Group, !!task_param_) %>%
        mutate(
            delta.rep = (Reported - lag(Reported))/lag(Reported),
            delta.res = (Resolved - lag(Resolved))/lag(Resolved),
            delta.out = (Outstanding - lag(Outstanding))/lag(Outstanding)
        ) %>%
        ungroup()
}


get_sig_from_monthly_tasks <- function(monthly_data, all_corridors) {

    #' Filter for Zone_Group = Corridor for all Corridors
    #' for use in sig$mo$tasks_...
    #'
    #' @param monthly_data cor_monthly data, e.g., cor_monthly_priority
    #' @param all_corridors All corridors data frame from S3
    #' @return A data frame, monthly data by corridor only, not zone or zone_group

    # This data frame has duplicates for each corridor for RTOP1, All RTOP, etc.
    # Take only one (in this case, the minimum as.character(Zone_Group),
    # but it doesn't matter because we're overwriting the Zone_Group anyway

    monthly_data %>%
        group_by(Corridor) %>%
        filter(as.character(Zone_Group) == min(as.character(Zone_Group))) %>%
        mutate(Zone_Group = Corridor) %>%
        filter(Corridor %in% all_corridors$Corridor) %>%
        ungroup()
}


get_outstanding_tasks_by_param <- function(teams, task_param, report_start_date) {

    #' Get all reported, resolved, cumulative reported, cumulative resolved
    #' and outstanding TEAMS tasks by day, month, corridor/zone group (cor)
    #' and corridor only (sig) by task parameter
    #'
    #' @param teams TEAMS data set where tasks are replicated for All RTOP, etc.
    #' @param task_param One of: Priority, Type, Subtype, ...
    #' @param report_start_date the report start date as a date type
    #' @return A list of four data frames: cor_daily, sig_daily, cor_monthly, sig_monthly
    #' @examples
    #' get_outstanding_tasks_by_param(teams, "Priority", ymd("2018-07-01"))

    task_param_ <- as.name(task_param)
    # -----------------------------------------------------------------------------
    # Tasks Reported by Day, by SignalID
    tasks_reported_by_day <- teams %>%
        group_by(Zone_Group, Zone, Corridor, SignalID, !!task_param_, Date = `Date Reported`) %>%
        count() %>%
        ungroup() %>%
        arrange(Zone_Group, Zone, Corridor, SignalID, Date)

    # Tasks Resolved by Day, by SignalID
    tasks_resolved_by_day <- teams %>%
        filter(!is.na(`Date Resolved`)) %>%
        group_by(Zone_Group, Zone, Corridor, SignalID, !!task_param_, Date = `Date Resolved`) %>%
        count() %>%
        ungroup() %>%
        arrange(Zone_Group, Zone, Corridor, SignalID, Date)

    # -----------------------------------------------------------------------------
    # Tasks Reported and Resolved by Day, by SignalID
    daily_tasks_status <- full_join(
        tasks_reported_by_day,
        tasks_resolved_by_day,
        by = c("Zone_Group", "Zone", "Corridor", "SignalID", task_param, "Date"),
        suffix = c("_reported", "_resolved"),
        relationship = "many-to-many"
    ) %>%
        ungroup() %>%
        replace_na(list(n_reported = 0, n_resolved = 0))

    # Tasks Reported and Resolved by Day, by Corridor
    cor_daily_tasks_status <- get_daily_tasks_status(
        daily_tasks_status,
        groupings = c("Zone_Group", "Zone", "Corridor", task_param)) %>%
        filter(!Zone_Group %in% c("All RTOP", "RTOP1", "RTOP2", "Zone 7")) %>%
        select(-Zone)

    # Tasks Reported and Resolved by Day, by Zone Group
    zone_group_daily_tasks_status <- daily_tasks_status %>%
        get_daily_tasks_status(groupings = c("Zone_Group", task_param)) %>%
        mutate(Corridor = Zone_Group)


    cor_daily_outstanding_tasks <- bind_rows(
        cor_daily_tasks_status,
        zone_group_daily_tasks_status) %>%
        mutate(Zone_Group = factor(Zone_Group),
               Corridor = factor(Corridor)) %>%
        filter(Date >= report_start_date,
               Date <= today())

    sig_daily_outstanding_tasks <- get_sig_from_monthly_tasks(
        cor_daily_tasks_status, all_corridors) %>%
        filter(Date >= report_start_date,
               Date <= today())

    cor_monthly_outstanding_tasks <- get_outstanding_tasks_by_month(cor_daily_outstanding_tasks, task_param_)
    sig_monthly_outstanding_tasks <- get_outstanding_tasks_by_month(sig_daily_outstanding_tasks, task_param_)

    # Return Value
    list("cor_daily" = cor_daily_outstanding_tasks,
         "sig_daily" = sig_daily_outstanding_tasks,
         "cor_monthly" = cor_monthly_outstanding_tasks,
         "sig_monthly" = sig_monthly_outstanding_tasks)
}





get_outstanding_tasks_by_day_range <- function(teams, report_start_date, first_of_month) {

    #' Get number of tasks that have been outstanding for
    #' 0-45 days, 45-90
    #'
    #' @param teams TEAMS data set where tasks are replicated for All RTOP, etc.
    #' @param report_start_date ...
    #' @param first_of_month date for the month as the first of the month. for past
    #' months, all resolved dates after the end of this month are considered NA
    #' for that month's oustanding tasks, as a date object
    #' @return A data frame [Zone_Group|Corridor|Month|`0-45`|`45-90`|over90|mttr]
    #' @examples
    #' get_outstanding_tasks_by_day_range(ymd("2018-07-01"), teams)

    last_day <- first_of_month + months(1) - days(1)

    mttr <- teams %>%
        filter(`Date Reported` <= last_day,
               `Date Resolved` < last_day,
               `Date Reported` > report_start_date,
               !is.na(`Date Resolved`)) %>%
        mutate(
            ttr = as.numeric(`Date Resolved` - `Date Reported`, units = "days"),
            Month = first_of_month) %>%
        group_by(Zone_Group, Zone, Corridor, Month) %>%
        summarize(
            mttr = mean(ttr),
            num_resolved = n(),
            .groups = "drop")

    over45 <- teams %>%
        filter(`Date Reported` < last_day - days(45),
               (is.na(`Date Resolved`) | `Date Resolved` > last_day)) %>%
        group_by(Zone_Group, Zone, Corridor) %>%
        count() %>%
        rename(over45 = n) %>%
        ungroup()

    over90 <- teams %>%
        filter(`Date Reported` < last_day - days(90),
               (is.na(`Date Resolved`) | `Date Resolved` > last_day)) %>%
        group_by(Zone_Group, Zone, Corridor) %>%
        count() %>%
        rename(over90 = n) %>%
        ungroup()

    all <- teams %>%
        filter((is.na(`Date Resolved`) | `Date Resolved` > last_day),
               `Date Reported` <= last_day) %>%
        group_by(Zone_Group, Zone, Corridor) %>%
        count() %>%
        rename(all_outstanding = n) %>%
        ungroup()

    outst <- list(over45, over90, all, mttr) %>%
        reduce(left_join, by = c("Zone_Group", "Zone", "Corridor"), relationship = "many-to-many") %>%
        replace_na(list(over45 = 0,
                        over90 = 0,
                        all_outstanding = 0)) %>%
        transmute(
            Zone_Group, Corridor,
            Month = first_of_month,
            `0-45` = all_outstanding - over45,
            `45-90` = over45 - over90,
            over45,
            over90,
            num_outstanding = all_outstanding,
            num_resolved,
            mttr)

    bind_rows(
        outst %>%
            filter(!Zone_Group %in% c("All RTOP", "RTOP1", "RTOP2", "Zone 7")),
        outst %>%
            mutate(Corridor = Zone_Group) %>%
            group_by(Zone_Group, Corridor, Month) %>%
            summarize(
                `0-45` = sum(`0-45`),
                `45-90` = sum(`45-90`),
                over45 = sum(over45),
                over90 = sum(over90),
                mttr = weighted.mean(mttr, num_resolved, na.rm = TRUE),
                num_outstanding = sum(num_outstanding),
                num_resolved = sum(num_resolved),
                .groups = "drop_last")
    ) %>%
        mutate(Zone_Group = factor(Zone_Group),
               Corridor = factor(Corridor))
}



get_outstanding_events <- function(teams, group_var, spatial_grouping="Zone_Group") {

    # group_var is either All, Type, Subtype, ...
    # spatial grouping is either Zone_Group or Corridor
    rep <- teams %>%
        filter(!is.na(`Date Reported`)) %>%
        mutate(Month = floor_date(`Date Reported`, "1 month")) %>%
        arrange(Month) %>%
        group_by_(spatial_grouping, group_var, quote(Month)) %>%
        summarize(Rep = n(),
                  .groups = "drop_last") %>%
        #group_by_(spatial_grouping, group_var) %>%
        mutate(cumRep = cumsum(Rep))

    res <- teams %>%
        filter(!is.na(`Date Resolved`)) %>%
        mutate(Month = `Date Resolved` - days(day(`Date Resolved`) -1)) %>%
        arrange(Month) %>%
        group_by_(spatial_grouping, group_var, quote(Month)) %>%
        summarize(Res = n(),
                  .groups = "drop_last") %>%
        #group_by_(spatial_grouping, group_var) %>%
        mutate(cumRes = cumsum(Res))

    left_join(rep, res, relationship = "many-to-many") %>%
        fill(cumRes, .direction = "down") %>%
        replace_na(list(Rep = 0, cumRep = 0,
                        Res = 0, cumRes = 0)) %>%
        group_by_(spatial_grouping, group_var) %>%
        mutate(outstanding = cumRep - cumRes) %>%
        ungroup()
}
