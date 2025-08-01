
get_corridors <- function(corr_fn, filter_signals = TRUE) {

    # Keep this up to date to reflect the Corridors_Latest.xlsx file
    cols <- list(SignalID = "numeric", #"text",
                 Zone_Group = "text",
                 Zone = "text",
                 Contract = "text",
                 District = "text",
                 Agency = "text",
                 `Main Street Name` = "text",
                 `Side Street Name` = "text",
                 Milepost = "numeric",
                 Asof = "date",
                 Duplicate = "numeric",
                 Include = "logical",
                 Modified = "date",
                 Note = "text",
                 Latitude = "numeric",
                 Longitude = "numeric",
                 County = "text",
                 City = "text",
                 Priority = "text",
                 Classification = "text")

    # Set the column types in the excel file as defined above
    x <- readxl::read_xlsx(corr_fn)
    col_types <- cols[names(x)]

    # Set any other columns (not defined above) to "text"
    col_types[sapply(col_types, is.null)] <- "text"

    df <- readxl::read_xlsx(corr_fn, col_types = unlist(col_types)) %>%

        rename(Zone_Group = Contract, Zone = District) %>%

        # Get the last modified record for the Signal|Zone|Corridor combination
        replace_na(replace = list(Modified = ymd("1900-01-01"))) %>%
        group_by(SignalID, Zone, Corridor) %>%
        filter(Modified == max(Modified)) %>%
        ungroup() %>%

        filter(!is.na(Corridor))

    # Gracefully handle trailing spaces in Corridors field
    df <- df %>% mutate(across(where(is.character), stringr::str_trim))

    # if filter_signals == FALSE, this creates all_corridors, which
    #   includes corridors without signals
    #   which is used for manual ped/det uptimes and camera uptimes
    if (filter_signals) {
        df <- df %>%
            filter(
                SignalID > 0,
                Include == TRUE)
    }

    df %>%
        tidyr::unite(Name, c(`Main Street Name`, `Side Street Name`), sep = ' @ ') %>%
        transmute(SignalID = factor(SignalID),
                  Zone = as.factor(Zone),
                  Zone_Group = factor(Zone_Group),
                  Corridor = as.factor(Corridor),
                  Subcorridor = as.factor(Subcorridor),
                  Milepost = as.numeric(Milepost),
                  Agency,
                  Name,
                  Asof = date(Asof),
                  Latitude,
                  Longitude,
                  TeamsLocationID = stringr::str_trim(`TEAMS GUID`),
                  Priority,
                  Classification) %>%
        mutate(Description = paste(SignalID, Name, sep = ": "))

}



check_corridors <- function(corridors) {
    distinct_corridors <- corridors %>% distinct(Zone, Zone_Group, Corridor)

    # Check 1: Same Corridor in multiple Zones
    corridors_in_multiple_zones <- distinct_corridors %>%
        group_by(Corridor) %>%
        count() %>%
        filter(n>1) %>%
        pull(Corridor)

    check1 <- distinct_corridors %>%
        filter(Corridor %in% corridors_in_multiple_zones) %>%
        arrange(Corridor, Zone, Zone_Group)


    # Check 2: Corridors with different cases (e.g., one line has incorrect capitalization.)
    corridors_with_case_mismatches <- corridors %>%
        distinct(Corridor) %>%
        group_by(corridor = tolower(Corridor)) %>%
        count() %>% filter(n>1) %>%
        pull(corridor)

    check2 <- distinct_corridors %>%
        filter(tolower(Corridor) %in% corridors_with_case_mismatches) %>%
        arrange(Corridor, Zone, Zone_Group)


    # Check 3: Corridors with simliar names. This is just a warning. Many are correct.
    # unique_corridors <- unique(as.character(corridors$Corridor))
    # m <- stringsimmatrix(unique_corridors, unique_corridors, method = "jw")
    # m[!upper.tri(m)] <- 0
    #
    # max_values <- apply(m, 1, max)
    # max_indexes <- apply(m, 1, which.max)
    #
    # similar_corridors <- data.frame(
    #     Corridor = unique_corridors,
    #     Closest_match = unique_corridors[max_indexes],
    #     Match_Score = max_values
    # )
    #
    # check3 <- similar_corridors %>%
    #     filter(Match_Score > 0.96) %>%
    #     arrange(desc(Match_Score)) %>%
    #     left_join(distinct_corridors, by = c("Corridor")) %>%
    #     left_join(distinct_corridors, by = c("Closest_match" = "Corridor"), suffix = c("_corr", "_match"))


    if (nrow(check1)) {
        print("Corridors in multiple zones:")
        print(check1)
    }
    if (nrow(check2))  {
        print("Same corridor, different cases:")
        print(check2)
    }
    # print(check3)

    pass_validation <- !(nrow(check1) | nrow(check2))
}

get_cam_config <- function(object, bucket, corridors) {

    cam_config0 <- aws.s3::s3read_using(
        read_excel,
        object = object,
        bucket = bucket
    ) %>%
        filter(Include == TRUE) %>%
        transmute(
            CameraID = factor(CameraID),
            Location,
            SignalID = factor(`MaxView ID`),
            As_of_Date = date(As_of_Date)) %>%
        distinct()

    corrs <- corridors %>%
        select(SignalID, Zone_Group, Zone, Corridor, Subcorridor)


    left_join(corrs, cam_config0, relationship = "many-to-many") %>%
        filter(!is.na(CameraID)) %>%
        mutate(Description = paste(CameraID, Location, sep = ": ")) %>%
        arrange(Zone_Group, Zone, Corridor, CameraID)
}



get_ped_config_ <- function(bucket) {

    function(date_) {
        date_ <- max(date_, ymd("2019-01-01"))

        s3key <- glue("config/maxtime_ped_plans/date={date_}/MaxTime_Ped_Plans.csv")
        s3bucket <- bucket

        if (nrow(aws.s3::get_bucket_df(s3bucket, s3key)) > 0) {
            col_spec <- cols_only(
                # .default = col_character(),
                SignalID = col_character(),
                IP = col_character(),
                PrimaryName = col_character(),
                SecondaryName = col_character(),
                Detector = col_character(),
                CallPhase = col_character())

            s3read_using(function(x) read_csv(x, col_types = col_spec) %>% suppressMessages(),
                         object = s3key,
                         bucket = s3bucket) %>%

                # New. A bit of a hack to account for multiple detectors configured
                group_by(SignalID, Detector) %>% filter(row_number() == 1) %>% ungroup() %>%

                transmute(SignalID = factor(SignalID),
                          Detector = factor(Detector),
                          CallPhase = factor(CallPhase)) %>%
                distinct()
        } else {
            data.frame()
        }
    }
}



get_ped_config_cel_ <- function(bucket, conf_athena) {

    function(date_) {

        ped_start_date <- floor_date(as_date(date_), unit = "months") - months(6)

        arrow::open_dataset(sources = glue("s3://{bucket}/config/cel_ped_detectors/")) %>%
            filter(as_date(date) >= ped_start_date) %>%
            distinct(SignalID, Detector, CallPhase) %>%
            collect()

    }
}



get_ped_config <- get_ped_config_cel_(conf$bucket)



## -- --- Adds CountPriority from detector config file --------------------- -- ##
## This determines which detectors to use for counts when there is more than
## one detector in a lane, such as for video, Gridsmart and Wavetronix Matrix

# This is a "function factory"
# It is meant to be used to create a get_det_config function that takes only the date:
# like: get_det_config <- get_det_config_(conf$bucket, "atspm_det_config_good")
get_det_config_  <- function(bucket, folder) {

    function(date_) {
        read_det_config <- function(s3object, s3bucket) {
            aws.s3::s3read_using(read_feather, object = s3object, bucket = s3bucket)
        }

        s3bucket <- bucket
        s3prefix = glue("config/{folder}/date={date_}")

        # Are there any files for this date?
        s3objects <- aws.s3::get_bucket(
            bucket = s3bucket,
            prefix = s3prefix)

        # If the s3 object exists, read it and return the data frame
        if (length(s3objects) == 1) {
            read_det_config(s3objects[1]$Contents$Key, s3bucket) %>%

                # New. A bit of a hack to account for multiple detectors configured
                group_by(SignalID, Detector) %>% filter(row_number() == 1) %>% ungroup() %>%

                mutate(SignalID = as.character(SignalID),
                       Detector = as.integer(Detector),
                       CallPhase = as.integer(CallPhase))

            # If the s3 object does not exist, but where there are objects for this date,
            # read all files and bind rows (for when multiple ATSPM databases are contributing)
        } else if (length(s3objects) > 0) {
            lapply(s3objects, function(x) {
                read_det_config(x$Key, s3bucket)})  %>%
                rbindlist() %>% as_tibble() %>%

                # New. A bit of a hack to account for multiple detectors configured
                group_by(SignalID, Detector) %>% filter(row_number() == 1) %>% ungroup() %>%

                mutate(SignalID = as.character(SignalID),
                       Detector = as.integer(Detector),
                       CallPhase = as.integer(CallPhase))
        } else {
            stop(glue("No detector config file for {date_}"))
        }
    }
}

get_det_config <- get_det_config_(conf$bucket, "atspm_det_config_good")


get_det_config_aog <- function(date_) {

    get_det_config(date_) %>%
        filter(!is.na(Detector)) %>%
        mutate(AOGPriority =
                   dplyr::case_when(
                       grepl("Exit", DetectionTypeDesc)  ~ 0,
                       grepl("Advanced Count", DetectionTypeDesc) ~ 1,
                       TRUE ~ 2)) %>%
        filter(AOGPriority < 2) %>%
        group_by(SignalID, CallPhase) %>%
        filter(AOGPriority == min(AOGPriority)) %>%
        ungroup() %>%

        transmute(SignalID = factor(SignalID),
                  Detector = factor(Detector),
                  CallPhase = factor(CallPhase),
                  TimeFromStopBar = TimeFromStopBar,
                  Date = date(date_))
}


get_det_config_qs <- function(date_) {

    # Detector config
    dc <- get_det_config(date_) %>%
        filter(grepl("Advanced Count", DetectionTypeDesc) |
                   grepl("Advanced Speed", DetectionTypeDesc)) %>%
        filter(!is.na(DistanceFromStopBar)) %>%
        filter(!is.na(Detector)) %>%

        transmute(SignalID = factor(SignalID),
                  Detector = factor(Detector),
                  CallPhase = factor(CallPhase),
                  TimeFromStopBar = TimeFromStopBar,
                  Date = date(date_))

    # Bad detectors
    bd <- s3read_using(
        read_parquet,
        bucket = conf$bucket,
        object = glue("mark/bad_detectors/date={date_}/bad_detectors_{date_}.parquet")) %>%
        transmute(SignalID = factor(SignalID),
                  Detector = factor(Detector),
                  Good_Day)

    # Join to take detector config for only good detectors for this day
    left_join(dc, bd, by=c("SignalID", "Detector")) %>%
        filter(is.na(Good_Day)) %>% select(-Good_Day)

}


get_det_config_sf <- function(date_) {

    get_det_config(date_) %>%
        filter(grepl("Stop Bar Presence", DetectionTypeDesc)) %>%
        filter(!is.na(Detector)) %>%


        transmute(SignalID = factor(SignalID),
                  Detector = factor(Detector),
                  CallPhase = factor(CallPhase),
                  TimeFromStopBar = TimeFromStopBar,
                  Date = date(date_))
}


get_det_config_vol <- function(date_) {

    get_det_config(date_) %>%
        transmute(SignalID = factor(SignalID),
                  Detector = factor(Detector),
                  CallPhase = factor(CallPhase),
                  CountPriority = as.integer(CountPriority),
                  TimeFromStopBar = TimeFromStopBar,
                  Date = date(date_)) %>%
        group_by(SignalID, CallPhase) %>%
        mutate(minCountPriority = min(CountPriority, na.rm = TRUE)) %>%
        ungroup() %>%
        filter(CountPriority == minCountPriority)
}




get_latest_det_config <- function(conf) {

    date_ <- today(tzone = "America/New_York")

    # Get most recent detector config file, start with today() and work backward
    while (TRUE) {
        x <- aws.s3::get_bucket(
            bucket = conf$bucket,
            prefix = glue("config/atspm_det_config_good/date={format(date_, '%F')}"))
        if (length(x)) {
            det_config <- s3read_using(arrow::read_feather,
                                       bucket = conf$bucket,
                                       object = x$Contents$Key)
            break
        } else {
            date_ <- date_ - days(1)
        }
    }
    det_config
}
