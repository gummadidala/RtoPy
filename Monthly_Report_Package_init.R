
# Monthly_Report_Package_init.R

#source("renv/activate.R")

source("Monthly_Report_Functions.R")
source("Database_Functions.R")


print(glue("{Sys.time()} Starting Package Script"))

if (interactive()) {
    plan(multisession)
} else {
    plan(multicore)
}

usable_cores <- get_usable_cores()
doParallel::registerDoParallel(cores = usable_cores)


corridors <- get_corridors(conf$corridors_filename_s3, filter_signals = TRUE)
all_corridors <- get_corridors(conf$corridors_filename_s3, filter_signals = FALSE)


signals_list <- unique(corridors$SignalID)

subcorridors <- corridors %>%
    filter(!is.na(Subcorridor)) %>%
    select(-Zone_Group) %>%
    rename(
        Zone_Group = Zone,
        Zone = Corridor,
        Corridor = Subcorridor)


conn <- get_athena_connection(conf$athena)

cam_config <- get_cam_config(
    object = conf$cctv_config_filename,
    bucket = conf$bucket,
    corridors = all_corridors)


usable_cores <- get_usable_cores()
doParallel::registerDoParallel(cores = usable_cores)


# # ###########################################################################

# # Package everything up for Monthly Report back 18 months. 5 quarters.

#----- DEFINE DATE RANGE FOR CALCULATIONS ------------------------------------#

report_end_date <- Sys.Date() - days(1)
report_start_date <- floor_date(report_end_date, unit = "months") - months(18)

if (conf$report_end_date == "yesterday") {
    report_end_date <- Sys.Date() - days(1)
} else {
    report_end_date <- conf$report_end_date
}

if (conf$calcs_start_date == "auto") {
    first_missing_date <- get_date_from_string(
        "first_missing", table_include_regex_pattern = "sig_dy_cu", exceptions = 0
    )
    calcs_start_date <- floor_date(first_missing_date, unit = "months")
    if (day(first_missing_date) <= 7) {
        calcs_start_date <- calcs_start_date - months(1)
    }
} else {
    calcs_start_date <- conf$calcs_start_date
}

round_to_tuesday <- function(date_) {
    if (is.null(date_)) {
        return (NULL)
    }
    if (is.character(date_)) {
        date_ <- ymd(date_)
    }
    date_ - wday(date_) + 3
}

wk_calcs_start_date <- round_to_tuesday(calcs_start_date)

dates <- seq(ymd(report_start_date), ymd(report_end_date), by = "1 month")
month_abbrs <- get_month_abbrs(report_start_date, report_end_date)

report_start_date <- as.character(report_start_date)
report_end_date <- as.character(report_end_date)
print(glue("{Sys.time()} Week Calcs Start Date: {wk_calcs_start_date}"))
print(glue("{Sys.time()} Calcs Start Date: {calcs_start_date}"))
print(glue("{Sys.time()} Report End Date: {report_end_date}"))

date_range <- seq(ymd(report_start_date), ymd(report_end_date), by = "1 day")
date_range_str <- paste0("{", paste0(as.character(date_range), collapse = ","), "}")

#options(warn = 2) # Turn warnings into errors we can run a traceback on. For debugging only.
