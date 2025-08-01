
# Monthly_Report_Calcs.R

#source("renv/activate.R")

library(yaml)
library(glue)

source("Monthly_Report_Functions.R")


writeLines(glue("\n\n{Sys.time()} Starting Calcs Script"))

if (interactive()) {
    plan(multisession)
} else {
    plan(multicore)
}
usable_cores <- get_usable_cores()
# usable_cores <- 1
doParallel::registerDoParallel(cores = usable_cores)


# aurora_pool <- get_aurora_connection_pool()
# aurora <- get_aurora_connection()

#----- DEFINE DATE RANGE FOR CALCULATIONS ------------------------------------#

start_date <- get_date_from_string(
    conf$start_date, s3bucket = conf$bucket, s3prefix = "mark/split_failures"
)
end_date <- get_date_from_string(conf$end_date)

# Manual overrides
# start_date <- "2020-01-04"
# end_date <- "2020-01-04"

month_abbrs <- get_month_abbrs(start_date, end_date)
#-----------------------------------------------------------------------------#

# # GET CORRIDORS #############################################################

# -- Code to update corridors file/table from Excel file

corridors <- get_corridors(conf$corridors_filename_s3, filter_signals = TRUE)
feather_filename <- sub("\\..*", ".feather", conf$corridors_filename_s3)
write_feather(corridors, feather_filename)

qs_filename <- sub("\\..*", ".qs", conf$corridors_filename_s3)
qsave(corridors, qs_filename)

all_corridors <- get_corridors(conf$corridors_filename_s3, filter_signals = FALSE)
feather_filename <- sub("\\..*", ".feather", paste0("all_", conf$corridors_filename_s3))
write_feather(all_corridors, feather_filename)

qs_filename <- sub("\\..*", ".qs", paste0("all_", conf$corridors_filename_s3))
qsave(all_corridors, qs_filename)


signals_list <- mclapply(
    seq(as_date(start_date), as_date(end_date), by = "1 day"),
    mc.cores = usable_cores,
    FUN = get_signalids_from_s3
) %>%
    unlist() %>%
    unique()


# Most recent detector config. Needed for Watchdog Notes, as feather files
# can't be read from shinyapps.io for some unknown reason.
get_latest_det_config(conf) %>%
    s3write_using(qsave, bucket = conf$bucket, object = "ATSPM_Det_Config_Good_Latest.qs")

# Add partitions that don't already exists to Athena ATSPM table
athena <- get_athena_connection(conf$athena)
partitions <- dbGetQuery(athena, glue("SHOW PARTITIONS {conf$athena$atspm_table}"))$partition
partitions <- sapply(stringr::str_split(partitions, "="), last)
date_range <- seq(as_date(start_date), as_date(end_date), by = "1 day") %>% as.character()
missing_partitions <- setdiff(date_range, partitions)

if (length(missing_partitions) > 10) {
    print(glue("Adding missing partition: date={missing_partitions}"))
    dbExecute(athena, glue("MSCK REPAIR TABLE {conf$athena$atspm_table}"))
} else if (length(missing_partitions) > 0) {
    print("Adding missing partitions:")
    for (date_ in missing_partitions) {
        add_athena_partition(conf$athena, conf$bucket, conf$athena$atspm_table, date_)
    }
}
dbDisconnect(athena)
# source("cel_ped_detectors_shim.R")
