
source("Monthly_Report_Functions.R")
source("Classes.R")

# vpd <- structure(metrics[["daily_traffic_volume"]], class = "metric")
# throughput <- structure(metrics[["throughput"]], class = "metric")
# aog <- arrivals_on_green <- structure(metrics[["arrivals_on_green"]], class = "metric")
# progression_ratio <- structure(metrics[["progression_ratio"]], class = "metric")
# queue_spillback_rate <- structure(metrics[["queue_spillback_rate"]], class = "metric")
# peak_period_split_failures <- structure(metrics[["peak_period_split_failures"]], class = "metric")
# off_peak_split_failures <- structure(metrics[["off_peak_split_failures"]], class = "metric")
# travel_time_index <- structure(metrics[["travel_time_index"]], class = "metric")
# planning_time_index <- structure(metrics[["planning_time_index"]], class = "metric")
# average_speed <- structure(metrics[["average_speed"]], class = "metric")
# daily_pedestrian_pushbuttons <- structure(metrics[["daily_pedestrian_pushbuttons"]], class = "metric")
# detector_uptime <- structure(metrics[["detector_uptime"]], class = "metric")
# ped_button_uptime <- structure(metrics[["ped_button_uptime"]], class = "metric")
# cctv_uptime <- structure(metrics[["cctv_uptime"]], class = "metric")
# comm_uptime <- structure(metrics[["comm_uptime"]], class = "metric")
# rsu_uptime <- structure(metrics[["rsu_uptime"]], class = "metric")

# [ ] No TEAMS
# [x] No Peak Period Volumes
# [ ] Make a place on S3 for images

get_quarterly_data <- function() {

    conn <- get_aurora_connection()

    df <- lapply(
        list(vpd,
             am_peak_vph,
             pm_peak_vph,
             throughput,
             arrivals_on_green,
             progression_ratio,
             queue_spillback_rate,
             peak_period_split_failures,
             off_peak_split_failures,
             travel_time_index,
             planning_time_index,
             average_speed,
             daily_pedestrian_pushbuttons,
             detector_uptime,
             ped_button_uptime,
             cctv_uptime,
             comm_uptime,
             tasks_reported,
             tasks_resolved,
             tasks_outstanding,
             tasks_over45,
             tasks_mttr),
        function(metric) {
            print(metric$label)
            dbReadTable(conn, glue("cor_qu_{metric$table}")) %>%
                mutate(Metric = metric$label) %>%
                rename(value = metric$variable)
        }
    ) %>% bind_rows() %>%
        filter(Zone_Group != "NA") %>%
        as_tibble() %>%
        select(Metric, Zone_Group, Corridor, Quarter, value, ones, vol, num) %>%
        mutate(weight = coalesce(as.numeric(ones), as.numeric(num), as.numeric(vol))) %>%
        select(-c(ones, vol, num)) %>%
        filter(grepl("\\d{4}.\\d{1}", Quarter)) %>%
        separate(Quarter, into = c("yr", "qu"), sep = "\\.") %>%
        mutate(date = ymd(glue("{yr}-{(as.integer(qu)-1)*3+1}-01")),  # date - start of quarter
               date = date + months(3) - days(1)) %>%  # date - end of quarter
        mutate(Quarter = as.character(lubridate::quarter(date, with_year = TRUE, fiscal_start = 7)))

    dfz <- df %>%
        filter(as.character(Zone_Group) == as.character(Corridor)) %>%
        rename(District = Zone_Group) %>%
        arrange(District, Quarter) %>%
        select(District, date, Quarter, Metric, value)


    regions <- unique(dfz$District)

    dfd <- df %>%
        filter(as.character(Zone_Group) != as.character(Corridor)) %>%
        rename(District = Zone_Group) %>%
        filter(!District %in% regions) %>%
        group_by(District, date, Quarter, Metric) %>%
        summarize(value = weighted.mean(value, weight), .groups = "drop") %>%
        arrange(District, Quarter) %>%
        select(District, date, Quarter, Metric, value)

    bind_rows(dfz, dfd) %>%
        arrange(District, Quarter)
}

write_quarterly_data <- function(qdata, filename = "quarterly_data.csv") {
    readr::write_csv(qdata, filename)
}

#---------- Bottlenecks -----------

get_quarterly_bottlenecks <- function() {
    tmcs <- s3read_using(
        read_excel,
        bucket = conf$bucket,
        object = "Corridor_TMCs_Latest.xlsx"
    )

    bottlenecks <- lapply(c("2020-10-01", "2020-11-01", "2020-12-01"), function(x) {
        s3read_using(
            read_parquet,
            bucket = conf$bucket,
            object = glue("mark/bottlenecks/date={x}/bottleneck_rankings_{x}.parquet"))
    }) %>% bind_rows()


    bottlenecks$length_current <- purrr::map(bottlenecks$tmcs_current, function(x) {
        sum(tmcs[tmcs$tmc %in% x,]$length)
    }) %>% unlist()

    bottlenecks$length_1yr <- purrr::map(bottlenecks$tmcs_1yr, function(x) {
        sum(tmcs[tmcs$tmc %in% x,]$length)
    }) %>% unlist()

    bottlenecks %>% select(-c(tmcs_current, tmcs_1yr))
    }

write_quarterly_bottlenecks <- function(bottlenecks, filename = "quarterly_bottlenecks.csv") {
    readr::write_csv(bottlenecks, filename)
}

df <- get_quarterly_data()
write_quarterly_data(df)
