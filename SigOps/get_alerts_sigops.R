
source("Monthly_Report_Package_init.R")
source("write_sigops_to_db.R")

read_zipped_feather <- function(x) {
    read_feather(unzip(x))
}

log_path <- "./logs"
if (!dir.exists(log_path)) {
    dir.create(log_path, recursive = TRUE)
}

conf <- read_yaml("Monthly_Report.yaml")

get_alerts <- function(conf) {

    objs <- aws.s3::get_bucket(bucket = conf$bucket,
                               prefix = 'sigops/watchdog/')
    alerts <- lapply(objs, function(obj) {
        key <- obj$Key
        print(key)
        f <- NULL
        if (endsWith(key, "feather.zip")) {
            f <- read_zipped_feather
        } else if (endsWith(key, "parquet") & !endsWith(key, "alerts.parquet")) {
            f <- read_parquet
        }
        if (!is.null(f)) {
            aws.s3::s3read_using(FUN = f,
                                 bucket = conf$bucket,
                                 object = key) %>%
                as_tibble() %>%
                mutate(SignalID = factor(SignalID),
                       Detector = factor(Detector),
                       Date = date(Date))
        }
    }) %>% bind_rows() %>%
        filter(!is.na(Corridor)) %>%
        replace_na(replace = list(Detector = factor(0), CallPhase = factor(0))) %>%
        transmute(
            Zone_Group = factor(Zone_Group),
            Zone = factor(Zone),
            Corridor = factor(Corridor),
            SignalID = factor(SignalID),
            CallPhase = factor(CallPhase),
            Detector = factor(Detector),
            Date = Date,
            Name = as.character(Name),
            Alert = factor(Alert),
            ApproachDesc) %>%
        filter(Date > today() - days(180)) %>%
        distinct() %>%
        arrange(Alert, SignalID, CallPhase, Detector, Date)


    # bind_rows(alerts, rms_alerts) %>%

    alerts %>%
        group_by(
            Zone_Group, Zone, SignalID, CallPhase, Detector, Alert
        ) %>%
        mutate(
            start_streak = ifelse(
                as.integer(Date - lag(Date), unit = "days") > 1 |
                    Date == min(Date),
                Date,
                NA)) %>%
        fill(start_streak) %>%
        mutate(streak = streak_run(start_streak, k = 90)) %>%
        ungroup() %>%
        select(-start_streak)
}


alerts <- get_alerts(conf)


# Upload to S3 Bucket: sigops/watchdog/
tryCatch({
    s3write_using(
        alerts,
        qsave,
        bucket = conf$bucket,
        object = "sigops/watchdog/alerts.qs",
        opts = list(multipart = TRUE))

    s3write_using(
        alerts,
        write_parquet,
        bucket = conf$bucket,
        object = "sigops/watchdog/alerts.parquet",
        opts = list(multipart = TRUE))

    write(
        glue(paste0(
            "{format(now(), '%F %H:%M:%S')}|SUCCESS|get_alerts.R|get_alerts|Line 173|",
            "Uploaded {conf$bucket}/sigops/watchdog/alerts.qs")),
        file.path(log_path, glue("get_alerts_{today()}.log")),
        append = TRUE
    )
}, error = function(e) {
    write(
        glue("{format(now(), '%F %H:%M:%S')}|ERROR|get_alerts.R|get_alerts|Line 173|Failed to upload to S3 - {e}"),
        file.path(log_path, glue("get_alerts_{today()}.log")),
        append = TRUE
    )
})


# Write to Angular Database for New SigOps
tryCatch({
    conn <- keep_trying(func = get_aurora_connection, n_tries = 5)
    dbExecute(conn, "TRUNCATE TABLE WatchdogAlerts")
    load_bulk_data(conn, "WatchdogAlerts", alerts)
    dbDisconnect(conn)

    write(
        paste0(
            glue("{format(now(), '%F %H:%M:%S')}|SUCCESS|get_alerts.R|get_alerts|Line 217|"),
            "Wrote to Angular Aurora Database: WatchdogAlerts table"),
        file.path(log_path, glue("get_alerts_{today()}.log")),
        append = TRUE
    )
}, error = function(e) {
    write(
        glue("{format(now(), '%F %H:%M:%S')}|ERROR|get_alerts.R|get_alerts|Line 217|Failed to write to Angular Database - {e}"),
        file.path(log_path, glue("get_alerts_{today()}.log")),
        append = TRUE
    )
})

