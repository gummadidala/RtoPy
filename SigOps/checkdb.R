
source("Monthly_Report_Package_init.R")

library("future.apply")
plan(multisession) ## Run in parallel on local computer

conn <- get_aurora_connection()

dt_fields <- c("Month", "Date", "Hour", "Timeperiod")

tables <- dbListTables(conn)
tables <- tables[grepl("^[a-z]{3}_.*", tables)]
for (pattrn in c("_qu_", "_udc", "_safety", "_maint", "_ops", "summary_data")) {
    tables <- tables[!grepl(pattrn, tables)]
}

if (dir.exists("tables")) unlink("tables", recursive = TRUE)
dir.create("tables")

invisible({
    future_lapply(tables, function(tabl) {
        conn <- get_aurora_connection()
        # cat(tabl, '\n')
        fields <- dbListFields(conn, tabl)
        dt_field <- intersect(dt_fields, fields)
        start_date <- as_date(Sys.Date()) - days(60)
        try(
            if (!file.exists(glue("tables/{tabl}.parquet"))) {
            dbGetQuery(conn, glue(paste(
    	    "SELECT CAST({dt_field} AS DATE) AS Date, count(*) as Records",
    	    "FROM {tabl} WHERE {dt_field} > '{start_date}'",
    	    "GROUP BY CAST({dt_field} AS DATE)"))
    	) %>%
    	    tibble::add_column(table = tabl, .before = 1) %>%
                arrange(Date) %>%
                mutate(period = stringr::str_extract(table, "(?<=_)([^_]+)")) %>%
                write_parquet(glue("tables/{tabl}.parquet"))
        })
        dbDisconnect(conn)
    })
})
print(glue("all {length(tables)} tables written"))

sigops_data <- lapply(tables, function(tabl) read_parquet(glue("tables/{tabl}.parquet"))) %>%
    bind_rows() %>%
    arrange(Date) %>%
    pivot_wider(names_from = "Date", values_from = "Records", values_fill = 0) %>%
    mutate(across(where(is.double), as.integer))

wk_dates <- names(sigops_data)[3:length(names(sigops_data))]
wk_dates <- wk_dates[lapply(wk_dates, wday) == 3]

sigops_data %>%
    select(table, period, all_of(wk_dates)) %>%
    filter(grepl("_wk", table)) %>%
    write_csv("sigops_data_wk.csv")

sigops_data %>%
    select(table, period, ends_with("-01")) %>%
    filter(grepl("_mo_", table)) %>%
    write_csv("sigops_data_mo.csv")

sigops_data %>%
    filter(!grepl("(_wk_)|(_mo_)", table)) %>%
    write_csv("sigops_data_dy.csv")

for (per in c("wk", "mo", "dy")) {
    aws.s3::put_object(
        glue("sigops_data_{per}.csv"),
        bucket = conf$bucket,
        object = glue("code/sigops_data_{per}.csv")
    )
print(glue("all tables uploaded"))
}
