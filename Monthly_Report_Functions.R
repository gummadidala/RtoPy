
# Monthly_Report_Functions.R

suppressMessages({
    library(DBI)
    library(RAthena)
    library(dbplyr)
    library(readxl)
    library(readr)
    library(dplyr)
    library(tidyr)
    library(stringr)
    library(purrr)
    library(lubridate)
    library(glue)
    library(data.table)
    library(formattable)
    library(forcats)
    library(fst)
    library(parallel)
    library(doParallel)
    library(future)
    library(pool)
    library(httr)
    library(aws.s3)
    library(sf)
    library(yaml)
    library(utils)
    library(readxl)
    library(rjson)
    library(plotly)
    library(crosstalk)
    library(runner)
    library(fitdistrplus)
    library(foreach)
    library(arrow)
    # https://arrow.apache.org/install/
    library(qs)
    library(sp)
    library(future.apply)
    # library(stringdist)
})


#options(dplyr.summarise.inform = FALSE)

select <- dplyr::select
filter <- dplyr::filter

conf <- read_yaml("Monthly_Report.yaml")

aws_conf <- read_yaml("Monthly_Report_AWS.yaml")
Sys.setenv("AWS_ACCESS_KEY_ID" = aws_conf$AWS_ACCESS_KEY_ID,
           "AWS_SECRET_ACCESS_KEY" = aws_conf$AWS_SECRET_ACCESS_KEY,
           "AWS_DEFAULT_REGION" = aws_conf$AWS_DEFAULT_REGION)

conf$athena$uid <- aws_conf$AWS_ACCESS_KEY_ID
conf$athena$pwd <- aws_conf$AWS_SECRET_ACCESS_KEY

if (Sys.info()["sysname"] == "Windows") {
    home_path <- dirname(path.expand("~"))
    python_path <- file.path(home_path, "Anaconda3", "python.exe")

} else if (Sys.info()["sysname"] == "Linux") {
    home_path <- "~"
    python_path <- file.path(home_path, "miniconda3", "bin", "python")

} else {
    stop("Unknown operating system.")
}

#use_python(python_path)

#pqlib <- reticulate::import_from_path("parquet_lib", path = ".")

# Colorbrewer Paired Palette Colors
LIGHT_BLUE = "#A6CEE3";   BLUE = "#1F78B4"
LIGHT_GREEN = "#B2DF8A";  GREEN = "#33A02C"
LIGHT_RED = "#FB9A99";    RED = "#E31A1C"
LIGHT_ORANGE = "#FDBF6F"; ORANGE = "#FF7F00"
LIGHT_PURPLE = "#CAB2D6"; PURPLE = "#6A3D9A"
LIGHT_BROWN = "#FFFF99";  BROWN = "#B15928"

RED2 = "#e41a1c"
GDOT_BLUE = "#256194"

BLACK <- "#000000"
WHITE <- "#FFFFFF"
GRAY <- "#D0D0D0"
DARK_GRAY <- "#7A7A7A"
DARK_DARK_GRAY <- "#494949"


SUN = 1; MON = 2; TUE = 3; WED = 4; THU = 5; FRI = 6; SAT = 7

AM_PEAK_HOURS = conf$AM_PEAK_HOURS
PM_PEAK_HOURS = conf$PM_PEAK_HOURS

if (Sys.info()["nodename"] %in% c("GOTO3213490", "Lenny")) { # The SAM or Lenny
    set_config(
        use_proxy("gdot-enterprise", port = 8080,
                  username = Sys.getenv("GDOT_USERNAME"),
                  password = Sys.getenv("GDOT_PASSWORD")))

} else { # shinyapps.io
    Sys.setenv(TZ="America/New_York")
}


source("Utilities.R")
source("S3ParquetIO.R")
source("Configs.R")
source("Counts.R")
source("Metrics.R")
source("Map.R")
source("Teams.R")
source("Aggregations.R")
source("Database_Functions.R")

usable_cores <- get_usable_cores()
