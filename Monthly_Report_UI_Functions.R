# Monthly_Report_UI_Functions

suppressMessages({
    library(flexdashboard)
    library(shiny)
    library(shinyMCE)
    library(yaml)
    library(dplyr)
    library(dbplyr)
    library(tidyr)
    library(lubridate)
    library(purrr)
    library(stringr)
    library(readr)
    library(glue)
    library(arrow)
    library(fst)
    library(forcats)
    library(plotly)
    library(crosstalk)
    library(future)
    library(promises)
    library(pool)
    library(rsconnect)
    library(formattable)
    library(data.table)
    library(htmltools)
    library(leaflet)
    library(leaflet.extras)
    library(sp)
    library(jsonlite)
    library(shinycssloaders)
    library(DT)
    library(log4r)
})


if (interactive()) {
    plan(multisession)
} else {
    plan(multicore)
}

source("Utilities.R")
source("Classes.R")

usable_cores <- get_usable_cores()
doParallel::registerDoParallel(cores = usable_cores)

logger <- log4r::logger(threshold = "DEBUG")

#options(warn = 2)
options(dplyr.summarise.inform = FALSE)

select <- dplyr::select
filter <- dplyr::filter
layout <- plotly::layout

# Colorbrewer Paired Palette Colors
LIGHT_BLUE = "#A6CEE3";   BLUE = "#1F78B4"
LIGHT_GREEN = "#B2DF8A";  GREEN = "#33A02C"
LIGHT_RED = "#FB9A99";    RED = "#E31A1C"
LIGHT_ORANGE = "#FDBF6F"; ORANGE = "#FF7F00"
LIGHT_PURPLE = "#CAB2D6"; PURPLE = "#6A3D9A"
LIGHT_BROWN = "#FFFF99";  BROWN = "#B15928"

DARK_GRAY = "#636363"
BLACK = "#000000"
DARK_GRAY_BAR = "#252525"
LIGHT_GRAY_BAR = "#bdbdbd"

RED2 = "#e41a1c"
GDOT_BLUE = "#045594"; GDOT_BLUE_RGB = "#2d6797"
GDOT_GREEN = "#13784B"
GDOT_YELLOW = "#EEB211"; GDOT_YELLOW_RGB = "rgba(238, 178, 17, 0.80)"
SIGOPS_BLUE = "#00458F"
SIGOPS_GREEN = "#007338"

colrs <- c("1" = LIGHT_BLUE, "2" = BLUE,
           "3" = LIGHT_GREEN, "4" = GREEN,
           "5" = LIGHT_RED, "6" = RED,
           "7" = LIGHT_ORANGE, "8" = ORANGE,
           "9" = LIGHT_PURPLE, "10" = PURPLE,
           "11" = LIGHT_BROWN, "12" = BROWN,
           "0" = DARK_GRAY,
           "NA" = DARK_GRAY,
           "Mainline" = BLUE,
           "Passage" = GREEN,
           "Demand" = RED,
           "Queue" = ORANGE,
           "Other" = DARK_GRAY)

RTOP1_ZONES <- c("Zone 1", "Zone 2", "Zone 3", "Zone 8")
RTOP2_ZONES <- c("Zone 4", "Zone 5", "Zone 6", "Zone 7m", "Zone 7d")


# Constants for Corridor Summary Table

metric.order <- c("du", "pau", "cctvu", "cu", "tp", "aog", "qs", "sf", "tti", "pti", "tasks")
metric.names <- c(
    "Vehicle Detector Availability",
    "Ped Pushbutton Availability",
    "CCTV Availability",
    "Communications Uptime",
    "Traffic Volume (Throughput)",
    "Arrivals on Green",
    "Queue Spillback Rate",
    "Split Failure",
    "Travel Time Index",
    "Planning Time Index",
    "Outstanding Tasks"
)
metric.goals <- c(rep("> 95%", 4),
                  "< 5% dec. from prev. mo",
                  "> 80%",
                  "< 10%",
                  "< 5%",
                  rep("< 2.0", 2),
                  "Dec. from prev. mo")
metric.goals.numeric <- c(rep(0.95, 4), -.05, .8, .10, .05, 2, 2, 0)
metric.goals.sign <- c(rep(">", 4), ">", ">", rep("<", 4), "<")

yes.color <- "green"
no.color <- "red"


as_int <- function(x) {scales::comma_format()(as.integer(x))}
as_2dec <- function(x) {sprintf(x, fmt = "%.2f")}
as_pct <- function(x) {sprintf(x * 100, fmt = "%.1f%%")}
as_currency <- function(x) {scales::dollar_format(accuracy = 1)(x)}

data_format <- function(data_type) {
    switch(
        data_type,
        "integer" = as_int,
        "decimal" = as_2dec,
        "percent" = as_pct,
        "currency" = as_currency)
}

tick_format <- function(data_type) {
    switch(
        data_type,
        "integer" = ",.0",
        "decimal" = ".2f",
        "percent" = ".0%",
        "currency" = "$,.2f")
}




goal <- list("tp" = NULL,
             "aogd" = 0.80,
             "prd" = 1.20,
             "qsd" = NULL,
             "sfd" = NULL,
             "tti" = 1.20,
             "pti" = 1.30,
             "du" = 0.95,
             "ped" = 0.95,
             "cctv" = 0.95,
             "cu" = 0.95,
             "pau" = 0.95)


conf <- read_yaml("Monthly_Report.yaml")

aws_conf <- read_yaml("Monthly_Report_AWS.yaml")
Sys.setenv("AWS_ACCESS_KEY_ID" = aws_conf$AWS_ACCESS_KEY_ID,
           "AWS_SECRET_ACCESS_KEY" = aws_conf$AWS_SECRET_ACCESS_KEY,
           "AWS_DEFAULT_REGION" = aws_conf$AWS_DEFAULT_REGION)

conf$athena$uid <- aws_conf$AWS_ACCESS_KEY_ID
conf$athena$pwd <- aws_conf$AWS_SECRET_ACCESS_KEY

source("Database_Functions.R")

athena_connection_pool <- get_athena_connection_pool(conf$athena)


last_month <- floor_date(today() - days(6), unit = "months")

endof_last_month <- last_month + months(1) - days(1)
first_month <- last_month - months(12)
report_months <- seq(last_month, first_month, by = "-1 month")
month_options <- report_months %>% format("%B %Y")

zone_group_options <- conf$zone_groups

poll_interval <- 1000*3600 # 3600 seconds = 1 hour



s3checkFunc <- function(bucket, object) {
    function() {
        aws.s3::get_bucket(bucket = bucket, prefix = object)$Contents$LastModified
    }
}



s3valueFunc <- function(bucket, object, aws_conf) {

    read_function <- if (endsWith(object, ".qs")) {
        qs::qread
    } else if (endsWith(object, ".feather")) {
        read_feather
    }

    function() {
        aws.s3::s3read_using(
            read_function,
            object = object,
            bucket = bucket,
            opts = list(key = aws_conf$AWS_ACCESS_KEY_ID,
                        secret = aws_conf$AWS_SECRET_ACCESS_KEY))
    }
}



s3reactivePoll <- function(intervalMillis, bucket, object, aws_s3) {
    reactivePoll(intervalMillis, session = NULL,
                 checkFunc = s3checkFunc(bucket = bucket, object = object),
                 valueFunc = s3valueFunc(bucket = bucket, object = object, aws_s3))
}



corridors_key <- sub("\\..*", ".qs", paste0("all_", conf$corridors_filename_s3))
corridors <- s3reactivePoll(poll_interval, bucket = conf$bucket, object = corridors_key, aws_conf)

sigops_connection_pool <<- get_aurora_connection_pool()
aurora_connection_pool <<- sigops_connection_pool

alerts <- s3reactivePoll(poll_interval, bucket = conf$bucket, object = "mark/watchdog/alerts.qs", aws_conf)



read_zipped_feather <- function(x) {
    read_feather(unzip(x))
}



# Functions to get and filter data for plotting ---------------------------



read_signal_data <- function(conn, signalid, plot_start_date, plot_end_date) {
    # Query for nested parquet data in Athena. The preferred way to do this.

    q <- glue(paste("select signalid, date, dat from signal_details, unnest (data) t(dat)",
                    "where signalid = {signalid}",
                    "and date >= '{plot_start_date}' and date <= '{plot_end_date}'"))
    q <- glue(paste("select signalid, date, dat.hour, dat.detector, dat.callphase,",
                    "dat.vol_rc, dat.vol_ac, dat.bad_day",
                    "from ({q}) order by dat.detector, date, dat.hour"))

    dbGetQuery(conn, sql(q)) %>%
        replace_na(list(callphase = 0)) %>%
        transmute(
            Timeperiod = as_date(date) + hours(hour),
            SignalID = factor(signalid),
            Detector = factor(detector),
            CallPhase = factor(callphase),
            vol_rc,
            vol_ac,
            bad_day = as.logical(as.integer(bad_day)))
}



get_last_modified <- function(zmdf_, zone_ = NULL, month_ = NULL) {
    df <- zmdf_ %>%
        dplyr::group_by(Month, Zone) %>%
        dplyr::filter(LastModified == max(LastModified)) %>%
        ungroup()
    # Filter by zone if provided
    if (!is.null(zone_)) {
        df <- df %>% filter(Zone == zone_)
    }
    # Filter by month if provided
    if (!is.null(month_)) {
        df <- df %>% filter(Month == month_)
    }
    df
}



read_from_db <- function(conn) {
    dbReadTable(conn, "progress_report_content") %>%
        group_by(Month, Zone) %>%
        top_n(10, LastModified) %>%
        ungroup() %>%
        mutate(Month = date(Month),
               LastModified = lubridate::as_datetime(LastModified))
}




# Performance Metrics Plots -----------------------------------------------


get_valuebox <- function(cor_monthly_df, var_, var_fmt, break_ = FALSE,
                         zone, mo, qu = NULL) {

    if (is.null(qu)) { # want monthly, not quarterly data
        vals <- cor_monthly_df %>%
            dplyr::filter(Corridor == zone & Month == mo) %>% as.list()
    } else {
        vals <- cor_monthly_df %>%
            dplyr::filter(Corridor == zone & Quarter == qu) %>% as.list()
    }

    vals$delta <- if_else(is.na(vals$delta), 0, vals$delta)

    val <- var_fmt(vals[[var_]])
    del <- paste0(if_else(vals$delta > 0, " (+", " ( "), as_pct(vals$delta), ")")

    shiny::validate(need(val, message = "NA"))

    if (break_) {
        tags$div(HTML(paste(
            val,  # tags$div(val, style = "font-family: Roboto Slab; font-weight: 700;"),
            tags$p(del, style = "font-size: 50%; line-height: 5px; padding: 0 0 0.2em 0;")
        )))
    } else {
        tags$div(HTML(paste(
            val,
            tags$span(del, style = "font-size: 70%;")
        )))
    }
}



# With Goals and Fill Colors
perf_plot_beta <- function(data_, value_, name_, color_, fill_color_,
                       format_func = function(x) {x},
                       hoverformat_ = ",.0f",
                       goal_ = NULL) {

    ax <- list(title = "", showticklabels = TRUE, showgrid = FALSE)
    ay <- list(title = "", showticklabels = FALSE, showgrid = FALSE, zeroline = FALSE, hoverformat = hoverformat_)

    value_ <- as.name(value_)
    data_ <- dplyr::rename(data_, value = !!value_)

    first <- data_[which.min(data_$Month), ]
    last <- data_[which.max(data_$Month), ]

    p <- plot_ly(type = "scatter", mode = "markers")

    if (!is.null(goal_)) {
        p <- p %>%
            add_trace(data = data_,
                      x = ~Month, y = goal_,
                      name = "Goal",
                      line = list(color = DARK_GRAY), #, dash = "dot"),
                      mode = 'lines') %>%
            add_trace(data = data_,
                      x = ~Month, y = ~value,
                      name = name_,
                      line = list(color = color_),
                      mode = 'lines',
                      fill = 'tonexty',
                      fillcolor = fill_color_)
    } else {
        p <- p %>%
            add_trace(data = data_,
                      x = ~Month, y = ~value,
                      name = name_,
                      line = list(color = color_),
                      mode = 'lines')
    }
    p %>%
        add_annotations(x = 0,
                        y = first$value,
                        text = format_func(first$value),
                        showarrow = FALSE,
                        xanchor = "right",
                        xref = "paper",
                        width = 50,
                        align = "right",
                        borderpad = 5) %>%
        add_annotations(x = 1,
                        y = last$value,
                        text = format_func(last$value),
                        font = list(size = 16),
                        showarrow = FALSE,
                        xanchor = "left",
                        xref = "paper",
                        width = 60,
                        align = "left",
                        borderpad = 5) %>%
        layout(xaxis = ax,
               yaxis = ay,
               showlegend = FALSE,
               margin = list(l = 50, #120
                             r = 60,
                             t = 10,
                             b = 10)) %>%
        plotly::config(displayModeBar = F)
}


# Without Goals and Fill Colors - not used for GDOT
perf_plot <- function(data_, value_, name_, color_,
                      format_func = function(x) {x},
                      hoverformat_ = ",.0f",
                      goal_ = NULL) {

    ax <- list(title = "", showticklabels = TRUE, showgrid = FALSE)
    ay <- list(title = "", showticklabels = FALSE, showgrid = FALSE, zeroline = FALSE, hoverformat = hoverformat_)

    value_ <- as.name(value_)
    data_ <- dplyr::rename(data_, value = !!value_)

    first <- data_[which.min(data_$Month), ]
    last <- data_[which.max(data_$Month), ]

    p <- plot_ly(type = "scatter", mode = "markers")

    if (!is.null(goal_)) {
        p <- p %>%
            add_trace(data = data_,
                      x = ~Month, y = goal_,
                      name = "Goal",
                      line = list(color = LIGHT_RED, dash = "dot"),
                      mode = 'lines')
    }
    p %>%
        add_trace(data = data_,
                  x = ~Month, y = ~value,
                  name = name_,
                  line = list(color = color_),
                  mode = 'lines+markers',
                  marker = list(size = 8,
                                color = color_,
                                line = list(width = 1,
                                            color = 'rgba(255, 255, 255, 255, .8)'))) %>%
        add_annotations(x = first$Month,
                        y = first$value,
                        text = format_func(first$value),
                        showarrow = FALSE,
                        xanchor = "right",
                        xshift = -10) %>%
        add_annotations(x = last$Month,
                        y = last$value,
                        text = format_func(last$value),
                        font = list(size = 16),
                        showarrow = FALSE,
                        xanchor = "left",
                        xshift = 20) %>%
        layout(xaxis = ax,
               yaxis = ay,
               annotations = list(x = -.02,
                                  y = 0.4,
                                  xref = "paper",
                                  yref = "paper",
                                  xanchor = "right",
                                  text = name_,
                                  font = list(size = 12),
                                  showarrow = FALSE),
               showlegend = FALSE,
               margin = list(l = 120,
                             r = 40)) %>%
        plotly::config(displayModeBar = F)
}



no_data_plot <- function(name_) {

    ax <- list(title = "", showticklabels = FALSE, showgrid = FALSE, zeroline = FALSE)
    ay <- list(title = "", showticklabels = FALSE, showgrid = FALSE, zeroline = FALSE)

    plot_ly(type = "scatter", mode = "markers") %>%
        layout(xaxis = ax,
               yaxis = ay,
               annotations = list(list(x = -.02,
                                       y = 0.4,
                                       xref = "paper",
                                       yref = "paper",
                                       xanchor = "right",
                                       text = name_,
                                       font = list(size = 12),
                                       showarrow = FALSE),
                                  list(x = 0.5,
                                       y = 0.5,
                                       xref = "paper",
                                       yref = "paper",
                                       text = "NO DATA",
                                       font = list(size = 16),
                                       showarrow = FALSE)),

               margin = list(l = 180,
                             r = 100)) %>%
        plotly::config(displayModeBar = F)

}


# Empty plot - space filler
x0 <- list(zeroline = FALSE, ticks = "", showticklabels = FALSE, showgrid = FALSE)
p0 <- plot_ly(type = "scatter", mode = "markers") %>% layout(xaxis = x0, yaxis = x0)



get_bar_line_dashboard_plot <- function(cor_weekly,
                                         cor_monthly,
                                         cor_hourly = NULL,
                                         var_,
                                         num_format, # percent, integer, decimal
                                         highlight_color,
                                         month_,
                                         zone_group_,
                                         x_bar_title = "___",
                                         x_line1_title = "___",
                                         x_line2_title = "___",
                                         plot_title = "___ ",
                                         goal = NULL,
                                         accent_average = TRUE) {

    var_ <- as.name(var_)
    if (num_format == "percent") {
        var_fmt <- as_pct
        tickformat_ <- ".0%"
    } else if (num_format == "integer") {
        var_fmt <- as_int
        tickformat_ <- ",.0"
    } else if (num_format == "decimal") {
        var_fmt <- as_2dec
        tickformat_ <- ".2f"
    }

    highlight_color_ <- highlight_color

    mdf <- filter_mr_data(cor_monthly, zone_group_)
    wdf <- filter_mr_data(cor_weekly, zone_group_)

    mdf <- filter(mdf, Month == month_)
    wdf <- filter(wdf, Date < month_ + months(1))


    if (nrow(mdf) > 0 & nrow(wdf) > 0) {
        # Current Month Data
        mdf <- mdf %>%
            arrange(!!var_) %>%
            mutate(var = !!var_,
                   Corridor = factor(Corridor, levels = Corridor))
        if (accent_average) {
            mdf <- mdf %>%
                mutate(col = factor(ifelse(Corridor==zone_group_, DARK_GRAY_BAR, LIGHT_GRAY_BAR)))
        } else {
            mdf <- mdf %>%
                mutate(col = factor(LIGHT_GRAY_BAR, levels = c(DARK_GRAY_BAR, LIGHT_GRAY_BAR)))
        }

        sdm <- SharedData$new(mdf, ~Corridor, group = "grp")

        bar_chart <- plot_ly(sdm,
                             type = "bar",
                             x = ~var,
                             y = ~Corridor,
                             marker = list(color = ~col),
                             text = ~var_fmt(var),
                             textposition = "auto",
                             insidetextfont = list(color = "black"),
                             name = "",
                             customdata = ~glue(paste(
                                 "<b>{Description}</b>",
                                 "<br>{plot_title}: <b>{var_fmt(var)}</b>")),
                             hovertemplate = "%{customdata}",
                             hoverlabel = list(font = list(family = "Source Sans Pro"))
        ) %>%
            layout(
                barmode = "overlay",
                xaxis = list(title = x_bar_title,
                             zeroline = FALSE,
                             tickformat = tickformat_),
                yaxis = list(title = ""),
                showlegend = FALSE,
                font = list(size = 11),
                margin = list(pad = 4,
                              l = 100)
            )
        if (!is.null(goal)) {
            bar_chart <- bar_chart %>%
                add_lines(x = goal,
                          y = ~Corridor,
                          mode = "lines",
                          marker = NULL,
                          line = list(color = LIGHT_RED),
                          name = "Goal (95%)",
                          showlegend = FALSE)
        }

        # Weekly Data - historical trend
        wdf <- wdf %>%
            mutate(var = !!var_)
        if (accent_average) {
            wdf <- wdf %>%
                mutate(col = factor(ifelse(Corridor == zone_group_, 0, 1)))
        } else {
            wdf <- wdf %>%
                mutate(col = factor(1, levels = c(0, 1)))
        }
        wdf <- wdf %>%
            group_by(Corridor)

        sdw <- SharedData$new(wdf, ~Corridor, group = "grp")

        weekly_line_chart <- plot_ly(sdw,
                                     type = "scatter",
                                     mode = "lines",
                                     x = ~Date,
                                     y = ~var,
                                     color = ~col, colors = c(BLACK, LIGHT_GRAY_BAR),
                                     alpha = 0.6,
                                     name = "",
                                     customdata = ~glue(paste(
                                         "<b>{Description}</b>",
                                         "<br>Week of: <b>{format(Date, '%B %e, %Y')}</b>",
                                         "<br>{plot_title}: <b>{var_fmt(var)}</b>")),
                                     hovertemplate = "%{customdata}",
                                     hoverlabel = list(font = list(family = "Source Sans Pro"))
        ) %>%
            layout(xaxis = list(title = x_line1_title),
                   yaxis = list(tickformat = tickformat_,
                                hoverformat = tickformat_),
                   title = "__plot1_title__",
                   showlegend = FALSE,
                   margin = list(t = 50)
            )

        if (!is.null(cor_hourly)) {

            hdf <- filter_mr_data(cor_hourly, zone_group_)

            hdf <- filter(hdf, date(Hour) == month_)

            # Hourly Data - current month
            hdf <- hdf %>%
                mutate(var = !!var_,
                       col = factor(ifelse(Corridor == zone_group_, 0, 1))) %>%
                group_by(Corridor)

            sdh <- SharedData$new(hdf, ~Corridor, group = "grp")

            hourly_line_chart <- plot_ly(sdh) %>%
                add_lines(x = ~Hour,
                          y = ~var,
                          color = ~col, colors = c(BLACK, LIGHT_GRAY_BAR),
                          alpha = 0.6,
                          name = "",
                          customdata = ~glue(paste(
                              "<b>{Description}</b>",
                              "<br>Hour: <b>{format(Hour, '%l:%M %p')}</b>",
                              "<br>{plot_title}: <b>{var_fmt(var)}</b>")),
                          hovertemplate = "%{customdata}",
                          hoverlabel = list(font = list(family = "Source Sans Pro"))
                ) %>%
                layout(xaxis = list(title = x_line2_title),
                       yaxis = list(tickformat = tickformat_),
                       title = "__plot2_title__",
                       showlegend = FALSE) %>%
                highlight(
                    color = highlight_color_,
                    opacityDim = 0.9,
                    defaultValues = c(zone_group_),
                    selected = attrs_selected(
                        insidetextfont = list(color = "white"),
                        textposition = "auto"),
                    on = "plotly_click",
                    off = "plotly_doubleclick")

            ax0 <- list(zeroline = FALSE, ticks = "", showticklabels = FALSE, showgrid = FALSE)
            p0 <- plot_ly(type="scatter", mode="markers") %>% layout(xaxis = ax0, yaxis = ax0)

            s1 <- subplot(weekly_line_chart, p0, hourly_line_chart,
                          titleX = TRUE, heights = c(0.6, 0.1, 0.3), nrows = 3)
        } else {
            s1 <- weekly_line_chart
        }

        subplot(bar_chart, s1, titleX = TRUE, widths = c(0.2, 0.8), margin = 0.03) %>%
            layout(margin = list(l = 100),
                   title = plot_title) %>%
            highlight(
                color = highlight_color_,
                opacityDim = 0.9,
                defaultValues = c(zone_group_),
                selected = attrs_selected(
                    insidetextfont = list(color = "white"),
                    textposition = "auto"),
                on = "plotly_click",
                off = "plotly_doubleclick")

    } else(
        no_data_plot("")
    )
}


get_tt_plot <- function(cor_monthly_tti, cor_monthly_tti_by_hr,
                         cor_monthly_pti, cor_monthly_pti_by_hr,
                         highlight_color = RED2,
                         month_,
                         zone_group_,
                         x_bar_title = "___",
                         x_line1_title = "___",
                         x_line2_title = "___",
                         plot_title = "___ ") {

    var_fmt <- as_2dec
    tickformat <- ".2f"

    mott <- full_join(cor_monthly_tti, cor_monthly_pti,
                      by = c("Corridor", "Zone_Group", "Month"),
                      suffix = c(".tti", ".pti"))  %>%
        filter(!is.na(Corridor)) %>%
        mutate(bti = pti - tti) %>%
        ungroup() %>%
        filter(Month < month_ + months(1)) %>%
        select(Corridor, Zone_Group, Month, tti, pti, bti)

    hrtt <- full_join(cor_monthly_tti_by_hr, cor_monthly_pti_by_hr,
                      by = c("Corridor", "Zone_Group", "Hour"),
                      suffix = c(".tti", ".pti"))  %>%
        filter(!is.na(Corridor)) %>%
        mutate(bti = pti - tti) %>%
        ungroup() %>%
        select(Corridor, Zone_Group, Hour, tti, pti, bti)

    mott <- filter_mr_data(mott, zone_group_)
    hrtt <- filter_mr_data(hrtt, zone_group_)

    mo_max <- round(max(mott$pti), 1) + .1
    hr_max <- round(max(hrtt$pti), 1) + .1

    if (nrow(mott) > 0 & nrow(hrtt) > 0) {
        sdb <- SharedData$new(dplyr::filter(mott, Month == month_),
                              ~Corridor, group = "grp")
        sdm <- SharedData$new(mott,
                              ~Corridor, group = "grp")
        sdh <- SharedData$new(dplyr::filter(hrtt, date(Hour) == month_),
                              ~Corridor, group = "grp")

        highlight_color_ <- RED2 # Colorbrewer red

        base_b <- plot_ly(sdb, color = I("gray")) %>%
            group_by(Corridor)
        base_m <- plot_ly(sdm, color = I("gray")) %>%
            group_by(Corridor)
        base_h <- plot_ly(sdh, color = I("gray")) %>%
            group_by(Corridor)

        pbar <- base_b %>%

            arrange(tti) %>%
            add_bars(x = ~tti,
                     y = ~factor(Corridor, levels = Corridor),
                     text = ~as_2dec(tti),
                     color = I("gray"),
                     textposition = "auto",
                     insidetextfont = list(color = "black"),
                     name = "",
                     customdata = ~glue(paste(
                         "<b>{Corridor}</b>",
                         "<br>Travel Time Index: <b>{var_fmt(tti)}</b>",
                         "<br>Planning Time Index: <b>{var_fmt(pti)}</b>")),
                     hovertemplate = "%{customdata}",
                     hoverlabel = list(font = list(family = "Source Sans Pro"))
            ) %>%
            add_bars(x = ~bti,
                     y = ~factor(Corridor, levels = Corridor),
                     text = ~as_2dec(pti),
                     color = I(LIGHT_BLUE),
                     textposition = "auto",
                     insidetextfont = list(color = "black"),
                     hoverinfo = "none") %>%
            layout(
                barmode = "stack",
                xaxis = list(title = x_bar_title,
                             zeroline = FALSE,
                             tickformat = tickformat,
                             range = c(0, 2)),
                yaxis = list(title = ""),
                showlegend = FALSE,
                font = list(size = 11),
                margin = list(pad = 4)
            )
        pttimo <- base_m %>%
            add_lines(x = ~Month,
                      y = ~tti,
                      alpha = 0.6,
                      name = "",
                      customdata = ~glue(paste(
                          "<b>{Corridor}</b>",
                          "<br><b>{format(Month, '%B %Y')}</b>",
                          "<br>Travel Time Index: <b>{var_fmt(tti)}</b>")),
                      hovertemplate = "%{customdata}",
                      hoverlabel = list(font = list(family = "Source Sans Pro"))
            ) %>%
            layout(xaxis = list(title = "Travel Time Index (TTI"),
                   yaxis = list(range = c(1, mo_max),
                                tickformat = tickformat),
                   showlegend = FALSE)

        pttihr <- base_h %>%
            add_lines(x = ~Hour,
                      y = ~tti,
                      alpha = 0.6,
                      name = "",
                      customdata = ~glue(paste(
                          "<b>{Corridor}</b>",
                          "<br>Hour: <b>{format(Hour, '%l:%M %p')}</b>",
                          "<br>Travel Time Index: <b>{var_fmt(tti)}</b>")),
                      hovertemplate = "%{customdata}",
                      hoverlabel = list(font = list(family = "Source Sans Pro"))
            ) %>%
            layout(xaxis = list(title = x_line1_title),
                   yaxis = list(range = c(1, hr_max),
                                tickformat = tickformat),
                   showlegend = FALSE)

        pptimo <- base_m %>%
            add_lines(x = ~Month,
                      y = ~pti,
                      alpha = 0.6,
                      name = "",
                      customdata = ~glue(paste(
                          "<b>{Corridor}</b>",
                          "<br><b>{format(Month, '%B %Y')}</b>",
                          "<br>Planning Time Index: <b>{var_fmt(pti)}</b>")),
                      hovertemplate = "%{customdata}",
                      hoverlabel = list(font = list(family = "Source Sans Pro"))) %>%
            layout(xaxis = list(title = "Planning Time Index (PTI)"),
                   yaxis = list(range = c(1, mo_max),
                                tickformat = tickformat),
                   showlegend = FALSE)

        pptihr <- base_h %>%
            add_lines(x = ~Hour,
                      y = ~pti,
                      alpha = 0.6,
                      name = "",
                      customdata = ~glue(paste(
                          "<b>{Corridor}</b>",
                          "<br>Hour: <b>{format(Hour, '%l:%M %p')}</b>",
                          "<br>Planning Time Index: <b>{var_fmt(pti)}</b>")),
                      hovertemplate = "%{customdata}",
                      hoverlabel = list(font = list(family = "Source Sans Pro"))
            ) %>%
            layout(xaxis = list(title = x_line2_title),
                   yaxis = list(range = c(1, hr_max),
                                tickformat = tickformat),
                   showlegend = FALSE)

        x0 <- list(zeroline = FALSE, ticks = "", showticklabels = FALSE, showgrid = FALSE)
        p0 <- plot_ly(type = "scatter", mode = "markers") %>%
            layout(xaxis = x0,
                   yaxis = x0)

        stti <- subplot(pttimo, p0, pttihr,
                        titleX = TRUE, heights = c(0.6, 0.1, 0.3), nrows = 3)

        sbti <- subplot(pptimo, p0, pptihr,
                        titleX = TRUE, heights = c(0.6, 0.1, 0.3), nrows = 3)

        subplot(pbar, stti, sbti, titleX = TRUE, widths = c(0.2, 0.4, 0.4), margin = 0.03) %>%
            layout(margin = list(l = 120, r = 80),
                   title = "Travel Time and Planning Time Index") %>%
            highlight(color = highlight_color_,
                      opacityDim = 0.9,
                      defaultValues = c(zone_group_),
                      selected = attrs_selected(
                          insidetextfont = list(color = "white"),
                          textposition = "auto", base = 0),
                      on = "plotly_click",
                      off = "plotly_doubleclick")

    } else {
        no_data_plot("")
    }
}




get_cor_det_uptime_plot <- function(avg_daily_uptime,
                                     avg_monthly_uptime,
                                     month_,
                                     zone_group_,
                                     month_name) {

    # Plot Detector Uptime for a Corridor. The basis of subplot.
    plot_detector_uptime <- function(df, corr, showlegend_) {
        plot_ly(data = df) %>%
            add_lines(x = ~Date,
                      y = ~uptime.pr,
                      color = I(LIGHT_BLUE),
                      name = "Presence",
                      legendgroup = "Presence",
                      showlegend = showlegend_) %>%
            add_lines(x = ~Date,
                      y = ~uptime.sb,
                      color = I(BLUE),
                      name = "Setback",
                      legendgroup = "Setback",
                      showlegend = showlegend_) %>%
            add_lines(x = ~Date,
                      y = 0.95,
                      color = I(LIGHT_RED),
                      name = "Goal (95%)",
                      legendgroup = "Goal",
                      showlegend = showlegend_) %>%
            layout(yaxis = list(title = "",
                                range = c(0, 1.1),
                                tickformat = "%"),
                   xaxis = list(title = ""),
                   annotations = list(text = corr,
                                      xref = "paper",
                                      yref = "paper",
                                      yanchor = "bottom",
                                      xanchor = "left",
                                      align = "center",
                                      x = 0.1,
                                      y = 0.95,
                                      showarrow = FALSE))
    }
    plot_detector_uptime_bar <- function(df) {

        df <- df %>%
            rename(uptime = uptime) %>%
            arrange(uptime) %>% ungroup() %>%
            mutate(col = factor(ifelse(Corridor==zone_group_, DARK_GRAY_BAR, LIGHT_GRAY_BAR)),
                   Corridor = factor(Corridor, levels = Corridor))

        plot_ly(data = arrange(df, uptime),
                marker = list(color = ~col)) %>%
            add_bars(
                x = ~uptime,
                y = ~Corridor,
                text = ~as_pct(uptime),
                textposition = "auto",
                insidetextfont = list(color = "black"),
                showlegend = FALSE,
                name = "",
                customdata = ~glue(paste(
                 "<b>{Description}</b>",
                 "<br>Uptime: <b>{as_pct(uptime)}</b>")),
                hovertemplate = "%{customdata}",
                hoverlabel = list(font = list(family = "Source Sans Pro"))) %>%
            add_lines(x = ~0.95,
                      y = ~Corridor,
                      mode = "lines",
                      marker = NULL,
                      color = I(LIGHT_RED),
                      name = "Goal (95%)",
                      legendgroup = "Goal",
                      showlegend = FALSE) %>%

            layout(
                barmode = "overlay",
                xaxis = list(title = paste(month_name, "Detector Uptime (%)"),
                             zeroline = FALSE,
                             tickformat = "%"),
                yaxis = list(title = ""),
                showlegend = FALSE,
                font = list(size = 11),
                margin = list(pad = 4,
                              l = 100)
            )
    }

    avg_daily_uptime <- filter_mr_data(avg_daily_uptime, zone_group_) %>%
        filter(Date <= month_ + months(1))

    avg_monthly_uptime <- filter_mr_data(avg_monthly_uptime, zone_group_) %>%
        filter(Month == month_)

    if (nrow(avg_daily_uptime) > 0) {
        # Create Uptime by Detector Type (Setback, Presence) by Corridor Subplots.
        cdfs <- split(avg_daily_uptime, avg_daily_uptime$Corridor)
        cdfs <- cdfs[lapply(cdfs, nrow)>0]

        p1 <- plot_detector_uptime_bar(avg_monthly_uptime)

        plts <- lapply(seq_along(cdfs), function(i) {
            plot_detector_uptime(cdfs[[i]], names(cdfs)[i], ifelse(i==1, TRUE, FALSE))
        })
        s2 <- subplot(plts, nrows = min(length(plts), 4), shareX = TRUE, shareY = TRUE, which_layout = 1)
        subplot(p1, s2, titleX = TRUE, widths = c(0.2, 0.8), margin = 0.03) %>%
            layout(margin = list(l = 100))
    } else {
        no_data_plot("")
    }
}



get_cor_comm_uptime_plot <- function(avg_daily_uptime,
                                      avg_monthly_uptime,
                                      month_,
                                      zone_group_,
                                      month_name) {

    # Plot Detector Uptime for a Corridor. The basis of subplot.
    plot_detector_uptime <- function(df, corr, showlegend_) {
        plot_ly(data = df) %>%
            add_lines(x = ~Date,
                      y = ~uptime, #
                      color = I(BLUE),
                      name = "Uptime", #
                      legendgroup = "Uptime", #
                      showlegend = showlegend_) %>%
            add_lines(x = ~Date,
                      y = 0.95,
                      color = I(LIGHT_RED),
                      name = "Goal (95%)",
                      legendgroup = "Goal",
                      showlegend = showlegend_) %>%
            layout(yaxis = list(title = "",
                                range = c(0, 1.1),
                                tickformat = "%"),
                   xaxis = list(title = ""),
                   annotations = list(text = corr,
                                      xref = "paper",
                                      yref = "paper",
                                      yanchor = "bottom",
                                      xanchor = "left",
                                      align = "center",
                                      x = 0.1,
                                      y = 0.95,
                                      showarrow = FALSE))
    }
    plot_detector_uptime_bar <- function(df) {

        df <- df %>%
            arrange(uptime) %>% ungroup() %>%
            mutate(col = factor(ifelse(Corridor==zone_group_, DARK_GRAY_BAR, LIGHT_GRAY_BAR)),
                   Corridor = factor(Corridor, levels = Corridor))

        plot_ly(data = arrange(df, uptime),
                marker = list(color = ~col)) %>%
            add_bars(
                x = ~uptime,
                y = ~Corridor,
                text = ~as_pct(uptime),
                textposition = "auto",
                insidetextfont = list(color = "black"),
                showlegend = FALSE,
                name = "",
                customdata = ~glue(paste(
                    "<b>{Description}</b>",
                    "<br>Uptime: <b>{as_pct(uptime)}</b>")),
                hovertemplate = "%{customdata}",
                hoverlabel = list(font = list(family = "Source Sans Pro"))) %>%
            add_lines(x = ~0.95,
                      y = ~Corridor,
                      mode = "lines",
                      marker = NULL,
                      color = I(LIGHT_RED),
                      name = "Goal (95%)",
                      legendgroup = "Goal",
                      showlegend = FALSE) %>%

            layout(
                barmode = "overlay",
                xaxis = list(title = paste(month_name, "Comms Uptime (%)"),
                             zeroline = FALSE,
                             tickformat = "%"),
                yaxis = list(title = ""),
                showlegend = FALSE,
                font = list(size = 11),
                margin = list(pad = 4,
                              l = 100)
            )
    }

    avg_daily_uptime <- filter_mr_data(avg_daily_uptime, zone_group_)
    avg_monthly_uptime <- filter_mr_data(avg_monthly_uptime, zone_group_)

    avg_daily_uptime <- filter(avg_daily_uptime, Date <= month_ + months(1))
    avg_monthly_uptime <- filter(avg_monthly_uptime, Month == month_)


    if (nrow(avg_daily_uptime) > 0) {
        # Create Uptime by Detector Type (Setback, Presence) by Corridor Subplots.
        cdfs <- split(avg_daily_uptime, avg_daily_uptime$Corridor)
        cdfs <- cdfs[lapply(cdfs, nrow)>0]

        p1 <- plot_detector_uptime_bar(avg_monthly_uptime)

        plts <- lapply(seq_along(cdfs), function(i) {
            plot_detector_uptime(cdfs[[i]], names(cdfs)[i], ifelse(i==1, TRUE, FALSE))
        })
        s2 <- subplot(plts, nrows = min(length(plts), 4), shareX = TRUE, shareY = TRUE, which_layout = 1)
        subplot(p1, s2, titleX = TRUE, widths = c(0.2, 0.8), margin = 0.03) %>%
            layout(margin = list(l = 100))
    } else {
        no_data_plot("")
    }
}



uptime_heatmap <- function(df_,
                           var_,
                           month_ = current_month(),
                           zone_group_ = zone_group()) {

    start_date <- floor_date(min(df_$Date), "month")
    end_date <- ceiling_date(max(df_$Date), "month") - days(1)

    fill_func <- function(names_) {
        setNames(as.list(rep(0, length(names_))), names_)
    }

    var <- as.name(var_)
    spr <- df_ %>%
        filter(
            Date < month_ + months(1),
            Zone_Group == zone_group_) %>%
        select(SignalID = Corridor, Description, Date, !!var) %>%
        distinct() %>%
        complete(nesting(SignalID, Description),
                 Date = seq(start_date, end_date, by = "days"),
                 fill = fill_func(names(df_))[as.character(var)]) %>%

        group_by(SignalID, Description, Date) %>%
        summarize(!!var := sum(!!var)) %>%
        ungroup() %>%
        spread(Date, !!var, fill = 0) %>%
        arrange(desc(SignalID))

    m <- as.matrix(spr %>% select(-SignalID, -Description))

    row_names <- spr$SignalID
    col_names <- colnames(m)
    colnames(m) <- NULL

    bind_description <- function(m) {
        glue("<b>{as.character(spr$Description)}</b><br>Uptime: <b>{as_pct(m)}</b>")
    }

    plot_ly(x = col_names,
            y = row_names,
            z = m,
            colors = colorRamp(c("white", BLUE)),
            type = "heatmap",
            ygap = 1,
            showscale = FALSE,
            customdata = apply(apply(m, 2, bind_description), 1, as.list),
            hovertemplate="<br>%{customdata}<br>%{x}<extra></extra>",
            hoverlabel = list(font = list(family = "Source Sans Pro"))) %>%

        layout(yaxis = list(type = "category",
                            title = ""),
               showlegend = FALSE,
               margin = list(l = 150))
}


get_uptime_plot <- function(daily_df,
                            monthly_df,
                            var_,
                            num_format, # percent, integer, decimal
                            month_,
                            zone_group_,
                            x_bar_title = "___",
                            x_line1_title = "___",
                            x_line2_title = "___",
                            plot_title = "___ ",
                            goal = NULL) {

    var <- as.name(var_)
    if (num_format == "percent") {
        var_fmt <- as_pct
        tickformat_ <- ".0%"
    } else if (num_format == "integer") {
        var_fmt <- as_int
        tickformat_ <- ",.0"
    } else if (num_format == "decimal") {
        var_fmt <- as_2dec
        tickformat_ <- ".2f"
    }

    if (nrow(daily_df) > 0 & nrow(monthly_df) > 0) {

        monthly_df_ <- monthly_df %>%
            filter(Month == month_,
                   Zone_Group == zone_group_) %>%
            arrange(desc(Corridor)) %>%
            mutate(var = !!var,
                   col = factor(ifelse(Corridor==zone_group_, DARK_GRAY_BAR, LIGHT_GRAY_BAR)),
                   Corridor = factor(Corridor, levels = Corridor))

        if (nrow(monthly_df_) > 0) {
            # Current Month Data
            bar_chart <- plot_ly(monthly_df_,
                                 type = "bar",
                                 x = ~var,
                                 y = ~Corridor,
                                 marker = list(color = ~col),
                                 text = ~var_fmt(var),
                                 textposition = "auto",
                                 insidetextfont = list(color = "black"),
                                 name = "",
                                 customdata = ~glue(paste(
                                     "<b>{Description}</b>",
                                     "<br>Uptime: <b>{var_fmt(var)}</b>")),
                                 hovertemplate = "%{customdata}",
                                 hoverlabel = list(font = list(family = "Source Sans Pro"))) %>%
                layout(
                    barmode = "overlay",
                    xaxis = list(title = x_bar_title,
                                 zeroline = FALSE,
                                 tickformat = tickformat_),
                    yaxis = list(title = ""),
                    showlegend = FALSE,
                    font = list(size = 11),
                    margin = list(pad = 4,
                                  l = 100,
                                  r = 50)
                    )
            if (!is.null(goal)) {
                bar_chart <- bar_chart %>%
                    add_lines(x = goal,
                              y = ~Corridor,
                              mode = "lines",
                              marker = NULL,
                              line = list(color = LIGHT_RED),
                              name = "Goal (95%)",
                              showlegend = FALSE)
                }

            # Daily Heatmap
            daily_heatmap <- uptime_heatmap(daily_df,
                                            var,
                                            month_,
                                            zone_group_)



            subplot(bar_chart, daily_heatmap, titleX = TRUE, widths = c(0.2, 0.8), margin = 0.03) %>%
                layout(margin = list(l = 100),
                       title = plot_title)
        } else {
            no_data_plot("")
        }
    } else {
        no_data_plot("")
    }
}




volplot_plotly <- function(
    dbpool_,
    signalid, plot_start_date, plot_end_date,
    title = "title", ymax = 1000) {

    if (is.null(ymax)) {
        yax <- list(rangemode = "tozero", tickformat = ",.0")
    } else {
        yax <- list(range = c(0, ymax), tickformat = ",.0")
    }

    # Works. fill_colr is still an enigma, but it works as is.
    pl <- function(dfi, i) {

        dfi <- dfi %>%
            mutate(
                #CallPhase = last(dfi$CallPhase), # Plotly gets confused with multiple colors
                maxy = if_else(bad_day==1, as.integer(max(1, vol_rc, na.rm = TRUE)), as.integer(0)),
                colr = colrs[as.character(CallPhase)],
                fill_colr = "")

        dfis <- split(dfi, dfi$CallPhase)
        dfis <- dfis[unlist(purrr::map(dfis, function(x) nrow(x)>0))]

        p <- plot_ly()
        for (df in dfis) {
            p <- p %>% add_trace(
                data = df,
                x = ~Timeperiod,
                y = ~vol_rc,
                type = "scatter",
                mode = "lines",
                fill = "tozeroy",
                line = list(color = ~colr),
                fillcolor = ~fill_colr,
                name = paste('Phase', dfi$CallPhase[1]),
                customdata = ~glue(paste(
                    "<b>Detector: {Detector}</b>",
                    "<br>{format(ymd_hms(Timeperiod), '%a %d %B %I:%M %p')}",
                    "<br>Volume: <b>{as_int(vol_rc)}</b>")),
                hovertemplate = "%{customdata}",
                hoverlabel = list(font = list(family = "Source Sans Pro")),
                showlegend = FALSE)
        }

        p %>% add_trace(
            data = dfi,
            x = ~Timeperiod,
            y = ~vol_ac,
            type = "scatter",
            mode = "lines",
            line = list(color = DARK_GRAY),
            name = "Adjusted Count",
            customdata = ~glue(paste(
                "<b>Detector: {Detector}</b>",
                "<br>{format(ymd_hms(Timeperiod), '%a %d %B %I:%M %p')}",
                "<br>Volume: <b>{as_int(vol_ac)}</b>")),
            hovertemplate = "%{customdata}",
            hoverlabel = list(font = list(family = "Source Sans Pro")),
            showlegend = (i==1)) %>%
            add_trace(
                data = dfi,
                x = ~Timeperiod,
                y = ~maxy,
                type = "scatter",
                mode = "lines",
                fill = "tozeroy",
                line = list(
                    color = "rgba(0,0,0,0.1)",
                    shape = "vh"),
                fillcolor = "rgba(0,0,0,0.2)",
                name = "Bad Days",

                customdata = ~glue(paste(
                    "<b>Detector: {Detector}</b>",
                    "<br>{format(date(Timeperiod), '%a %d %B %Y')}",
                    "<br><b>{if_else(bad_day==1, 'Bad Day', 'Good Day')}</b>")),
                hovertemplate = "%{customdata}",
                hoverlabel = list(font = list(family = "Source Sans Pro")),
                showlegend = (i==1)) %>%
            layout(yaxis = yax,
                   annotations = list(x = -.03,
                                      y = 0.5,
                                      xref = "paper",
                                      yref = "paper",
                                      xanchor = "right",
                                      text = paste0("Det #", dfi$Detector[1]),
                                      font = list(size = 12),
                                      showarrow = FALSE))
    }

    withProgress(message = "Loading chart", value = 0, {
        incProgress(amount = 0.1)

        conn <- poolCheckout(dbpool_)
        df <- read_signal_data(conn, signalid, plot_start_date, plot_end_date)
        poolReturn(conn)

        incProgress(amount = 0.5)

        if (nrow(df)) {
            dfs <- split(df, df$Detector)
            dfs <- dfs[lapply(dfs, nrow)>0]
            names(dfs) <- NULL

            plts <- purrr::imap(dfs, ~pl(.x, .y))
            incProgress(amount = 0.3)

            subplot(plts, nrows = length(plts), shareX = TRUE) %>%
                layout(annotations = list(text = title,
                                          xref = "paper",
                                          yref = "paper",
                                          yanchor = "bottom",
                                          xanchor = "center",
                                          align = "center",
                                          x = 0.5,
                                          y = 1,
                                          showarrow = FALSE),
                       showlegend = TRUE,
                       margin = list(l = 120),
                       xaxis = list(
                           type = 'date'))
        } else {
            no_data_plot("")
        }
    })

}




# TEAMS Plots -------------------------------------------------------------


gather_outstanding_events <- function(cor_monthly_events) {

    cor_monthly_events %>%
        ungroup() %>%
        select(Zone_Group, Corridor, Month, Reported, Resolved, Outstanding) %>%
        gather(Events, Status, -c(Month, Corridor, Zone_Group))
}



plot_teams_tasks <- function(tab, var_,
                              title_ = "", textpos = "auto", height_ = 300) { #

    var_ <- as.name(var_)

    tab <- tab %>%
        mutate(var = fct_reorder(!!var_, Reported))

    p1 <- plot_ly(data = tab, height = height_) %>%
        add_bars(x = ~Reported,  # ~Rep,
                 y = ~var,
                 color = I(LIGHT_BLUE),
                 name = "Reported",
                 text = ~Reported, #  ~Rep,
                 textposition = textpos,
                 insidetextfont = list(size = 11, color = "black"),
                 outsidetextfont = list(size = 11)) %>%
        layout(margin = list(l = 180),
               showlegend = FALSE,
               yaxis = list(title = ""),
               xaxis = list(title = "Reported",
                            zeroline = FALSE),
               margin = list(pad = 4))
    p2 <- plot_ly(data = tab) %>%
        add_bars(x = ~Resolved,  # ~Res,
                 y = ~var,
                 color = I(BLUE),
                 name = "Resolved",
                 text = ~Resolved,  # ~Res,
                 textposition = textpos,
                 insidetextfont = list(size = 11, color = "white"),
                 outsidetextfont = list(size = 11)) %>%
        layout(margin = list(l = 180),
               showlegend = FALSE,
               yaxis = list(title = ""),
               xaxis = list(title = "Resolved",
                            zeroline = FALSE),
               margin = list(pad = 4))
    p3 <- plot_ly(data = tab) %>%
        add_bars(x = ~Outstanding,  # ~outstanding,
                 y = ~var,
                 color = I(SIGOPS_GREEN),
                 name = "Outstanding",
                 text = ~Outstanding,
                 textposition = textpos,
                 insidetextfont = list(size = 11, color = "white"),
                 outsidetextfont = list(size = 11)) %>%
        layout(margin = list(l = 180),
               showlegend = FALSE,
               yaxis = list(title = ""),
               xaxis = list(title = "Cum. Outstanding",
                            zeroline = FALSE),
               margin = list(pad = 4))
    subplot(p1, p2, p3, shareY = TRUE, shareX = TRUE) %>%
        layout(title = list(text = title_, font = list(size = 12)))
}



# Number reported and resolved in each month. Side-by-side.
cum_events_plot <- function(df) {
    plot_ly(height = 300) %>%
        add_bars(data = filter(df, Events == "Reported"),
                 x = ~Month,
                 y = ~Status,
                 name = "Reported",
                 marker = list(color = LIGHT_BLUE)) %>%
        add_bars(data = filter(df, Events == "Resolved"),
                 x = ~Month,
                 y = ~Status,
                 name = "Resolved",
                 marker = list(color = BLUE)) %>%
        add_trace(data = filter(df, Events == "Outstanding"),
                  x = ~Month,
                  y = ~Status,
                  type = 'scatter', mode = 'lines', name = 'Outstanding',
                  line = list(color = SIGOPS_GREEN)) %>%
        add_trace(data = filter(df, Events == "Outstanding"),
                  x = ~Month,
                  y = ~Status,
                  type = 'scatter',
                  mode = 'markers',
                  name = 'Outstanding',
                  marker = list(color = SIGOPS_GREEN),
                  showlegend = FALSE) %>%
        layout(barmode = "group",
               yaxis = list(title = "Events"),
               xaxis = list(title = ""),
               legend = list(x = 0.5, y = 0.9, orientation = "h"))
}



plot_individual_cctvs <- function(daily_cctv_df,
                                   month_ = current_month(),
                                   zone_group_ = zone_group()) {

    spr <- daily_cctv_df %>%
        filter(Date < month_ + months(1),
               Zone_Group == zone_group_,
               as.character(Corridor) != as.character(Zone_Group)) %>%
        rename(CameraID = Corridor,
               Corridor = Zone_Group) %>%
        dplyr::select(CameraID, Description, Date, up) %>%
        distinct() %>%
        spread(Date, up, fill = 0) %>%
        arrange(desc(CameraID))

    m <- as.matrix(spr %>% dplyr::select(-CameraID, -Description))
    m <- round(m,0)

    row_names <- spr$CameraID
    col_names <- colnames(m)
    colnames(m) <- NULL

    status <- function(x) {
        options <- c("Camera Down", "Working at encoder, but not 511", "Working on 511")
        options[x+1]
    }
    bind_description <- function(camera_status) {
        glue("<b>{as.character(spr$Description)}</b><br>{camera_status}")
    }

    plot_ly(x = col_names,
            y = row_names,
            z = m,
            colors = c(LIGHT_GRAY_BAR, "#e48c5b", BROWN),
            type = "heatmap",
            ygap = 1,
            showscale = FALSE,
            customdata = apply(apply(apply(m, 2, status), 2, bind_description), 1, as.list),
            hovertemplate="<br>%{customdata}<br>%{x}<extra></extra>",
            hoverlabel = list(font = list(family = "Source Sans Pro"))) %>%
        layout(yaxis = list(type = "category",
                            title = ""),
               margin = list(l = 150))
}



udcplot_plotly <- function(hourly_udc) {

    corridor_udc_plot <- function(hourly_udc, i) {

        current_month <- date(max(hourly_udc$Month))  # hourly_udc$month_hour))  # year_month
        last_month <- current_month - months(1)
        last_year <- current_month - years(1)

        current_month_str <- format(current_month, "%B %Y")
        last_month_str <- format(last_month, "%B %Y")
        last_year_str <- format(last_year, "%B %Y")

        this_month_hrly <- hourly_udc %>%
            filter(
                Month == current_month)
        last_month_hrly <- hourly_udc %>%
            filter(
                Month == last_month) %>%
            mutate(month_hour = month_hour + months(1))
        last_year_hrly <- hourly_udc %>%
            filter(
                Month == last_year) %>%
            mutate(month_hour = month_hour + years(1))

        DARK_BLUE <- "#0068B2"
        LIGHT_LIGHT_BLUE <- "#BED6E2"
        DARK_GRAY = "#636363"
        DARK_GRAY_BAR = "#252525"

        title_ <- hourly_udc$Corridor[1]
        ymax_ <- hourly_udc$max_delay_cost[1]

        plot_ly() %>%
            add_lines(
                data = last_year_hrly, # same month, a year ago
                x = ~month_hour,
                y = ~delay_cost,
                name = last_year_str,   # "Last Year",  #
                line = list(color = LIGHT_BLUE),
                fill = "tozeroy",
                fillcolor = LIGHT_LIGHT_BLUE,
                customdata = ~glue(paste(
                    "<b>{Corridor}</b>",
                    "<br><b>{format(month_hour, '%I:%M %p')}</b>",
                    "<br>User Delay Cost: <b>{as_currency(delay_cost)}</b>")),
                hovertemplate = "%{customdata}",
                hoverlabel = list(font = list(family = "Source Sans Pro")),
                showlegend = (i==1)) %>%
            add_lines(
                data = last_month_hrly, # last month, this year
                x = ~month_hour,
                y = ~delay_cost,
                name = last_month_str,
                line = list(color = BLUE),
                customdata = ~glue(paste(
                    "<b>{Corridor}</b>",
                    "<br><b>{format(month_hour, '%I:%M %p')}</b>",
                    "<br>User Delay Cost: <b>{as_currency(delay_cost)}</b>")),
                hovertemplate = "%{customdata}",
                hoverlabel = list(font = list(family = "Source Sans Pro")),
                showlegend = (i==1)) %>%
            add_lines(
                data = this_month_hrly, # this month, this year
                x = ~month_hour,
                y = ~delay_cost,
                name = current_month_str,
                line = list(color = SIGOPS_GREEN),
                customdata = ~glue(paste(
                    "<b>{Corridor}</b>",
                    "<br><b>{format(month_hour, '%I:%M %p')}</b>",
                    "<br>User Delay Cost: <b>{as_currency(delay_cost)}</b>")),
                hovertemplate = "%{customdata}",
                hoverlabel = list(font = list(family = "Source Sans Pro")),
                showlegend = (i==1)) %>%
            layout(xaxis = list(title = "",
                                tickformat = "%I:%M %p"),
                   yaxis = list(title = "",
                                tickformat = "$,.2",
                                range = c(0, ymax_)),
                   annotations = list(text = glue("<b>{title_}</b>"),
                                      font = list(size = 14),
                                      xref = "paper",
                                      yref = "paper",
                                      yanchor = "bottom",
                                      xanchor = "left",
                                      align = "center",
                                      x = 0.2,
                                      y = 0.5,
                                      showarrow = FALSE))
    }

    df <- hourly_udc %>% mutate(max_delay_cost = round(max(delay_cost), -3) + 1000)
    dfs <- split(df, df$Corridor)
    dfs <- dfs[lapply(dfs, nrow)>0]
    names(dfs) <- NULL

    plts <- purrr::imap(dfs, ~corridor_udc_plot(.x, .y))


    subplot(plts,
            margin = 0.03, nrows = ceiling(length(plts)/2), shareX = FALSE, shareY = FALSE) %>%
        layout(title = "User Delay Costs ($)",
               margin = list(t = 60))
}



# Watchdog Alerts Plots ---------------------------------------------------


filter_alerts_by_date <- function(alerts, dr) {

    start_date <- dr[1]
    end_date <- dr[2]

    alerts %>%
        filter(Date >= start_date & Date <= end_date)
}



filter_alerts <- function(alerts_by_date, alert_type_, zone_group_, corridor_, phase_, id_filter_, active_streak)  {

    most_recent_date <- alerts_by_date %>%
        group_by(Alert) %>%
        summarize(Date = max(Date)) %>%
        spread(Alert, Date) %>% as.list()
    df <- filter(alerts_by_date, Alert == alert_type_)

    if (nrow(df)) {

        # Filter by Zone Group if "All Corridors" selected
        if (corridor_ == "All Corridors") {
            if (zone_group_ == "All RTOP") {
                df <- filter(df, Zone_Group %in% c("RTOP1", "RTOP2"))

            } else if (zone_group_ == "Zone 7") {
                df <- filter(df, Zone %in% c("Zone 7m", "Zone 7d"))

            } else if (grepl("^Zone", zone_group_)) {
                df <- filter(df, Zone == zone_group_)

            } else {
                df <- filter(df, Zone_Group == zone_group_)
            }
            # Otherwise filter by Corridor
        } else {
            df <- filter(df, Corridor == corridor_)
        }

        # Filter filter bar text (regular expression) within Name, Corridor, SignalID
        df <- filter(df, grepl(pattern = id_filter_, x = Name, ignore.case = TRUE, perl = TRUE) |
                         grepl(pattern = id_filter_, x = Corridor, ignore.case = TRUE, perl = TRUE) |
                         grepl(pattern = id_filter_, x = SignalID, ignore.case = TRUE, perl = TRUE))
    }

    # Filter by phase, but not for Missing Records, which doesn't have a phase
    if (nrow(df)) {
        if (alert_type_ != "Missing Records" & phase_ != "All") {
            df <- filter(df, CallPhase == as.numeric(phase_)) # filter
        }

    }

    # If there is anything left in the alerts table, create the table and plots data frames
    if (nrow(df)) {

        table_df <- df %>%
            # Streak is 0 unless it's the most recent date in the alerts_by_date data set
            # So, if the streak is not current as of the last date, it's 0. Streak is "Current Streak"
            mutate(Streak = if_else(Date == most_recent_date[[alert_type_]], streak, as.integer(0))) %>%
            group_by(Zone, Corridor, SignalID, CallPhase, Detector, ApproachDesc, Name, Alert) %>%
            summarize(
                Occurrences = n(),
                Streak = max(Streak) #, MaxDate = max(Date)
            ) %>%
            ungroup() %>%
            arrange(desc(Streak), desc(Occurrences))

        if (alert_type_ != "Bad Vehicle Detection") {
            table_df <- table_df %>% select(-ApproachDesc)
        }

        if (active_streak == "Active") {
            table_df <- table_df %>% filter(Streak > 0)
            df <- df %>% right_join(select(table_df, SignalID, Detector))
        } else if (active_streak == "Active 3-days") {
            table_df <- table_df %>% filter(Streak > 2)
            df <- df %>% right_join(select(table_df, SignalID, Detector))
        }

        if (alert_type_ == "Missing Records") {

            plot_df <- df %>%
                arrange(
                    as.integer(as.character(SignalID)), Name) %>%
                mutate(
                    signal_phase = paste0(as.character(SignalID), ": ", Name)) %>%
                select(-Name)

            table_df <- table_df %>% select(-c(CallPhase, Detector))

        } else if (alert_type_ == "Bad Vehicle Detection" || alert_type_ == "Bad Ped Pushbuttons") {

            plot_df <- df %>%
                arrange(
                    as.integer(as.character(SignalID)),
                    Name,
                    as.integer(as.character(Detector))) %>%
                mutate(
                    signal_phase = paste0(as.character(SignalID), ": ", Name, " | det ", Detector)) %>%
                select(-c(Name, Detector))

        } else if (alert_type_ == "No Camera Image") {

            plot_df <- df %>%
                arrange(
                    as.character(SignalID), Name) %>%
                mutate(
                    signal_phase = paste0(as.character(SignalID), ": ", Name)) %>%
                select(-Name)

            table_df <- table_df %>% select(-c(CallPhase, Detector))

        } else {

            plot_df <- df %>%
                arrange(
                    as.integer(as.character(SignalID)),
                    Name,
                    as.integer(as.character(CallPhase))) %>%
                mutate(
                    signal_phase = paste0(as.character(SignalID), ": ", Name, " | ph ", CallPhase)) %>%
                select(-Name)

            if (alert_type_ != "Count") {
                table_df <- table_df %>% select(-Detector)
            }
        }

        plot_df <- plot_df %>%
            mutate(signal_phase = if_else(
                Zone_Group == "Ramp Meters",
                stringr::str_replace(signal_phase, " \\| ", glue(" | {ApproachDesc} | ")),
                as.character(signal_phase))) %>%
            mutate(signal_phase = ordered(signal_phase, levels = unique(signal_phase)))

        intersections <- length(unique(plot_df$signal_phase))

    } else { #df_is_empty

        plot_df <- data.frame()
        table_df <- data.frame()
        intersections <- 0
    }

    list("plot" = plot_df,
         "table" = table_df,
         "intersections" = intersections)
}



plot_alerts <- function(df, date_range) {

    if (nrow(df) > 0) {

        start_date <- max(min(df$Date), date_range[1])
        end_date <- max(max(df$Date), date_range[2])

        if (df$Zone_Group[[1]] == "Ramp Meters") {
            scale_fill <- scale_fill_manual(values = colrs, name = "Phase")
            fill_variable <- as.name("CallPhase")
        } else {
            scale_fill <- scale_fill_gradient(low = "#fc8d59", high = "#7f0000", limits = c(0,90))
            fill_variable <- as.name("streak")
        }

        p <- ggplot() +

            # tile plot
            geom_tile(data = df,
                      aes(x = Date,
                          y = signal_phase,
                          fill = !!fill_variable), # streak),  # CallPhase),
                      color = "white") +

            scale_fill +

            # fonts, text size and labels and such
            theme(panel.grid.major = element_blank(),
                  panel.grid.minor = element_blank(),
                  axis.ticks.x = element_line(color = "gray50"),
                  axis.text.x = element_text(size = 11),
                  axis.text.y = element_text(size = 11),
                  axis.ticks.y = element_blank(),
                  axis.title = element_text(size = 11),
                  legend.position = "right") +

            scale_x_date(position = "top") + #, limits = date_range) #+
            scale_y_discrete(limits = rev(levels(df$signal_phase))) +
            labs(x = "",
                 y = "") + #Intersection (and phase, if applicable)") +


            # draw white gridlines between tick labels
            geom_vline(xintercept = as.numeric(seq(start_date, end_date, by = "1 day")) - 0.5,
                       color = "white")

        if (length(unique(df$signal_phase)) > 1) {
            p <- p +
                geom_hline(yintercept = seq(1.5, length(unique(df$signal_phase)) - 0.5, by = 1),
                           color = "white")
        }
        p
    }
}



plot_empty <- function(zone) {

    ggplot() +
        annotate("text", x = 1, y = 1,
                 label = "No Data") +
        theme(panel.grid.major = element_blank(),
              panel.grid.minor = element_blank(),
              axis.ticks = element_blank(),
              axis.text = element_blank(),
              axis.title = element_blank())
}




# Functions for Corridor Summary Table ------------------------------------


metric_formats <- function(x) { # workaround function to format the metrics differently
    sapply(x, function(x) {
        if (!is.na(x)) {
            if (x <= 1) {
                as_pct(x)
            } else if ((x <= 10) & (x - round(x, 0) != 0)) { # TTI and PTI, hack for oustanding tasks
                as_2dec(x)
            } else { # throughput and outstanding tasks
                as_int(x)
            }
        } else {
            return("N/A")
        }
    })
}



getcolorder <- function(no.corridors) {
    getcolorder <- c(1, 2)
    for (i in 1:no.corridors) {
        getcolorder <- c(getcolorder, i + 2, i + no.corridors + 2)
    }
    for (i in 1:no.corridors) {
        getcolorder <- c(getcolorder, i + no.corridors * 2 + 2, i + no.corridors * 3 + 2) #updated to include checks for deltas
    }
    getcolorder
}



get_corridor_summary_table <- function(data) {

    #' Creates an html table for a corridor by zone in a month
    #' with all metrics color coded by whether they met goal
    #' and whether they're trending up or down.
    #'
    #' @param data corridor_summary_table
    #' @param zone_group a Zone Group
    #' @return An html table for markdown or shiny

    #if (zone_group %in% unique(data$Zone_Group)) {
        # table for plotting
        dt <- data %>%
            select(-c(seq(5, 25, 2)), -Zone_Group, -Month) %>%
            gather("Metric", "value", 2:12) %>%
            spread(Corridor, value)
        dt <- dt[order(match(dt$Metric, metric.order)), ]
        dt$Metrics <- metric.names
        dt$Goal <- metric.goals
        dt <- select(dt, c((ncol(dt) - 1):ncol(dt), 2:(ncol(dt) - 2))) # vary based on # of columns
        setDT(dt)

        # table with deltas - to be used for formatting data table
        dt.deltas <- data %>%
            select(-c(seq(4, 24, 2)), -Zone_Group, -Month) %>%
            rename_at(vars(ends_with(".delta")), ~str_replace(., ".delta", "")) %>%
            gather("Metric", "value", 2:12) %>%
            spread(Corridor, value)
        dt.deltas <- dt.deltas[order(match(dt.deltas$Metric, metric.order)), ]
        dt.deltas$Metrics <- metric.names
        dt.deltas$Goal <- metric.goals
        # vary based on # of columns
        dt.deltas <- select(
            dt.deltas,
            c((ncol(dt.deltas) - 1):ncol(dt.deltas), 2:(ncol(dt.deltas) - 2)))
        setDT(dt.deltas)

        # table with checks on whether or not we're meeting
        # the various metrics for the month - also to be used formatting data table
        dt.checks <- copy(dt)
        setDT(dt.checks)
        dt.checks[Metrics == "Traffic Volume (Throughput)", (3:ncol(dt.checks)) := dt.deltas[Metrics == "Traffic Volume (Throughput)", 3:ncol(dt.deltas)]] # replace out throughput check with delta
        dt.checks[Metrics == "Outstanding Tasks", (3:ncol(dt.checks)) := dt.deltas[Metrics == "Outstanding Tasks", 3:ncol(dt.deltas)]] # replace out tasks check with delta
        dt.checks[, Goal.Numeric := metric.goals.numeric]
        dt.checks[, Goal.Sign := metric.goals.sign]
        dt.checks[, (3:(ncol(dt.checks) - 2)) := lapply(.SD, function(x) {
            ifelse((x >= Goal.Numeric & Goal.Sign == ">") | (x <= Goal.Numeric & Goal.Sign == "<"), "Meeting Goal", "Not Meeting Goal")
        }), .SDcols = 3:(ncol(dt.checks) - 2)]
        dt.checks[, c((ncol(dt.checks) - 1):ncol(dt.checks)) := NULL]


        #table with checks on whether or not delta shows improvement or deterioration - also to be used formatting data table
        dt.deltas.checks <- copy(dt.deltas)
        setDT(dt.deltas.checks)
        dt.deltas.checks[,Goal.Sign := metric.goals.sign]
        dt.deltas.checks[,(3:(ncol(dt.deltas.checks)-1)) := lapply(.SD,function(x) {
            ifelse((x >= 0 & Goal.Sign == ">") | (x <= 0 & Goal.Sign == "<"), "Improvement", "Deterioration")
        }),.SDcols = 3:(ncol(dt.deltas.checks) - 1)]
        dt.deltas.checks[, ncol(dt.deltas.checks) := NULL]

        # overall data table
        dt.overall <- dt[dt.deltas, on = .(Metrics = Metrics, Goal = Goal)] %>%
            rename_at(vars(starts_with("i.")), ~str_replace(., "i.", "Delta."))
        dt.overall <- dt.overall[dt.checks, on = .(Metrics = Metrics, Goal = Goal)] %>%
            rename_at(vars(starts_with("i.")), ~str_replace(., "i.", "Check.Goal."))
        dt.overall <- dt.overall[dt.deltas.checks,on = .(Metrics=Metrics,Goal=Goal)] %>%
            rename_at(vars(starts_with("i.")), ~str_replace(., "i.", "Check.Delta."))

        no.corridors <- (ncol(dt.overall) - 2) / 4 # how many corridors are we working with?

        ###########
        # DATA TABLE DISPLAY AND FORMATTING
        ###########

        # update formatting for individual rows
        dt.overall[, (3:(3 + no.corridors - 1)) := lapply(.SD, function(x) metric_formats(x)), .SDcols = (3:(3 + no.corridors - 1))]
        dt.overall[11, (3:(3+no.corridors - 1)) := lapply(.SD, function (x) {
            ifelse(x=="100.0%", "1", ifelse(x == "0.0%", "0", x))
        }),.SDcols = (3:(3 + no.corridors - 1))] #hack for tasks

        # function to get columns lined up with deltas immediately after the metrics
        setcolorder(dt.overall, getcolorder(no.corridors))


        # formatter for just metrics (deltas in separate column)
        format.metrics <- sapply(names(dt.overall)[seq(3, no.corridors * 2 + 2, 2)], function(x) {
            eval(
                parse(
                    text = sub(
                        "_SUB_",
                        paste0("`Check.Goal.", x, "`"),
                        "formatter(\"span\", style = ~ style(display = \"block\",
                \"border-radius\" = \"4px\", \"padding-right\" = \"4px\",
                \"color\" = ifelse(_SUB_=='Meeting Goal', yes.color, no.color)))"
                    )
                )
            )
        }, simplify = F, USE.NAMES = T)

        # formatter for deltas (separate column - arrows)
        format.deltas <- sapply(names(dt.overall)[seq(4, no.corridors * 2 + 2, 2)], function(x) {
            eval( # evaluate the following expression
                parse(
                    text = # parse the following string to an expression
                        gsub(
                            "_SUB_", # find "_SUB_"
                            x,
                            "formatter(\"span\", style = ~ style(display = \"block\",
                        \"border-radius\" = \"4px\", \"padding-right\" = \"4px\",
                        \"color\" = ifelse(`Check._SUB_`=='Improvement', yes.color, no.color)),
                        ~ icontext(sapply(`_SUB_`, function(x) if (!is.na(x)) {if (x > 0) 'arrow-up' else if (x < 0) 'arrow-down'} else '')))")))
        }, simplify = F, USE.NAMES = T)


        # columns checking whether we're meeting checks - HIDE in formattable
        hide.checks <- sapply(names(dt.overall[, (ncol(dt.overall) - no.corridors * 2 + 1):(ncol(dt.overall))]),
                              function(x) eval(parse(text = sub("_SUB_", as.character(x), "`_SUB_` = F"))),
                              simplify = F, USE.NAMES = T
        )


        formattable::formattable(dt.overall,
                                 align = c("l", "c", rep(c("r", "l"), no.corridors)),
                                 c( # `Goal` = formatter("span", style = ~ style(width="1000px")),
                                     format.metrics,
                                     format.deltas,
                                     hide.checks
                                 )
        )

}


get_zone_group_text_table <- function(month, zone_group) {

    #' Creates an html table for a corridor by zone in a month
    #'
    #' @param month Month in date format
    #' @param zone_group a Zone Group
    #' @return An html table

    #current_month <- months[order(months)][match(month, months.formatted)]
    tt <- filter(df.text, Month == ymd(current_month), Zone == zone_group)
    colnames(tt)[3] <- "&nbsp"
    formattable::formattable(
        tt,
        align = "l",
        list(
            Month = F,
            Zone = F
        )
    )
}




# Health Metrics UI Functions ---------------------------------------------


# separate functions for maintenance/ops/safety datatables since formatting is different - would be nice to abstractify
get_monthly_maintenance_health_table <- function(data_) {

    all_cols <- names(data_)
    rounded_cols <- c(all_cols[endsWith(all_cols, "Score")], "Flash Events")
    percent_cols <- c("Percent Health", "Missing Data", all_cols[endsWith(all_cols, "Uptime")])

    datatable(data_,
              filter = "top",
              rownames = FALSE,
              extensions = "Scroller",
              options = list(
                  scrollY = 550,
                  scrollX = TRUE,
                  pageLength = 1000,
                  dom = "t",
                  selection = "none"
              )
    ) %>%
        formatPercentage(percent_cols) %>%
        formatRound(rounded_cols, digits = 0) %>%
	    formatStyle("Percent Health", fontWeight = 'bold', fontSize = '20px') %>% #edit 5/7/21 - GDOT requested that % health "stand out more"
        formatStyle("Subcorridor",
                    target = "row",
                    backgroundColor = styleEqual("ALL", "lightgray"),
                    fontStyle = styleEqual("ALL", "italic")
        ) %>%
        formatStyle("Corridor",
                    target = "row",
                    backgroundColor = styleEqual("ALL", "gray"),
                    fontWeight = styleEqual("ALL", "bold")
        ) %>%
        formatStyle("Missing Data",
                    color = styleInterval(c(0.05, 0.3, 0.5), c("#1A1A1A", "#D6604D", "#B2182B", "#67001F")), #  c("black", "gold", "orangered", "crimson")),
                    borderRight = "2px solid #ddd"
        ) %>%
        formatStyle("Flash Events Score", borderRight = "2px solid #ddd") %>%
        formatStyle("Flash Events", borderRight = "2px solid #ddd")
}



# separate functions for maintenance/ops/safety datatables since formatting is different - would be nice to abstractify
get_monthly_operations_health_table <- function(data_) {

    all_cols <- names(data_)
    rounded0_cols <- all_cols[endsWith(all_cols, "Score")]
    rounded1_cols <- "Ped Delay"
    rounded2_cols <- c("Platoon Ratio", "Travel Time Index", "Buffer Index")
    percent_cols <- c("Percent Health", "Missing Data", "Split Failures")

    datatable(data_,
              filter = "top",
              rownames = FALSE,
              extensions = "Scroller",
              options = list(
                  scrollY = 550,
                  scrollX = TRUE,
                  pageLength = 1000,
                  dom = "t",
                  selection = "none"
              )
    ) %>%
        formatPercentage(percent_cols) %>%
        formatRound(rounded0_cols, digits = 0) %>%
        formatRound(rounded1_cols, digits = 1) %>%
        formatRound(rounded2_cols, digits = 2) %>%
	    formatStyle("Percent Health", fontWeight = 'bold', fontSize = '20px') %>% #edit 5/7/21 - GDOT requested that % health "stand out more"
        formatStyle("Subcorridor",
                    target = "row",
                    backgroundColor = styleEqual("ALL", "lightgray"),
                    fontStyle = styleEqual("ALL", "italic")
        ) %>%
        formatStyle("Corridor",
                    target = "row",
                    backgroundColor = styleEqual("ALL", "gray"),
                    fontWeight = styleEqual("ALL", "bold")
        ) %>%
        formatStyle("Missing Data",
                    color = styleInterval(c(0.05, 0.3, 0.5), c("#1A1A1A", "#D6604D", "#B2182B", "#67001F")), #  c("black", "gold", "orangered", "crimson")),
                    borderRight = "2px solid #ddd"
        ) %>%
        formatStyle("Buffer Index Score", borderRight = "2px solid #ddd") %>%
        formatStyle("Buffer Index", borderRight = "2px solid #ddd")
}



# separate functions for maintenance/ops/safety datatables since formatting is different - would be nice to abstractify
get_monthly_safety_health_table <- function(data_) {

    all_cols <- names(data_)
    rounded0_cols <- all_cols[endsWith(all_cols, "Score")]
    # rounded1_cols <- NULL
    rounded2_cols <- all_cols[endsWith(all_cols, "Index")]
    percent_cols <- c("Percent Health", "Missing Data")

    datatable(data_,
              filter = "top",
              rownames = FALSE,
              extensions = "Scroller",
              options = list(
                  scrollY = 550,
                  scrollX = TRUE,
                  pageLength = 1000,
                  dom = "t",
                  selection = "none"
              )
    ) %>%
        formatPercentage(percent_cols) %>%
        formatRound(rounded0_cols, digits = 0) %>%
        # formatRound(rounded1_cols, digits = 1) %>%
        formatRound(rounded2_cols, digits = 2) %>%
	    formatStyle("Percent Health", fontWeight = 'bold', fontSize = '20px') %>% #edit 5/7/21 - GDOT requested that % health "stand out more"
        formatStyle("Subcorridor",
                    target = "row",
                    backgroundColor = styleEqual("ALL", "lightgray"),
                    fontStyle = styleEqual("ALL", "italic")
        ) %>%
        formatStyle("Corridor",
                    target = "row",
                    backgroundColor = styleEqual("ALL", "gray"),
                    fontWeight = styleEqual("ALL", "bold")
        ) %>%
        formatStyle("Missing Data",
                    color = styleInterval(c(0.05, 0.3, 0.5), c("#1A1A1A", "#D6604D", "#B2182B", "#67001F")), #  c("black", "gold", "orangered", "crimson")),
                    borderRight = "2px solid #ddd"
        ) # %>%
        # formatStyle("Buffer Index Score", borderRight = "2px solid #ddd") %>%
        # formatStyle("Buffer Index", borderRight = "2px solid #ddd")
}


# function to filter health data based on user inputs
get_health_data_filtered <- function(data_, zone_group_, corridor_) {

    data_ <- mutate(data_, Zone_Group = Zone)

    if (corridor_ == "All Corridors") {
        # filter based on Zone with accommodations for All RTOP/RTOP1/RTOP2
        filter_mr_data(data_, zone_group_)
    } else {
        filter(data_, Corridor == corridor_)
    }
}
