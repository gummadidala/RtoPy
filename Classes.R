#
# Define classes and functions that operate on classes
# Possibly make this file all about the metrics class and rename to metrics.R
#

suppressMessages({
    library(shiny)
    library(yaml)
})

metrics <- read_yaml("metrics.yaml")


# Function to create a metric (child) by specifying only differences from another (parent) metric.
# Created for health metrics for which there are many, but nearly identical.
inherit <- function(parent, child) {
    tryCatch({
        keys <- unique(c(names(parent), names(child)))
        x <- setNames(Map(c, parent[keys], child[keys]), keys)
        sapply(x, tail, 1)
    }, warning = function(w) {
        print(paste("Can't inherit child from parent:", w))
        NULL
    })
}


vpd <- structure(
    metrics[["daily_traffic_volume"]], class = "metric"
)
am_peak_vph <- structure(
    metrics[["am_peak_hour_volume"]], class = "metric"
)
pm_peak_vph <- structure(
    metrics[["pm_peak_hour_volume"]], class = "metric"
)
throughput <- structure(
    metrics[["throughput"]], class = "metric"
)
aog <- arrivals_on_green <- structure(
    metrics[["arrivals_on_green"]], class = "metric"
)
progression_ratio <- structure(
    metrics[["progression_ratio"]], class = "metric"
)
queue_spillback_rate <- structure(
    metrics[["queue_spillback_rate"]], class = "metric"
)
peak_period_split_failures <- structure(
    metrics[["peak_period_split_failures"]], class = "metric"
)
off_peak_split_failures <- structure(
    metrics[["off_peak_split_failures"]], class = "metric"
)
travel_time_index <- structure(
    metrics[["travel_time_index"]], class = "metric"
)
planning_time_index <- structure(
    metrics[["planning_time_index"]], class = "metric"
)
average_speed <- structure(
    metrics[["average_speed"]], class = "metric"
)
daily_pedestrian_pushbuttons <- structure(
    metrics[["daily_pedestrian_pushbuttons"]], class = "metric"
)
detector_uptime <- structure(
    inherit(metrics[["uptime"]], metrics[["detector_uptime"]]), class = "metric"
)
ped_button_uptime <- structure(
    inherit(metrics[["uptime"]], metrics[["ped_button_uptime"]]), class = "metric"
)
cctv_uptime <- structure(
    inherit(metrics[["uptime"]], metrics[["cctv_uptime"]]), class = "metric"
)
comm_uptime <- structure(
    inherit(metrics[["uptime"]], metrics[["comm_uptime"]]), class = "metric"
)
rsu_uptime <- structure(
    inherit(metrics[["uptime"]], metrics[["rsu_uptime"]]), class = "metric"
)
tasks <- structure(
    inherit(metrics[["tasks_template"]], metrics[["tasks"]]), class = "metric"
)
tasks_by_type <- structure(
    inherit(metrics[["tasks_template"]], metrics[["tasks_by_type"]]), class = "metric"
)
tasks_by_subtype <- structure(
    inherit(metrics[["tasks_template"]], metrics[["tasks_by_subtype"]]), class = "metric"
)
tasks_by_source <- structure(
    inherit(metrics[["tasks_template"]], metrics[["tasks_by_source"]]), class = "metric"
)
tasks_reported <- structure(
    inherit(metrics[["tasks_template"]], metrics[["tasks_reported"]]), class = "metric"
)
tasks_resolved <- structure(
    inherit(metrics[["tasks_template"]], metrics[["tasks_resolved"]]), class = "metric"
)
tasks_outstanding <- structure(
    inherit(metrics[["tasks_template"]], metrics[["tasks_outstanding"]]), class = "metric"
)
tasks_over45 <- structure(
    inherit(metrics[["tasks_template"]], metrics[["tasks_over45"]]), class = "metric"
)
tasks_mttr <- structure(
    inherit(metrics[["tasks_template"]], metrics[["tasks_mttr"]]), class = "metric"
)

maint_percent_health <- structure(
    inherit(metrics[["health_metrics"]], metrics[["maint_percent_health"]]), class = "metric"
)
maint_missing_data <- structure(
    inherit(metrics[["health_metrics"]], metrics[["maint_missing_data"]]), class = "metric"
)
du_score <- structure(
    inherit(metrics[["health_metrics"]], metrics[["du_score"]]), class = "metric"
)
pau_score <- structure(
    inherit(metrics[["health_metrics"]], metrics[["pau_score"]]), class = "metric"
)
cu_score <- structure(
    inherit(metrics[["health_metrics"]], metrics[["cu_score"]]), class = "metric"
)
cctv_score <- structure(
    inherit(metrics[["health_metrics"]], metrics[["cctv_score"]]), class = "metric"
)
flash_score <- structure(
    inherit(metrics[["health_metrics"]], metrics[["flash_score"]]), class = "metric"
)
du_health <- structure(
    inherit(metrics[["health_metrics"]], metrics[["du_health"]]), class = "metric"
)
pau_health <- structure(
    inherit(metrics[["health_metrics"]], metrics[["pau_health"]]), class = "metric"
)
cu_health <- structure(
    inherit(metrics[["health_metrics"]], metrics[["cu_health"]]), class = "metric"
)
cctv_health <- structure(
    inherit(metrics[["health_metrics"]], metrics[["cctv_health"]]), class = "metric"
)
flash_health <- structure(
    inherit(metrics[["health_metrics"]], metrics[["flash_health"]]), class = "metric"
)

ops_missing_data <- structure(
    inherit(metrics[["health_metrics"]], metrics[["maint_missing_data"]]), class = "metric"
)
ops_percent_health <- structure(
    inherit(metrics[["health_metrics"]], metrics[["ops_percent_health"]]), class = "metric"
)
pr_score <- structure(
    inherit(metrics[["health_metrics"]], metrics[["pr_score"]]), class = "metric"
)
pd_score <- structure(
    inherit(metrics[["health_metrics"]], metrics[["pd_score"]]), class = "metric"
)
sf_score <- structure(
    inherit(metrics[["health_metrics"]], metrics[["sf_score"]]), class = "metric"
)
tti_score <- structure(
    inherit(metrics[["health_metrics"]], metrics[["tti_score"]]), class = "metric"
)
bi_score <- structure(
    inherit(metrics[["health_metrics"]], metrics[["bi_score"]]), class = "metric"
)
pr_health <- structure(
    inherit(metrics[["health_metrics"]], metrics[["pr_health"]]), class = "metric"
)
pd_health <- structure(
    inherit(metrics[["health_metrics"]], metrics[["pd_health"]]), class = "metric"
)
sf_health <- structure(
    inherit(metrics[["health_metrics"]], metrics[["sf_health"]]), class = "metric"
)
tti_health <- structure(
    inherit(metrics[["health_metrics"]], metrics[["tti_health"]]), class = "metric"
)
bi_health <- structure(
    inherit(metrics[["health_metrics"]], metrics[["bi_health"]]), class = "metric"
)

safety_percent_health <- structure(
    inherit(metrics[["health_metrics"]], metrics[["safety_percent_health"]]), class = "metric"
)
safety_missing_data <- structure(
    inherit(metrics[["health_metrics"]], metrics[["safety_missing_data"]]), class = "metric"
)
bpsi_score <- structure(
    inherit(metrics[["health_metrics"]], metrics[["bpsi_score"]]), class = "metric"
)
rsi_score <- structure(
    inherit(metrics[["health_metrics"]], metrics[["rsi_score"]]), class = "metric"
)
cri_score <- structure(
    inherit(metrics[["health_metrics"]], metrics[["cri_score"]]), class = "metric"
)
kabco_score <- structure(
    inherit(metrics[["health_metrics"]], metrics[["kabco_score"]]), class = "metric"
)
bpsi_health <- structure(
    inherit(metrics[["health_metrics"]], metrics[["bpsi_health"]]), class = "metric"
)
rsi_health <- structure(
    inherit(metrics[["health_metrics"]], metrics[["rsi_health"]]), class = "metric"
)
cri_health <- structure(
    inherit(metrics[["health_metrics"]], metrics[["cri_health"]]), class = "metric"
)
kabco_health <- structure(
    inherit(metrics[["health_metrics"]], metrics[["kabco_health"]]), class = "metric"
)




get_descriptionBlock <- function(x, zone_group, month, quarter = NULL) {

    if (is.null(quarter)) { # want monthly, not quarterly data
        vals <- cor$mo[[x$table]] %>%
            dplyr::filter(Corridor == zone_group & Month == month) %>% as.list()
    } else {
        vals <- cor$qu[[x$table]] %>%
            dplyr::filter(Corridor == zone_group & Quarter == quarter) %>% as.list()
    }

    vals$delta <- if_else(is.na(vals$delta), 0, vals$delta)

    if (vals$delta > 0) {
        delta_prefix <- " +"
        color <- "success"
        color_icon <- "caret-up"
    } else if (vals$delta == 0) {
        delta_prefix <- "  "
        color <- "black"
        color_icon <- NULL
    } else { # vals$delta < 0
        delta_prefix <- "  "
        color <- "failure"
        color_icon <- "caret-down"
    }

    value <- data_format(x$data_type)(vals[[x$variable]])
    delta <- glue('{delta_prefix} {as_pct(vals$delta)}')


    validate(need(value, message = "NA"))


    descriptionBlock(
        number = delta,
        numberColor = color,
        numberIcon = color_icon,
        header = value,
        text = x$label,
        rightBorder = TRUE,
        marginBottom = FALSE
    )
}



get_minimal_trendline <- function(x, zone_group) {
    ggplot(
        data = filter(cor$wk[[x$table]],
                      as.character(Zone_Group) == as.character(Corridor),
                      Zone_Group == zone_group),
        mapping = aes_string(x = "Date", y = x$variable)) +
        geom_line(color = GDOT_BLUE) +
        theme_void()
}



# Empty plot - space filler
x0 <- list(zeroline = FALSE, ticks = "", showticklabels = FALSE, showgrid = FALSE)
p0 <- plot_ly(type = "scatter", mode = "markers") %>% layout(xaxis = x0, yaxis = x0)



get_valuebox_value <- function(metric, zone_group, corridor, month, quarter = NULL, line_break = FALSE) {

    if (corridor == "All Corridors") {
        corridor <- NULL
    }

    if (is.null(quarter)) { # want monthly, not quarterly data
        vals <- query_data(
            metric,
            level = "corridor",
            resolution = "monthly",
            zone_group = zone_group,
            corridor = corridor,
            month = month,
            upto = FALSE
        )
    } else {
        vals <- query_data(
            metric,
            level = "corridor",
            resolution = "quarterly",
            zone_group = zone_group,
            corridor = corridor,
            quarter = quarter,
            upto = FALSE
        )
    }

    if (is.null(corridor)) {
        if (zone_group %in% c("All RTOP", "RTOP1", "RTOP2", "Zone 7")) {
            vals <- subset(vals, Zone_Group == zone_group)
        } else if (is.null(corridor)) {
            vals <- subset(vals, Zone_Group == Corridor)
        }
    }
    vals <- as.list(vals)

    value <- data_format(metric$data_type)(vals[[metric$variable]])
    shiny::validate(need(value, message = "NA"))


    vals$delta <- if_else(is.na(vals$delta), 0, vals$delta)

    if (vals$delta > 0) {
        delta_prefix <- "+"
    } else if (vals$delta == 0) {
        delta_prefix <- " "
    } else { # vals$delta < 0
        delta_prefix <- " "
    }

    delta <- glue("({delta_prefix}{as_pct(vals$delta)})")

    if (line_break) {
        tags$div(HTML(paste(
            value,
            tags$p(delta, style = "font-size: 50%; line-height: 5px; padding: 0 0 0.2em 0;")
        )))
    } else {
        tags$div(HTML(paste(
            value,
            tags$span(delta, style = "font-size: 70%;")
        )))
    }
}



summary_plot <- function(metric, zone_group, corridor, month) {

    ax <- list(
        title = "", showticklabels = TRUE, showgrid = FALSE)
    ay <- list(
        title = "", showticklabels = FALSE, showgrid = FALSE,
        zeroline = FALSE, hoverformat = tick_format(metric$data_type))


    if (!is.null(corridor)) {
        if (corridor == "All Corridors") {
            corridor <- NULL
        }
    }

    data <- query_data(
        metric,
        level = "corridor",
        resolution = "monthly",
        zone_group = zone_group,
        corridor = corridor,
        month = month,
        upto = TRUE
    )

    if (is.null(corridor)) {
        if (zone_group %in% c("All RTOP", "RTOP1", "RTOP2", "Zone 7")) {
            data <- subset(data, Zone_Group == zone_group)
        } else {
            data <- subset(data, Zone_Group == Corridor)
        }
    }
    shiny::validate(need(nrow(data) > 0, "No Data"))

    first <- data[which.min(data$Month), ]
    last <- data[which.max(data$Month), ]

    p <- plot_ly(type = "scatter", mode = "markers")

    if (!is.null(metric$goal)) {
        p <- p %>%
            add_trace(
                x = data$Month,
                y = metric$goal,
                name = "Goal",
                line = list(color = DARK_GRAY, width = 1),
                mode = 'lines') %>%
            add_trace(
                x = data$Month,
                y = data[[metric$variable]],
                name = metric$label,
                line = list(color = metric$highlight_color),
                mode = 'lines',
                fill = 'tonexty',
                fillcolor = metric$fill_color)
    } else {
        p <- p %>%
            add_trace(
                x = data$Month,
                y = first[[metric$variable]],
                name = NULL,
                line = list(color = LIGHT_GRAY_BAR, width = 1),
                mode = 'lines') %>%
            add_trace(
                x = data$Month,
                y = data[[metric$variable]],
                name = metric$label,
                line = list(color = metric$highlight_color),
                mode = 'lines'
            )
    }
    p %>%
        add_annotations(x = 0,
                        y = first[[metric$variable]],
                        text = data_format(metric$data_type)(first[[metric$variable]]),
                        showarrow = FALSE,
                        xanchor = "right",
                        xref = "paper",
                        width = 50,
                        align = "right",
                        borderpad = 5) %>%
        add_annotations(x = 1,
                        y = last[[metric$variable]],
                        text = data_format(metric$data_type)(last[[metric$variable]]),
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



get_trend_multiplot <- function(metric, level, zone_group, month, line_chart = "weekly", accent_average = TRUE) {

    if (class(month) == "character") {
        month <- as_date(month)
    }
    var_ <- as.name(metric$variable)

    mdf <- query_data(metric, level, zone_group = zone_group, month = month, upto = FALSE)

    if ((line_chart == "weekly" & !metric$has_weekly) | (line_chart == "monthly")) {
        wdf <- query_data(metric, level, resolution = "monthly", zone_group = zone_group, month = month, upto = TRUE) %>%
            rename(Date = Month)
    } else {
        wdf <- query_data(metric, level, resolution = line_chart, zone_group = zone_group, month = month, upto = TRUE)
    }


    if (nrow(mdf) > 0 & nrow(wdf) > 0) {
        # Current Month Data
        mdf <- mdf %>%
            filter(Month == month) %>%
            arrange(!!var_) %>%
            mutate(var = !!var_,
                   col = factor(ifelse(accent_average & Corridor == zone_group, DARK_GRAY_BAR, LIGHT_GRAY_BAR)),
                   text_col = factor(ifelse(accent_average & Corridor == zone_group, "white", "black")),
                   Corridor = factor(Corridor, levels = Corridor))

        sdm <- SharedData$new(mdf, ~Corridor, group = "grp")

        bar_chart <- plot_ly(sdm,
                             type = "bar",
                             x = ~var,
                             y = ~Corridor,
                             marker = list(color = ~col),
                             text = ~data_format(metric$data_type)(var),
                             textposition = "auto",
                             insidetextfont = list(color = ~text_col),
                             name = "",
                             customdata = ~glue(paste(
                                 "<b>{Description}</b>",
                                 "<br>{metric$label}: <b>{data_format(metric$data_type)(var)}</b>")),
                             hovertemplate = "%{customdata}",
                             hoverlabel = list(font = list(family = "Source Sans Pro"))
        ) %>% layout(
                barmode = "overlay",
                xaxis = list(title = "Selected Month",
                             zeroline = FALSE,
                             tickformat = tick_format(metric$data_type)),
                yaxis = list(title = ""),
                showlegend = FALSE,
                font = list(size = 11),
                margin = list(pad = 4,
                              l = 100)
            )
        if (!is.null(metric$goal)) {
            bar_chart <- bar_chart %>%
                add_lines(x = metric$goal,
                          y = ~Corridor,
                          mode = "lines",
                          marker = NULL,
                          line = list(color = LIGHT_RED),
                          name = "Goal",
                          showlegend = FALSE)
        }

        # Weekly Data - historical trend
        wdf <- wdf %>%
            mutate(var = !!var_,
                   col = factor(ifelse(accent_average & Corridor == zone_group, 1, 0), levels = c(0, 1)),
                   Corridor = factor(Corridor)) %>%
            filter(!is.na(var)) %>%
            group_by(Corridor)

        sdw <- SharedData$new(wdf, ~Corridor, group = "grp")

        weekly_line_chart <- plot_ly(sdw,
                                     type = "scatter",
                                     mode = "lines",
                                     x = ~Date,
                                     y = ~var,
                                     color = ~col, colors = c(LIGHT_GRAY_BAR, BLACK),
                                     alpha = 0.6,
                                     name = "",
                                     customdata = ~glue(paste(
                                         "<b>{Description}</b>",
                                         "<br>Week of: <b>{format(Date, '%B %e, %Y')}</b>",
                                         "<br>{metric$label}: <b>{data_format(metric$data_type)(var)}</b>")),
                                     hovertemplate = "%{customdata}",
                                     hoverlabel = list(font = list(family = "Source Sans Pro"))
        ) %>% layout(xaxis = list(title = "Weekly Trend"),
                   yaxis = list(tickformat = tick_format(metric$data_type),
                                hoverformat = tick_format(metric$data_type)),
                   title = "__plot1_title__",
                   showlegend = FALSE,
                   margin = list(t = 50)
            )

        # Hourly Data - current month
        if (!is.null(metric$hourly_table)) {

            hdf <- query_data(
                metric,
                level, resolution = "monthly", hourly = TRUE,
                zone_group = zone_group, month = month, upto = FALSE
                ) %>%
                mutate(var = !!var_,
                       col = factor(ifelse(accent_average & Corridor == zone_group, 1, 0), levels = c(0, 1))) %>%
                group_by(Corridor)

            sdh <- SharedData$new(hdf, ~Corridor, group = "grp")

            hourly_line_chart <- plot_ly(sdh) %>%
                add_lines(x = ~Hour,
                          y = ~var,
                          color = ~col, colors = c(LIGHT_GRAY_BAR, BLACK),
                          alpha = 0.6,
                          name = "",
                          customdata = ~glue(paste(
                              "<b>{Description}</b>",
                              "<br>Hour: <b>{format(Hour, '%l:%M %p')}</b>",
                              "<br>{metric$label}: <b>{data_format(metric$data_type)(var)}</b>")),
                          hovertemplate = "%{customdata}",
                          hoverlabel = list(font = list(family = "Source Sans Pro"))
                ) %>% layout(xaxis = list(title = metric$label),
                       yaxis = list(tickformat = tick_format(metric$data_type)),
                       title = "__plot2_title__",
                       showlegend = FALSE)

            s1 <- subplot(weekly_line_chart, p0, hourly_line_chart,
                          titleX = TRUE, heights = c(0.6, 0.1, 0.3), nrows = 3)
        } else {
            s1 <- weekly_line_chart
        }

        subplot(bar_chart, s1, titleX = TRUE, widths = c(0.2, 0.8), margin = 0.03) %>%
            layout(margin = list(l = 100),
                   title = metric$label) %>%
            highlight(
                color = metric$highlight_color,
                opacityDim = 0.9,
                defaultValues = c(zone_group),
                selected = attrs_selected(
                    insidetextfont = list(color = "white"),
                    textposition = "auto"),
                on = "plotly_click",
                off = "plotly_doubleclick")

    } else(
        no_data_plot("")
    )
}



travel_times_plot <- function(level, zone_group, month) {

    data_format <- data_format(travel_time_index$data_type)
    tick_format <- tick_format(travel_time_index$data_type)

    cor_monthly_tti <- query_data(
        travel_time_index, level = level, zone_group = zone_group, month = month, upto = TRUE)
    cor_monthly_pti <- query_data(
        planning_time_index, level = level, zone_group = zone_group, month = month, upto = TRUE)

    cor_monthly_tti_by_hr <- query_data(
        travel_time_index, level = level, zone_group = zone_group, month = month, hourly = TRUE, upto = FALSE)
    cor_monthly_pti_by_hr <- query_data(
        planning_time_index, level = level, zone_group = zone_group, month = month, hourly = TRUE, upto = FALSE)

    if (class(month) == "character") {
        month <- as_date(month)
    }

    mott <- full_join(cor_monthly_tti, cor_monthly_pti,
                      by = c("Corridor", "Zone_Group", "Month"),
                      suffix = c(".tti", ".pti")) %>%
        filter(!is.na(Corridor)) %>%
        mutate(bti = pti - tti) %>%
        select(Corridor, Zone_Group, Month, tti, pti, bti) %>%
        mutate(
            col = factor(ifelse(Corridor == zone_group, 1, 0), levels = c(0, 1)),
            text_col = ifelse(Corridor == zone_group, "white", "black"))


    hrtt <- full_join(cor_monthly_tti_by_hr, cor_monthly_pti_by_hr,
                      by = c("Corridor", "Zone_Group", "Hour"),
                      suffix = c(".tti", ".pti"))  %>%
        filter(!is.na(Corridor)) %>%
        mutate(bti = pti - tti) %>%
        select(Corridor, Zone_Group, Hour, tti, pti, bti) %>%
        mutate(
            col = factor(ifelse(Corridor == zone_group, 1, 0), levels = c(0, 1)),
            Corridor = factor(Corridor))

    mo_max <- round(max(mott$pti), 1) + 0.1
    hr_max <- round(max(hrtt$pti), 1) + 0.1

    mo_min <- round(min(mott$pti), 1) - 0.1
    hr_min <- round(min(hrtt$pti), 1) - 0.1


    if (nrow(mott) > 0 & nrow(hrtt) > 0) {

        mdf <- mott[mott$Month == month,] %>%
            arrange(tti) %>%
            mutate(Corridor = factor(Corridor, levels = Corridor)) %>%
            group_by(Corridor)

        sdb <- SharedData$new(mdf, ~Corridor, group = "grp")
        sdm <- SharedData$new(mott %>% group_by(Corridor), ~Corridor, group = "grp")
        sdh <- SharedData$new(hrtt %>% group_by(Corridor), ~Corridor, group = "grp")

        pbar <- plot_ly(sdb) %>%
            add_bars(x = ~tti,
                     y = ~Corridor,
                     text = ~data_format(tti),
                     color = ~col, colors = c(LIGHT_GRAY_BAR, BLACK),
                     textposition = "auto",
                     insidetextfont = list(color = "black"),  # ~text_col), # -- couldn't get this to work.
                     name = "",
                     customdata = ~glue(paste(
                         "<b>{Corridor}</b>",
                         "<br>Travel Time Index: <b>{data_format(tti)}</b>",
                         "<br>Planning Time Index: <b>{data_format(pti)}</b>")),
                     hovertemplate = "%{customdata}",
                     hoverlabel = list(font = list(family = "Source Sans Pro"))
            ) %>%
            add_bars(x = ~bti,
                     y = ~factor(Corridor, levels = Corridor),
                     text = ~data_format(pti),
                     color = I(LIGHT_BLUE),
                     textposition = "auto",
                     insidetextfont = list(color = "black"),
                     hoverinfo = "none") %>%
            layout(
                barmode = "stack",
                xaxis = list(title = glue("{format(month, '%b %Y')} TTI & PTI"),
                             zeroline = FALSE,
                             tickformat = tick_format,
                             range = c(0, 2)),
                yaxis = list(title = ""),
                showlegend = FALSE,
                font = list(size = 11),
                margin = list(pad = 4)
            )

        pttimo <- plot_ly(sdm) %>%
            add_lines(x = ~Month,
                      y = ~tti,
                      color = ~col, colors = c(LIGHT_GRAY_BAR, BLACK),
                      alpha = 0.6,
                      name = "",
                      customdata = ~glue(paste(
                          "<b>{Corridor}</b>",
                          "<br><b>{format(Month, '%B %Y')}</b>",
                          "<br>Travel Time Index: <b>{data_format(tti)}</b>")),
                      hovertemplate = "%{customdata}",
                      hoverlabel = list(font = list(family = "Source Sans Pro"))
            ) %>%
            layout(xaxis = list(title = travel_time_index$label),
                   yaxis = list(range = c(mo_min, mo_max),
                                tickformat = tick_format),
                   showlegend = FALSE)

        pttihr <- plot_ly(sdh) %>%
            add_lines(x = ~Hour,
                      y = ~tti,
                      color = ~col, colors = c(LIGHT_GRAY_BAR, BLACK),
                      alpha = 0.6,
                      name = "",
                      customdata = ~glue(paste(
                          "<b>{Corridor}</b>",
                          "<br>Hour: <b>{format(Hour, '%l:%M %p')}</b>",
                          "<br>Travel Time Index: <b>{data_format(tti)}</b>")),
                      hovertemplate = "%{customdata}",
                      hoverlabel = list(font = list(family = "Source Sans Pro"))
            ) %>%
            layout(xaxis = list(title = glue("{format(month, '%b %Y')} PTI by Hr")),
                   yaxis = list(range = c(hr_min, hr_max),
                                tickformat = tick_format),
                   showlegend = FALSE)

        pptimo <- plot_ly(sdm) %>%
            add_lines(x = ~Month,
                      y = ~pti,
                      color = ~col, colors = c(LIGHT_GRAY_BAR, BLACK),
                      alpha = 0.6,
                      name = "",
                      customdata = ~glue(paste(
                          "<b>{Corridor}</b>",
                          "<br><b>{format(Month, '%B %Y')}</b>",
                          "<br>Planning Time Index: <b>{data_format(pti)}</b>")),
                      hovertemplate = "%{customdata}",
                      hoverlabel = list(font = list(family = "Source Sans Pro"))) %>%
            layout(xaxis = list(title = planning_time_index$label),
                   yaxis = list(range = c(mo_min, mo_max),
                                tickformat = tick_format),
                   showlegend = FALSE)

        pptihr <- plot_ly(sdh) %>%
            add_lines(x = ~Hour,
                      y = ~pti,
                      color = ~col, colors = c(LIGHT_GRAY_BAR, BLACK),
                      alpha = 0.6,
                      name = "",
                      customdata = ~glue(paste(
                          "<b>{Corridor}</b>",
                          "<br>Hour: <b>{format(Hour, '%l:%M %p')}</b>",
                          "<br>Planning Time Index: <b>{data_format(pti)}</b>")),
                      hovertemplate = "%{customdata}",
                      hoverlabel = list(font = list(family = "Source Sans Pro"))
            ) %>%
            layout(xaxis = list(title = glue("{format(month, '%b %Y')} PTI by Hr")),
                   yaxis = list(range = c(hr_min, hr_max),
                                tickformat = tick_format),
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
            highlight(color = travel_time_index$highlight_color,
                      opacityDim = 0.9,
                      defaultValues = c(zone_group),
                      selected = attrs_selected(
                          insidetextfont = list(color = "white"),
                          textposition = "auto", base = 0),
                      on = "plotly_click",
                      off = "plotly_doubleclick")

    } else {
        no_data_plot("")
    }
}



uptime_multiplot <- function(metric, level, zone_group, month) {

    # Plot  Uptime for a Corridor. The basis of subplot.
    uptime_line_plot <- function(df, corr, showlegend_) {
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
    uptime_bar_plot <- function(df, month) {

        df <- df %>%
            arrange(uptime) %>% ungroup() %>%
            mutate(
                col = factor(ifelse(Corridor == zone_group, DARK_GRAY_BAR, LIGHT_GRAY_BAR)),
                text_col = factor(ifelse(Corridor == zone_group, "white", "black")),
                Corridor = factor(Corridor, levels = Corridor))

        plot_ly(data = arrange(df, uptime),
                marker = list(color = ~col)) %>%
            add_bars(
                x = ~uptime,
                y = ~Corridor,
                text = ~as_pct(uptime),
                textposition = "auto",
                insidetextfont = list(color = ~text_col),
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
                xaxis = list(title = glue("{format(month, '%b %Y')} Uptime (%)"),
                             zeroline = FALSE,
                             tickformat = "%"),
                yaxis = list(title = ""),
                showlegend = FALSE,
                font = list(size = 11),
                margin = list(pad = 4,
                              l = 100)
            )
    }

    if (class(month) == "character") {
        month <- as_date(month)
    }

    avg_daily_uptime <- query_data(
        metric, level = level, resolution = "daily", zone_group = zone_group, month = month, upto = TRUE
    )
    avg_monthly_uptime <- query_data(
        metric, level = level, resolution = "monthly", zone_group = zone_group, month = month, upto = FALSE
    )

    if (nrow(avg_daily_uptime) > 0) {
        cdfs <- split(avg_daily_uptime, avg_daily_uptime$Corridor)
        cdfs <- cdfs[lapply(cdfs, nrow)>0]

        p1 <- uptime_bar_plot(avg_monthly_uptime, month)

        plts <- lapply(seq_along(cdfs), function(i) {
            uptime_line_plot(cdfs[[i]], names(cdfs)[i], ifelse(i==1, TRUE, FALSE))
        })
        s2 <- subplot(plts, nrows = min(length(plts), 4), shareX = TRUE, shareY = TRUE, which_layout = 1)
        subplot(p1, s2, titleX = TRUE, widths = c(0.2, 0.8), margin = 0.03) %>%
            layout(margin = list(l = 100))
    } else {
        no_data_plot("")
    }
}



individual_cctvs_plot <- function(zone_group, month) {

    daily_cctv_df <- query_data(
        cctv_uptime, level = "signal", "daily", zone_group = zone_group, month = month
        )

    spr <- daily_cctv_df[daily_cctv_df$Corridor != zone_group,] %>%
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
            customdata = apply(apply(as.matrix(apply(m, 2, status)), 2, bind_description), 1, as.list),
            hovertemplate="<br>%{customdata}<br>%{x}<extra></extra>",
            hoverlabel = list(font = list(family = "Source Sans Pro"))) %>%
        layout(yaxis = list(type = "category",
                            title = ""),
               margin = list(l = 150))
}
