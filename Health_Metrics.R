# Corridor Health Metrics Functions

suppressMessages({
    library(shiny)
    library(dplyr)
    library(tidyr)
    library(DT)
    library(aws.s3)
    library(lubridate)
    library(purrr)
    library(stringr)
    library(formattable)
    library(data.table)
    library(DT)
    library(plotly)
    library(readr)
    library(yaml)
    library(qs)
})



# USER-DEFINED CONSTANTS AND LOOKUPS

health_conf <- read_yaml("health_metrics.yaml")

scoring_lookup <- health_conf$scoring_lookup %>%
    as.data.frame() %>%
    as.data.table()
weights_lookup <- health_conf$weights_lookup %>%
    as.data.frame() %>%
    as.data.table()



# USER-DEFINED FUNCTIONS

# function to commpile summary data from the sub/cor/sig datasets
get_summary_data <- function(df, current_month = NULL) {
    #' Converts sub or cor data set to a single data frame for the current_month
    #' for use in get_subcorridor_summary_table function
    #'
    #' @param df cor or sub data
    #' @param current_month Current month in date format
    #' @return A data frame, monthly data of all metrics by Zone and Corridor
    # current_month <- months[order(months)][match(current_month, months.formatted)]

    # Cleaner Version (mostly) for future use more broadly on the site.
    # Still have the issue of certain metrics not applying at certain levels,
    # e.g., tti in sig data frame has no meaning since it's a corridor, not a signal-level metric
    #
    # data <- list(
    #     select(df$mo[["du"]], Zone_Group, Corridor, Month, du = uptime, du.delta = delta),
    #     select(df$mo[["cu"]], Zone_Group, Corridor, Month, cu = uptime, cu.delta = delta),
    #     select(df$mo[["pau"]], Zone_Group, Corridor, Month, pau = uptime, pau.delta = delta),
    #     select(df$mo[["cctv"]], Zone_Group, Corridor, Month, cctv = uptime, cctv.delta = delta),
    #     select(df$mo[["ru"]], Zone_Group, Corridor, Month, ru = uptime, ru.delta = delta),
    #
    #     select(df$mo[["vpd"]], Zone_Group, Corridor, Month, vpd, vpd.delta = delta),
    #     select(df$mo[["papd"]], Zone_Group, Corridor, Month, papd, papd.delta = delta),
    #     select(df$mo[["pd"]], Zone_Group, Corridor, Month, pd, pd.delta = delta),
    #     select(df$mo[["tp"]], Zone_Group, Corridor, Month, vph, vph.delta = delta),
    #     select(df$mo[["aogd"]], Zone_Group, Corridor, Month, aog, aog.delta = delta),
    #     select(df$mo[["pr"]], Zone_Group, Corridor, Month, pr, pr.delta = delta),
    #     select(df$mo[["qsd"]], Zone_Group, Corridor, Month, qs, qs.delta = delta),
    #     select(df$mo[["sfd"]], Zone_Group, Corridor, Month, sfd, sfd.delta = delta),
    #     select(df$mo[["sfo"]], Zone_Group, Corridor, Month, sfo, sfo.delta = delta),
    #     select(df$mo[["pd"]], Zone_Group, Corridor, Month, pd, pd.delta = delta),
    #     select(df$mo[["flash"]], Zone_Group, Corridor, Month, flash_events = flash, flash.delta = delta),
    # ifelse(nrow(df$mo$tti) > 0,
    #        select(
    #            df$mo[["tti"]], Zone_Group, Corridor, Month, tti, tti.delta = delta),
    #        data.frame(
    #            Corridor = factor(), Zone_Group = integer(), Month = as_date(POSIXct()), tti = double(), tti.delta = double())
    # )
    # ifelse(nrow(df$mo$pti) > 0,
    #        select(
    #            df$mo[["pti"]], Zone_Group, Corridor, Month, pti, pti.delta = delta),
    #        data.frame(
    #            Corridor = factor(), Zone_Group = integer(), Month = as_date(POSIXct()), pti = double(), pti.delta = double())
    # )
    #
    #
    # ) %>%
    #     reduce(full_join, by = c("Zone_Group", "Corridor", "Month"))



    data <- list(
        rename(df$mo$du, du = uptime, du.delta = delta),
        rename(df$mo$pau, pau = uptime, pau.delta = delta),
        rename(df$mo$cctv, cctv = uptime, cctv.delta = delta),
        #rename(df$mo$ru, rsu = uptime, rsu.delta = delta),
        rename(df$mo$cu, cu = uptime, cu.delta = delta),
        rename(df$mo$tp, tp.delta = delta),
        rename(df$mo$aogd, aog.delta = delta),
        rename(df$mo$prd, pr.delta = delta),
        rename(df$mo$qsd, qs.delta = delta),
        rename(df$mo$sfd, sf.delta = delta),
        rename(df$mo$pd, pd.delta = delta) %>% relocate(pd.delta, .after = pd),
        rename(df$mo$flash, flash_events = flash, flash.delta = delta),
        if (nrow(df$mo$tti) > 0) {
            rename(df$mo$tti, tti.delta = delta)
        } else { # if dealing with signals data - copy another df, rename, and set columns to NA
            rename(df$mo$vpd[0,], tti = vpd, tti.delta = delta)
        },
        if (nrow(df$mo$pti) > 0) {
            rename(df$mo$pti, pti.delta = delta)
        } else { # if dealing with signals data - copy another df, rename, and set columns to NA
            rename(df$mo$vpd[0,], pti = vpd, pti.delta = delta)
        },
        # safety metrics - new 3/17/21
        rename(df$mo$kabco, kabco.delta = delta),
        rename(df$mo$cri, cri.delta = delta),
        if (!is.null(df$mo$rsi)) {
            rename(df$mo$rsi, rsi.delta = delta)
        } else { # if dealing with signals data - copy another df, rename, and set columns to NA
            rename(df$mo$vpd[0,], rsi = vpd, rsi.delta = delta)
        },
        if (!is.null(df$mo$bpsi)) {
            rename(df$mo$bpsi, bpsi.delta = delta)
        } else { # if dealing with signals data - copy another df, rename, and set columns to NA
            rename(df$mo$vpd[0,], bpsi = vpd, bpsi.delta = delta)
        }
    ) %>%
        reduce(full_join, by = c("Zone_Group", "Corridor", "Month"), relationship = "many-to-many") %>%
        filter(
            as.character(Zone_Group) != as.character(Corridor)
        ) %>%
        select(
            -uptime.sb,
            -uptime.pr,
            #-Duration,
            # -Events,
            #-num,
            -starts_with("Description"),
            -starts_with("ones"),
            -starts_with("cycles"),
            -starts_with("pct"),
            -starts_with("vol"),
            -starts_with("num"),
        ) %>%
	distinct()

    if ("mttr" %in% names(df$mo)) { # cor, not sub
        data <- data %>%
            arrange(Month, Zone_Group, Corridor)
    } else {
        data <- data %>%
            rename(
                Subcorridor = Corridor,
                Corridor = Zone_Group
            ) %>%
            arrange(Month, Corridor, Subcorridor)
    }
    data <- data[, c(2, 1, 3:length(data))] # reorder Subcorridor, Corridor
    if (!is.null(current_month)) {
        data <- data %>% filter(Month == ymd(current_month))
    }
    return(data)
}



# function to compute scores/weights at sub/sig level
get_health_all <- function(df) {
    df <- df %>%
        select(
            Zone_Group, Zone, Corridor, Subcorridor, Month, Context,

            # maintenance
            Detection_Uptime = du,
            Ped_Act_Uptime = pau,
            Comm_Uptime = cu,
            CCTV_Uptime = cctv,
            #RSU_Uptime = rsu,
            Flash_Events = flash_events,

            # safety
            Platoon_Ratio = pr,
            Ped_Delay = pd,
            Split_Failures = sf_freq,
            TTI = tti,
            pti,

            ## safety - updated 3/17/21
            Crash_Rate_Index = cri,
            KABCO_Crash_Severity_Index = kabco,
            High_Speed_Index = rsi,
            Ped_Injury_Exposure_Index = bpsi
        ) %>%
        mutate( # scores for each metric
            # cleanup
            Flash_Events = ifelse(is.na(Flash_Events), 0, Flash_Events), # assuming we want this
            BI = pti - TTI,
            pti = NULL,
            Crash_Rate_Index = ifelse(is.infinite(Crash_Rate_Index), NA, Crash_Rate_Index),
            KABCO_Crash_Severity_Index = ifelse(is.infinite(KABCO_Crash_Severity_Index), NA, KABCO_Crash_Severity_Index),

            # maintenance
            # backward: set roll to -Inf (higher value => higher score)
            Detection_Uptime_Score = get_lookup_value(
                scoring_lookup, "detection", "score", Detection_Uptime, "backward"),
            Ped_Act_Uptime_Score = get_lookup_value(
                scoring_lookup, "ped_actuation", "score", Ped_Act_Uptime, "backward"),
            Comm_Uptime_Score = get_lookup_value(
                scoring_lookup, "comm", "score", Comm_Uptime, "backward"),
            CCTV_Uptime_Score = get_lookup_value(
                scoring_lookup, "cctv", "score", CCTV_Uptime, "backward"),
            #RSU_Uptime_Score = get_lookup_value(
            #    scoring_lookup, "rsu", "score", RSU_Uptime, "backward"),
            Flash_Events_Score = get_lookup_value(
                scoring_lookup, "flash_events", "score", Flash_Events),

            # operations
            Platoon_Ratio_Score = get_lookup_value(
                scoring_lookup, "pr", "score", Platoon_Ratio),
            Ped_Delay_Score = get_lookup_value(
                scoring_lookup, "ped_delay", "score", Ped_Delay),
            Split_Failures_Score = get_lookup_value
            (scoring_lookup, "sf", "score", Split_Failures),
            TTI_Score = get_lookup_value(
                scoring_lookup, "tti", "score", TTI),
            BI_Score = get_lookup_value(
                scoring_lookup, "bi", "score", BI),

            ## safety
            Crash_Rate_Index_Score = get_lookup_value(
                scoring_lookup, "crash_rate", "score", Crash_Rate_Index),
            KABCO_Crash_Severity_Index_Score = get_lookup_value(
                scoring_lookup, "kabco", "score", KABCO_Crash_Severity_Index),
            High_Speed_Index_Score = get_lookup_value(
                scoring_lookup, "high_speed", "score", High_Speed_Index),
            Ped_Injury_Exposure_Index_Score = get_lookup_value(
                scoring_lookup, "ped_injury_exposure", "score", Ped_Injury_Exposure_Index)

        ) %>%
        inner_join(weights_lookup, by = c("Context"))

    # Maintenance
    df$Detection_Uptime_Weight[is.na(df$Detection_Uptime_Score)] <- NA
    df$Ped_Act_Uptime_Weight[is.na(df$Ped_Act_Uptime_Score)] <- NA
    df$Comm_Uptime_Weight[is.na(df$Comm_Uptime_Score)] <- NA
    df$CCTV_Uptime_Weight[is.na(df$CCTV_Uptime_Score)] <- NA
    #df$RSU_Uptime_Weight[is.na(df$RSU_Uptime_Score)] <- NA
    df$Flash_Events_Weight[is.na(df$Flash_Events_Score)] <- NA

    # operations
    df$Platoon_Ratio_Weight[is.na(df$Platoon_Ratio_Score)] <- NA
    df$Ped_Delay_Weight[is.na(df$Ped_Delay_Score)] <- NA
    df$Split_Failures_Weight[is.na(df$Split_Failures_Score)] <- NA
    df$TTI_Weight[is.na(df$TTI_Score)] <- NA
    df$BI_Weight[is.na(df$BI_Score)] <- NA

    # Safety
    df$Crash_Rate_Index_Weight[is.na(df$Crash_Rate_Index_Score)] <- NA
    df$KABCO_Crash_Severity_Index_Weight[is.na(df$KABCO_Crash_Severity_Index_Score)] <- NA
    df$High_Speed_Index_Weight[is.na(df$High_Speed_Index_Score)] <- NA
    df$Ped_Injury_Exposure_Index_Weight[is.na(df$Ped_Injury_Exposure_Index_Score)] <- NA

    df
}



# function to product maintenance % health data frame
get_health_maintenance <- function(df) {
    health <- df %>%
        select(
            Zone_Group, Zone, Corridor, Subcorridor, Month, Context, Context_Category,
            #starts_with(c("Detection", "Ped_Act", "Comm", "CCTV", "RSU", "Flash_Events")))
            starts_with(c("Detection", "Ped_Act", "Comm", "CCTV", "Flash_Events")))


    # filter out scores and weights and make sure they're both in the same sort order
    scores <- health %>%
        select(ends_with("Score")) %>%
        select(sort(tidyselect::peek_vars()))
    weights <- health %>%
        select(ends_with("Weight")) %>%
        select(sort(tidyselect::peek_vars()))

    health$Percent_Health <- rowSums(scores * weights, na.rm = T)/rowSums(weights, na.rm = T)/10
    health$Missing_Data <- 1 - rowSums(weights, na.rm = T)/100

    health <- health %>%
        group_by(Zone_Group, Zone, Corridor, Subcorridor, Month) %>%
        mutate(
            Percent_Health = mean(Percent_Health, na.rm = T),
            Missing_Data = mean(Missing_Data, na.rm = T)) %>%
        ungroup() %>%


        get_percent_health_subtotals() %>%
        select(
            `Zone Group` = Zone_Group,
            Zone,
            Corridor,
            Subcorridor,
            Month,
            `Context` = Context_Category,
            `Percent Health` = Percent_Health,
            `Missing Data` = Missing_Data,
            `Detection Uptime Score` = Detection_Uptime_Score,
            `Ped Actuation Uptime Score` = Ped_Act_Uptime_Score,
            `Comm Uptime Score` = Comm_Uptime_Score,
            `CCTV Uptime Score` = CCTV_Uptime_Score,
            #`RSU Uptime Score` = RSU_Uptime_Score,
            `Flash Events Score` = Flash_Events_Score,
            `Detection Uptime` = Detection_Uptime,
            `Ped Actuation Uptime` = Ped_Act_Uptime,
            `Comm Uptime` = Comm_Uptime,
            `CCTV Uptime` = CCTV_Uptime,
            #`RSU Uptime` = RSU_Uptime,
            `Flash Events` = Flash_Events
        )
}



# function to product safety % health data frame
get_health_operations <- function(df) {
    health <- df %>%
        select(
            Zone_Group, Zone, Corridor, Subcorridor, Month, Context, Context_Category,
            starts_with(c("Platoon_Ratio", "Ped_Delay", "Split_Failures", "TTI", "BI")))


    # filter out scores and weights and make sure they're both in the same sort order
    scores <- health %>%
        select(ends_with("Score")) %>%
        select(sort(tidyselect::peek_vars()))
    weights <- health %>%
        select(ends_with("Weight")) %>%
        select(sort(tidyselect::peek_vars()))

    health$Percent_Health <- rowSums(scores * weights, na.rm = T)/rowSums(weights, na.rm = T)/10
    health$Missing_Data <- 1 - rowSums(weights, na.rm = T)/100

    health <- health %>%
        group_by(Zone_Group, Zone, Corridor, Subcorridor, Month) %>%
        mutate(
            Percent_Health = mean(Percent_Health, na.rm = T),
            Missing_Data = mean(Missing_Data, na.rm = T)) %>%
        ungroup() %>%


        get_percent_health_subtotals() %>%
        select(
            `Zone Group` = Zone_Group,
            Zone,
            Corridor,
            Subcorridor,
            Month,
            `Context` = Context_Category,
            `Percent Health` = Percent_Health,
            `Missing Data` = Missing_Data,
            `Platoon Ratio Score` = Platoon_Ratio_Score,
            `Ped Delay Score` = Ped_Delay_Score,
            `Split Failures Score` = Split_Failures_Score,
            `Travel Time Index Score` = TTI_Score,
            `Buffer Index Score` = BI_Score,
            `Platoon Ratio` = Platoon_Ratio,
            `Ped Delay` = Ped_Delay,
            `Split Failures` = Split_Failures,
            `Travel Time Index` = TTI,
            `Buffer Index` = BI
        )
}



# function to product safety % health data frame
get_health_safety <- function(df) {
    health <- df %>%
        select(
            Zone_Group, Zone, Corridor, Subcorridor, Month, Context, Context_Category,
            starts_with(c("Crash_Rate", "KABCO", "High_Speed", "Ped_Injury")))


    # filter out scores and weights and make sure they're both in the same sort order
    scores <- health %>%
        select(ends_with("Score")) %>%
        select(sort(tidyselect::peek_vars()))
    weights <- health %>%
        select(ends_with("Weight")) %>%
        select(sort(tidyselect::peek_vars()))

    health$Percent_Health <- rowSums(scores * weights, na.rm = T)/rowSums(weights, na.rm = T)/10
    health$Missing_Data <- 1 - rowSums(weights, na.rm = T)/100

    health <- health %>%
        group_by(Zone_Group, Zone, Corridor, Subcorridor, Month) %>%
        mutate(
            Percent_Health = mean(Percent_Health, na.rm = T),
            Missing_Data = mean(Missing_Data, na.rm = T)) %>%
        ungroup() %>%

        get_percent_health_subtotals() %>%
        select(
            `Zone Group` = Zone_Group,
            Zone,
            Corridor,
            Subcorridor,
            Month,
            `Context` = Context_Category,
            `Percent Health` = Percent_Health,
            `Missing Data` = Missing_Data,
            #scores
            `Crash Rate Index Score` = Crash_Rate_Index_Score,
            `KABCO Crash Severity Index Score` = KABCO_Crash_Severity_Index_Score,
            `High Speed Index Score` = High_Speed_Index_Score,
            `Ped Injury Exposure Index Score` = Ped_Injury_Exposure_Index_Score,
            #metrics
            `Crash Rate Index` = Crash_Rate_Index,
            `KABCO Crash Severity Index` = KABCO_Crash_Severity_Index,
            `High Speed Index` = High_Speed_Index,
            `Ped Injury Exposure Index` = Ped_Injury_Exposure_Index,
        )
}



# function used to look up health score for various metric
# data.table approach - update so last field is up/down
get_lookup_value <- function(dt, lookup_col, lookup_val, x, direction = "forward") {
    setkeyv(dt, lookup_col)
    cols <- c(lookup_val, lookup_col)
    dt <- dt[, ..cols]
    x <- data.table(x)
    # setkeyv(x, "x") #this re-orders x
    val <- dt[x, roll = ifelse(direction == "forward", Inf, -Inf)][, ..lookup_val]
    val %>%
        as_vector() %>%
        unname() # return as vector, not tibble, to avoid replacing column names in DF
}



# function that computes % health subtotals for corridor and zone level
get_percent_health_subtotals <- function(df) {
    corridor_subtotals <- df %>%
        group_by(Zone_Group, Zone, Corridor, Month) %>%
        summarise(Percent_Health = mean(Percent_Health, na.rm = TRUE), .groups = "drop")
    zone_subtotals <- corridor_subtotals %>%
        group_by(Zone_Group, Zone, Month) %>%
        summarise(Percent_Health = mean(Percent_Health, na.rm = TRUE), .groups = "drop")

    bind_rows(df, corridor_subtotals, zone_subtotals) %>%
        group_by(Zone, Month) %>%
        tidyr::fill(Zone_Group) %>%
        group_by(Corridor) %>%
        tidyr::fill(Context_Category) %>%
        ungroup() %>%
        mutate(
            Zone_Group = factor(Zone_Group),
            Zone = factor(Zone)
        ) %>%
        arrange(Zone_Group, Zone, Corridor, Subcorridor, Month) %>%
        mutate(
            Corridor = factor(coalesce(Corridor, Zone)),
            Subcorridor = factor(coalesce(Subcorridor, Corridor))
        )
}



get_health_metrics_plot_df <- function(df) {
    df %>%
        select(
            Zone_Group = Corridor,
            Corridor = Subcorridor,
            #Description = Subcorridor,
            everything()) %>%
        select(-`Zone Group`, -Zone) %>%
        mutate(
            Zone_Group = factor(Zone_Group),
            Corridor = factor(Corridor)
            #Description = "Desc"
        ) %>%
        arrange(Zone_Group, Corridor, Month)
}



get_cor_health_metrics_plot_df <- function(df) {
    df %>%
        select(-`Zone Group`) %>%
        rename(Zone_Group = Zone) %>%
        mutate(
            Zone_Group = factor(Zone_Group),
            Corridor = factor(Corridor)
            #Description = "Desc"
        ) %>%
        arrange(Zone_Group, Corridor, Month)
}



add_subcorridor <- function(df)  {
    df %>%
        rename(SignalID = Subcorridor) %>%
        left_join(
            select(corridors, Zone, Corridor, Subcorridor, SignalID),
            by = c("Zone", "Corridor", "SignalID"),
            relationship = "many-to-many") %>%
        relocate(Subcorridor, .after = Corridor) %>%
        filter(!is.na(Subcorridor))
}



corridor_groupings <- read_excel(
    conf$corridors_filename_s3,
    sheet = "Contexts",
    range = cell_cols("A:F")
    ) %>%
    mutate(Context = as.integer(Context))


csd <- get_summary_data(sub)
ssd <- get_summary_data(sig) %>%
    # Give each CameraID its associated SignalID and combine with other metrics on SignalID
    left_join(
        select(cam_config, SignalID, CameraID),
        by = c("Corridor" = "CameraID"),
        relationship = "many-to-many") %>%
    mutate(Corridor = as.factor(coalesce(as.character(SignalID), as.character(Corridor)))) %>%
    select(-SignalID) %>%
    filter(!grepl("CAM", Corridor)) %>%
    group_by(Corridor, Zone_Group, Month) %>%
    fill(everything(), .direction = "updown") %>%
    slice_head(n = 1) %>%
    ungroup()%>%
    mutate(across(where(is.numeric), ~replace(.x, is.infinite(.x), NA)))


# input data frame for subcorridor health metrics
sub_health_data <- csd %>%
    inner_join(corridor_groupings, by = c("Corridor", "Subcorridor"), relationship = "many-to-many")

# input data frame for signal health metrics
sig_health_data <- ssd %>%
    rename(
        SignalID = Corridor,
        Corridor = Zone_Group
    ) %>%
    left_join(
        select(corridors, Corridor, SignalID, Subcorridor),
        by = c("Corridor", "SignalID"), relationship = "many-to-many") %>%
    inner_join(corridor_groupings, by = c("Corridor", "Subcorridor"), relationship = "many-to-many") %>%
    #hack to bring in TTI/PTI from corresponding subcorridors for each signal - only applies if context in 1-4
    left_join(
        select(sub_health_data, Corridor, Subcorridor, Month, tti, pti),
        by = c("Corridor", "Subcorridor", "Month"), relationship = "many-to-many") %>%
    mutate(
        tti.x = ifelse(Context %in% c(1:4), tti.y, NA),
        pti.x = ifelse(Context %in% c(1:4), pti.y, NA)
    ) %>%
    select(-Subcorridor, -tti.y, -pti.y) %>% # workaround - drop subcorridor column and replace with signalID
    rename(Subcorridor = SignalID, tti = tti.x, pti = pti.x)

# GENERATE COMPILED TABLES FOR WEBPAGE

## all data for all 3 health metrics - think this should be run in the background and cached?
# compile all data needed for health metrics calcs - does not calculate % health yet
health_all_sub <- get_health_all(sub_health_data)
health_all_sig <- get_health_all(sig_health_data)

# compile maintenance % health
maintenance_sub <- get_health_maintenance(health_all_sub)
maintenance_sig <- get_health_maintenance(health_all_sig)

maintenance_cor <- maintenance_sub %>%
    select(-c(Subcorridor, Context)) %>%
    group_by(`Zone Group`, Zone, Corridor, Month) %>%
    summarize(across(everything(), function(x) mean(x, na.rm = T)), .groups = "drop")

# compile operations % health
operations_sub <- get_health_operations(health_all_sub)
operations_sig <- get_health_operations(health_all_sig)

# operations_cor <- get_health_operations(health_all_cor)
operations_cor <- operations_sub %>%
    select(-c(Subcorridor, Context)) %>%
    group_by(`Zone Group`, Zone, Corridor, Month) %>%
    summarize(across(everything(), function(x) mean(x, na.rm = T)), .groups = "drop")

## compile safety % health
safety_sub <- get_health_safety(health_all_sub)
safety_sig <- get_health_safety(health_all_sig)

safety_cor <- safety_sub %>%
    select(-c(Subcorridor, Context)) %>%
    group_by(`Zone Group`, Zone, Corridor, Month) %>%
    summarize(across(everything(), function(x) mean(x, na.rm = T)), .groups = "drop")


cor$mo$maint_plot <- get_cor_health_metrics_plot_df(maintenance_cor)
sub$mo$maint_plot <- get_health_metrics_plot_df(maintenance_sub)
sig$mo$maint_plot <- get_health_metrics_plot_df(maintenance_sig)

cor$mo$ops_plot <- get_cor_health_metrics_plot_df(operations_cor)
sub$mo$ops_plot <- get_health_metrics_plot_df(operations_sub)
sig$mo$ops_plot <- get_health_metrics_plot_df(operations_sig)

cor$mo$safety_plot <- get_cor_health_metrics_plot_df(safety_cor)
sub$mo$safety_plot <- get_health_metrics_plot_df(safety_sub)
sig$mo$safety_plot <- get_health_metrics_plot_df(safety_sig)

cor$mo$maint <- maintenance_cor
sub$mo$maint <- maintenance_sub
sig$mo$maint <- maintenance_sig %>% add_subcorridor()

cor$mo$ops <- operations_cor
sub$mo$ops <- operations_sub
sig$mo$ops <- operations_sig %>% add_subcorridor()

cor$mo$safety <- safety_cor
sub$mo$safety <- safety_sub
sig$mo$safety <- safety_sig %>% add_subcorridor()

