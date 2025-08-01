
# Monthly_Report_Package.R

source("Monthly_Report_Package_init.R")

# options(warn = 2)


# # PACKAGE UP FOR FLEXDASHBOARD ##############################################

print(glue("{Sys.time()} Package for Monthly Report [27 of 29 (mark1)]"))


tryCatch(
    {
        cor <- list()
        cor$dy <- list(
            "du" = readRDS("cor_avg_daily_detector_uptime.rds"),
            "cu" = readRDS("cor_daily_comm_uptime.rds"),
            "pau" = readRDS("cor_daily_pa_uptime.rds"),
            "cctv" = readRDS("cor_daily_cctv_uptime.rds"),
            "ttyp" = readRDS("tasks_by_type.rds")$cor_daily,
            "tsub" = readRDS("tasks_by_subtype.rds")$cor_daily,
            "tpri" = readRDS("tasks_by_priority.rds")$cor_daily,
            "tsou" = readRDS("tasks_by_source.rds")$cor_daily,
            "tasks" = readRDS("tasks_all.rds")$cor_daily,
            "reported" = readRDS("tasks_all.rds")$cor_daily %>%
                transmute(Zone_Group, Corridor, Date, Reported, delta = NA),
            "resolved" = readRDS("tasks_all.rds")$cor_daily %>%
                transmute(Zone_Group, Corridor, Date, Resolved, delta = NA),
            "outstanding" = readRDS("tasks_all.rds")$cor_daily %>%
                transmute(Zone_Group, Corridor, Date, Outstanding, delta = NA)
        )
        cor$wk <- list(
            "vpd" = readRDS("cor_weekly_vpd.rds"),
            # "vph" = readRDS("cor_weekly_vph.rds"),
            "vphpa" = readRDS("cor_weekly_vph_peak.rds")$am,
            "vphpp" = readRDS("cor_weekly_vph_peak.rds")$pm,
            "papd" = readRDS("cor_weekly_papd.rds"),
            # "paph" = readRDS("cor_weekly_paph.rds"),
            "pd" = readRDS("cor_weekly_pd_by_day.rds"),
            "tp" = readRDS("cor_weekly_throughput.rds"),
            "aogd" = readRDS("cor_weekly_aog_by_day.rds"),
            "prd" = readRDS("cor_weekly_pr_by_day.rds"),
            "qsd" = readRDS("cor_wqs.rds"),
            "sfd" = readRDS("cor_wsf.rds"),
            "sfo" = readRDS("cor_wsfo.rds"),
            "du" = readRDS("cor_weekly_detector_uptime.rds"),
            "cu" = readRDS("cor_weekly_comm_uptime.rds"),
            "pau" = readRDS("cor_weekly_pa_uptime.rds"),
            "cctv" = readRDS("cor_weekly_cctv_uptime.rds")
        )
        cor$mo <- list(
            "vpd" = readRDS("cor_monthly_vpd.rds"),
            # "vph" = readRDS("cor_monthly_vph.rds"),
            "vphpa" = readRDS("cor_monthly_vph_peak.rds")$am,
            "vphpp" = readRDS("cor_monthly_vph_peak.rds")$pm,
            "papd" = readRDS("cor_monthly_papd.rds"),
            # "paph" = readRDS("cor_monthly_paph.rds"),
            "pd" = readRDS("cor_monthly_pd_by_day.rds"),
            "tp" = readRDS("cor_monthly_throughput.rds"),
            "aogd" = readRDS("cor_monthly_aog_by_day.rds"),
            "aogh" = readRDS("cor_monthly_aog_by_hr.rds"),
            "prd" = readRDS("cor_monthly_pr_by_day.rds"),
            "prh" = readRDS("cor_monthly_pr_by_hr.rds"),
            "qsd" = readRDS("cor_monthly_qsd.rds"),
            "qsh" = readRDS("cor_mqsh.rds"),
            "sfd" = readRDS("cor_monthly_sfd.rds"),
            "sfh" = readRDS("cor_msfh.rds"),
            "sfo" = readRDS("cor_monthly_sfo.rds"),
            "tti" = readRDS("cor_monthly_tti.rds"),
            "ttih" = readRDS("cor_monthly_tti_by_hr.rds"),
            "pti" = readRDS("cor_monthly_pti.rds"),
            "ptih" = readRDS("cor_monthly_pti_by_hr.rds"),
            "bi" = readRDS("cor_monthly_bi.rds"),
            "bih" = readRDS("cor_monthly_bi_by_hr.rds"),
            "spd" = readRDS("cor_monthly_spd.rds"),
            "spdh" = readRDS("cor_monthly_spd_by_hr.rds"),
            "du" = readRDS("cor_monthly_detector_uptime.rds"),
            "cu" = readRDS("cor_monthly_comm_uptime.rds"),
            "pau" = readRDS("cor_monthly_pa_uptime.rds"),
            "cctv" = readRDS("cor_monthly_cctv_uptime.rds"),
            # "events" = readRDS("cor_monthly_events.rds"),
            "ttyp" = readRDS("tasks_by_type.rds")$cor_monthly,
            "tsub" = readRDS("tasks_by_subtype.rds")$cor_monthly,
            "tpri" = readRDS("tasks_by_priority.rds")$cor_monthly,
            "tsou" = readRDS("tasks_by_source.rds")$cor_monthly,
            "tasks" = readRDS("tasks_all.rds")$cor_monthly,


            "reported" = readRDS("tasks_all.rds")$cor_monthly %>%
                transmute(Zone_Group, Corridor, Month, Reported, delta = delta.rep),
            "resolved" = readRDS("tasks_all.rds")$cor_monthly %>%
                transmute(Zone_Group, Corridor, Month, Resolved, delta = delta.res),
            "outstanding" = readRDS("tasks_all.rds")$cor_monthly %>%
                transmute(Zone_Group, Corridor, Month, Outstanding, delta = delta.out),


            "over45" = readRDS("cor_tasks_by_date.rds") %>%
                transmute(Zone_Group, Corridor, Month, over45, delta = delta.over45),
            "mttr" = readRDS("cor_tasks_by_date.rds") %>%
                transmute(Zone_Group, Corridor, Month, mttr, delta = delta.mttr),
            "hourly_udc" = readRDS("hourly_udc.rds"),
            "udc_trend_table" = readRDS("udc_trend_table_list.rds"),
            "flash" = readRDS("cor_monthly_flash.rds"),
            "bpsi" = readRDS("cor_monthly_bpsi.rds"),
            "rsi" = readRDS("cor_monthly_rsi.rds"),
            "cri" = readRDS("cor_monthly_crash_rate_index.rds"),
            "kabco" = readRDS("cor_monthly_kabco_index.rds")
        )
        cor$qu <- list(
            "vpd" = get_quarterly(cor$mo$vpd, "vpd"),
            # "vph" = data.frame(), # get_quarterly(cor$mo$vph, "vph"),
            "vphpa" = get_quarterly(cor$mo$vphpa, "vph"),
            "vphpp" = get_quarterly(cor$mo$vphpp, "vph"),
            "papd" = get_quarterly(cor$mo$papd, "papd"),
            "pd" = get_quarterly(cor$mo$pd, "pd"),
            "tp" = get_quarterly(cor$mo$tp, "vph"),
            "aogd" = get_quarterly(cor$mo$aogd, "aog", "vol"),
            "prd" = get_quarterly(cor$mo$prd, "pr", "vol"),
            "qsd" = get_quarterly(cor$mo$qsd, "qs_freq"),
            "sfd" = get_quarterly(cor$mo$sfd, "sf_freq"),
            "sfo" = get_quarterly(cor$mo$sfo, "sf_freq"),
            "tti" = get_quarterly(cor$mo$tti, "tti"),
            "pti" = get_quarterly(cor$mo$pti, "pti"),
            "bi" = get_quarterly(cor$mo$bi, "bi"),
            "spd" = get_quarterly(cor$mo$spd, "speed_mph"),
            "du" = get_quarterly(cor$mo$du, "uptime"),
            "cu" = get_quarterly(cor$mo$cu, "uptime"),
            "pau" = get_quarterly(cor$mo$pau, "uptime"),
            "cctv" = get_quarterly(cor$mo$cctv, "uptime", "num"),
            "reported" = get_quarterly(cor$mo$tasks, "Reported"),
            "resolved" = get_quarterly(cor$mo$tasks, "Resolved"),
            "outstanding" = get_quarterly(cor$mo$tasks, "Outstanding", operation = "latest"),
            "over45" = get_quarterly(cor$mo$over45, "over45", operation = "latest"),
            "mttr" = get_quarterly(cor$mo$mttr, "mttr", operation = "latest"),
            "bpsi" = get_quarterly(cor$mo$bpsi, "bpsi"),
            "rsi" = get_quarterly(cor$mo$rsi, "rsi"),
            "cri" = get_quarterly(cor$mo$cri, "cri"),
            "kabco" = get_quarterly(cor$mo$kabco, "kabco")
        )

        for (per in c("mo", "wk")) {
            for (tabl in names(cor[[per]])) {
                if ("data.frame" %in% class(cor[[per]][[tabl]])) {
                    cor[[per]][[tabl]] <- cor[[per]][[tabl]] %>%
                        mutate(Description = Corridor)
                }
            }
        }

        cor$summary_data <- get_corridor_summary_data(cor)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)


tryCatch(
    {
        sub <- list()
        sub$dy <- list(
            "du" = readRDS("sub_avg_daily_detector_uptime.rds") %>%
                select(Zone_Group, Corridor, Date, uptime.sb, uptime.pr, uptime),
            "cu" = readRDS("sub_daily_comm_uptime.rds") %>%
                select(Zone_Group, Corridor, Date, uptime),
            "pau" = readRDS("sub_daily_pa_uptime.rds") %>%
                select(Zone_Group, Corridor, Date, uptime),
            "cctv" = readRDS("sub_daily_cctv_uptime.rds") %>%
                select(Zone_Group, Corridor, Date, uptime)
        )
        sub$wk <- list(
            "vpd" = readRDS("sub_weekly_vpd.rds") %>%
                select(Zone_Group, Corridor, Date, vpd),
            "vphpa" = readRDS("sub_weekly_vph_peak.rds")$am %>%
                select(Zone_Group, Corridor, Date, vph),
            "vphpp" = readRDS("sub_weekly_vph_peak.rds")$pm %>%
                select(Zone_Group, Corridor, Date, vph),
            "papd" = readRDS("sub_weekly_papd.rds") %>%
                select(Zone_Group, Corridor, Date, papd),
            # "paph" = readRDS("sub_weekly_paph.rds"),
            "pd" = readRDS("sub_weekly_pd_by_day.rds") %>%
                select(Zone_Group, Corridor, Date, pd),
            "tp" = readRDS("sub_weekly_throughput.rds") %>%
                select(Zone_Group, Corridor, Date, vph),
            "aogd" = readRDS("sub_weekly_aog_by_day.rds") %>%
                select(Zone_Group, Corridor, Date, aog),
            "prd" = readRDS("sub_weekly_pr_by_day.rds") %>%
                select(Zone_Group, Corridor, Date, pr),
            "qsd" = readRDS("sub_wqs.rds") %>%
                select(Zone_Group, Corridor, Date, qs_freq),
            "sfd" = readRDS("sub_wsf.rds") %>%
                select(Zone_Group, Corridor, Date, sf_freq),
            "sfo" = readRDS("sub_wsfo.rds") %>%
                select(Zone_Group, Corridor, Date, sf_freq),
            "du" = readRDS("sub_weekly_detector_uptime.rds") %>%
                select(Zone_Group, Corridor, Date, uptime),
            "cu" = readRDS("sub_weekly_comm_uptime.rds") %>%
                select(Zone_Group, Corridor, Date, uptime),
            "pau" = readRDS("sub_weekly_pa_uptime.rds") %>%
                select(Zone_Group, Corridor, Date, uptime),
            "cctv" = readRDS("sub_weekly_cctv_uptime.rds") %>%
                select(Zone_Group, Corridor, Date, uptime)
        )
        sub$mo <- list(
            "vpd" = readRDS("sub_monthly_vpd.rds"),
            # "vph" = readRDS("sub_monthly_vph.rds"),
            "vphpa" = readRDS("sub_monthly_vph_peak.rds")$am,
            "vphpp" = readRDS("sub_monthly_vph_peak.rds")$pm,
            "papd" = readRDS("sub_monthly_papd.rds"),
            # "paph" = readRDS("sub_monthly_paph.rds"),
            "pd" = readRDS("sub_monthly_pd_by_day.rds"),
            "tp" = readRDS("sub_monthly_throughput.rds"),
            "aogd" = readRDS("sub_monthly_aog_by_day.rds"),
            "aogh" = readRDS("sub_monthly_aog_by_hr.rds"),
            "prd" = readRDS("sub_monthly_pr_by_day.rds"),
            "prh" = readRDS("sub_monthly_pr_by_hr.rds"),
            "qsd" = readRDS("sub_monthly_qsd.rds"),
            "qsh" = readRDS("sub_mqsh.rds"),
            "sfd" = readRDS("sub_monthly_sfd.rds"),
            "sfo" = readRDS("sub_monthly_sfo.rds"),
            "sfh" = readRDS("sub_msfh.rds"),
            "tti" = readRDS("sub_monthly_tti.rds"),
            "ttih" = readRDS("sub_monthly_tti_by_hr.rds"),
            "pti" = readRDS("sub_monthly_pti.rds"),
            "ptih" = readRDS("sub_monthly_pti_by_hr.rds"),
            "bi" = readRDS("sub_monthly_bi.rds"),
            "bih" = readRDS("sub_monthly_bi_by_hr.rds"),
            "spd" = readRDS("sub_monthly_spd.rds"),
            "spdh" = readRDS("sub_monthly_spd_by_hr.rds"),
            "du" = readRDS("sub_monthly_detector_uptime.rds"),
            "cu" = readRDS("sub_monthly_comm_uptime.rds"),
            "pau" = readRDS("sub_monthly_pa_uptime.rds"),
            "cctv" = readRDS("sub_monthly_cctv_uptime.rds"),
            "flash" = readRDS("sub_monthly_flash.rds"),
            "bpsi" = readRDS("sub_monthly_bpsi.rds"),
            "rsi" = readRDS("sub_monthly_rsi.rds"),
            "cri" = readRDS("sub_monthly_crash_rate_index.rds"),
            "kabco" = readRDS("sub_monthly_kabco_index.rds")
        )
        sub$qu <- list(
            "vpd" = get_quarterly(sub$mo$vpd, "vpd"),
            # "vph" = get_quarterly(sub$mo$vph, "vph"),
            "vphpa" = get_quarterly(sub$mo$vphpa, "vph"),
            "vphpp" = get_quarterly(sub$mo$vphpp, "vph"),
            "tp" = get_quarterly(sub$mo$tp, "vph"),
            "aogd" = get_quarterly(sub$mo$aogd, "aog", "vol"),
            "prd" = get_quarterly(sub$mo$prd, "pr", "vol"),
            "qsd" = get_quarterly(sub$mo$qsd, "qs_freq"),
            "sfd" = get_quarterly(sub$mo$sfd, "sf_freq"),
            "sfo" = get_quarterly(sub$mo$sfo, "sf_freq"),
            "du" = get_quarterly(sub$mo$du, "uptime"),
            "cu" = get_quarterly(sub$mo$cu, "uptime"),
            "pau" = get_quarterly(sub$mo$pau, "uptime"),
            "cctv" = get_quarterly(sub$mo$cctv, "uptime")
        )

        for (per in c("mo", "wk")) {
            for (tabl in names(sub[[per]])) {
                if ("data.frame" %in% class(sub[[per]][[tabl]])) {
                    sub[[per]][[tabl]] <- sub[[per]][[tabl]] %>%
                        mutate(Description = Corridor)
                }
            }
        }
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)



tryCatch(
    {
        sig <- list()
        sig$dy <- list(
            "du" = sigify(readRDS("avg_daily_detector_uptime.rds"), cor$dy$du, corridors) %>%
                select(Zone_Group, Corridor, Description, Date, uptime, uptime.sb, uptime.pr),
            "cu" = sigify(readRDS("daily_comm_uptime.rds"), cor$dy$cu, corridors) %>%
                select(Zone_Group, Corridor, Description, Date, uptime),
            "pau" = sigify(readRDS("daily_pa_uptime.rds"), cor$dy$pau, corridors) %>%
                select(Zone_Group, Corridor, Description, Date, uptime),
            "cctv" = sigify(readRDS("daily_cctv_uptime.rds"), cor$dy$cctv, cam_config, identifier = "CameraID") %>%
                select(Zone_Group, Corridor, Description, Date, uptime, up)
        )
        sig$wk <- list(
            "vpd" = sigify(readRDS("weekly_vpd.rds"), cor$wk$vpd, corridors) %>%
                select(Zone_Group, Corridor, Description, Date, vpd),
            "vphpa" = sigify(readRDS("weekly_vph_peak.rds")$am, cor$wk$vphpa, corridors) %>%
                select(Zone_Group, Corridor, Description, Date, vph),
            "vphpp" = sigify(readRDS("weekly_vph_peak.rds")$pm, cor$wk$vphpp, corridors) %>%
                select(Zone_Group, Corridor, Description, Date, vph),
            "papd" = sigify(readRDS("weekly_papd.rds"), cor$wk$papd, corridors) %>%
                select(Zone_Group, Corridor, Description, Date, papd),
            # "paph" = sigify(readRDS("weekly_paph.rds"), cor$wk$paph, corridors),
            "pd" = sigify(readRDS("weekly_pd_by_day.rds"), cor$wk$pd, corridors) %>%
                select(Zone_Group, Corridor, Description, Date, pd),
            "tp" = sigify(readRDS("weekly_throughput.rds"), cor$wk$tp, corridors) %>%
                select(Zone_Group, Corridor, Description, Date, vph),
            "aogd" = sigify(readRDS("weekly_aog_by_day.rds"), cor$wk$aogd, corridors) %>%
                select(Zone_Group, Corridor, Description, Date, aog),
            "prd" = sigify(readRDS("weekly_pr_by_day.rds"), cor$wk$prd, corridors) %>%
                select(Zone_Group, Corridor, Description, Date, pr),
            "qsd" = sigify(readRDS("wqs.rds"), cor$wk$qsd, corridors) %>%
                select(Zone_Group, Corridor, Description, Date, qs_freq),
            "sfd" = sigify(readRDS("wsf.rds"), cor$wk$sfd, corridors) %>%
                select(Zone_Group, Corridor, Description, Date, sf_freq),
            "sfo" = sigify(readRDS("wsfo.rds"), cor$wk$sfo, corridors) %>%
                select(Zone_Group, Corridor, Description, Date, sf_freq),
            "du" = sigify(readRDS("weekly_detector_uptime.rds"), cor$wk$du, corridors) %>%
                select(Zone_Group, Corridor, Description, Date, uptime),
            "cu" = sigify(readRDS("weekly_comm_uptime.rds"), cor$wk$cu, corridors) %>%
                select(Zone_Group, Corridor, Description, Date, uptime),
            "pau" = sigify(readRDS("weekly_pa_uptime.rds"), cor$wk$pau, corridors) %>%
                select(Zone_Group, Corridor, Description, Date, uptime),
            "cctv" = sigify(readRDS("weekly_cctv_uptime.rds"), cor$wk$cctv, cam_config, identifier = "CameraID") %>%
                select(Zone_Group, Corridor, Description, Date, uptime)
        )
        sig$mo <- list(
            "vpd" = sigify(readRDS("monthly_vpd.rds"), cor$mo$vpd, corridors) %>%
                select(-c(Name, ones)),
            "vphpa" = sigify(readRDS("monthly_vph_peak.rds")$am, cor$mo$vphpa, corridors) %>%
                select(-c(Name, ones)),
            "vphpp" = sigify(readRDS("monthly_vph_peak.rds")$pm, cor$mo$vphpp, corridors) %>%
                select(-c(Name, ones)),
            "papd" = sigify(readRDS("monthly_papd.rds"), cor$mo$papd, corridors) %>%
                select(-c(Name, ones)),
            # "paph" = sigify(readRDS("monthly_paph.rds"), cor$mo$paph, corridors) %>%
            #    select(-c(Name, ones)),
            "pd" = sigify(readRDS("monthly_pd_by_day.rds"), cor$mo$pd, corridors) %>%
                select(-c(Name, Events)),
            "tp" = sigify(readRDS("monthly_throughput.rds"), cor$mo$tp, corridors) %>%
                select(-c(Name, ones)),
            "aogd" = sigify(readRDS("monthly_aog_by_day.rds"), cor$mo$aogd, corridors) %>%
                select(-c(Name, vol)),
            "aogh" = sigify(readRDS("monthly_aog_by_hr.rds"), cor$mo$aogh, corridors) %>%
                select(-c(Name, vol)),
            "prd" = sigify(readRDS("monthly_pr_by_day.rds"), cor$mo$prd, corridors) %>%
                select(-c(Name, vol)),
            "prh" = sigify(readRDS("monthly_pr_by_hr.rds"), cor$mo$prh, corridors) %>%
                select(-c(Name, vol)),
            "qsd" = sigify(readRDS("monthly_qsd.rds"), cor$mo$qsd, corridors) %>%
                select(-c(Name, cycles)),
            "qsh" = sigify(readRDS("mqsh.rds"), cor$mo$qsh, corridors) %>%
                select(-c(Name, cycles)),
            "sfd" = sigify(readRDS("monthly_sfd.rds"), cor$mo$sfd, corridors) %>%
                select(-c(Name, cycles)),
            "sfh" = sigify(readRDS("msfh.rds"), cor$mo$sfh, corridors) %>%
                select(-c(Name, cycles)),
            "sfo" = sigify(readRDS("monthly_sfo.rds"), cor$mo$sfo, corridors) %>%
                select(-c(Name, cycles)),
            "tti" = data.frame(),
            "pti" = data.frame(),
            "bi" = data.frame(),
            "spd" = data.frame(),
            "du" = sigify(readRDS("monthly_detector_uptime.rds"), cor$mo$du, corridors) %>%
                select(Zone_Group, Corridor, Month, uptime, uptime.sb, uptime.pr, delta),
            "cu" = sigify(readRDS("monthly_comm_uptime.rds"), cor$mo$cu, corridors) %>%
                select(Zone_Group, Corridor, Month, uptime, delta),
            "pau" = sigify(readRDS("monthly_pa_uptime.rds"), cor$mo$pau, corridors) %>%
                select(Zone_Group, Corridor, Month, uptime, delta),
            "cctv" = sigify(readRDS("monthly_cctv_uptime.rds"), cor$mo$cctv, cam_config, identifier = "CameraID") %>%
                select(Zone_Group, Corridor, Description, Month, uptime, delta),
            "ttyp" = readRDS("tasks_by_type.rds")$sig_monthly,
            "tsub" = readRDS("tasks_by_subtype.rds")$sig_monthly,
            "tpri" = readRDS("tasks_by_priority.rds")$sig_monthly,
            "tsou" = readRDS("tasks_by_source.rds")$sig_monthly,
            "tasks" = readRDS("tasks_all.rds")$sig_monthly,
            "reported" = readRDS("tasks_all.rds")$sig_monthly %>%
                transmute(Zone_Group, Corridor, Month, Reported, delta = delta.rep),
            "resolved" = readRDS("tasks_all.rds")$sig_monthly %>%
                transmute(Zone_Group, Corridor, Month, Resolved, delta = delta.res),
            "outstanding" = readRDS("tasks_all.rds")$sig_monthly %>%
                transmute(Zone_Group, Corridor, Month, Outstanding, delta = delta.out),
            "over45" = readRDS("sig_tasks_by_date.rds") %>%
                transmute(Zone_Group, Corridor, Month, over45, delta = delta.over45),
            "mttr" = readRDS("sig_tasks_by_date.rds") %>%
                transmute(Zone_Group, Corridor, Month, mttr, delta = delta.mttr),
            "flash" = sigify(readRDS("monthly_flash.rds"), cor$mo$flash, corridors) %>%
                select(-c(Name, ones)),
            "cri" = sigify(readRDS("monthly_crash_rate_index.rds"), cor$mo$cri, corridors) %>%
                select(Zone_Group, Corridor, Month, cri, delta),
            "kabco" = sigify(readRDS("monthly_kabco_index.rds"), cor$mo$kabco, corridors) %>%
                select(Zone_Group, Corridor, Month, kabco, delta)
        )
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)



source("Health_Metrics.R")



print(glue("{Sys.time()} Upload to AWS [28 of 29 (mark1)]"))


qsave(cor, "cor.qs")
qsave(sig, "sig.qs")
qsave(sub, "sub.qs")

aws.s3::put_object(
    file = "cor.qs",
    object = "cor_ec2.qs",
    bucket = conf$bucket,
    multipart = TRUE
)
aws.s3::put_object(
    file = "sig.qs",
    object = "sig_ec2.qs",
    bucket = conf$bucket,
    multipart = TRUE
)
aws.s3::put_object(
    file = "sub.qs",
    object = "sub_ec2.qs",
    bucket = conf$bucket,
    multipart = TRUE
)


print(glue("{Sys.time()} Write to Database [29 of 29 (mark1)]"))

source("write_sigops_to_db.R")

# Update Aurora Nightly
conn <- keep_trying(func = get_aurora_connection, n_tries = 6, sleep = 10)
print("Database connection created.")
# recreate_database(conn)

tryCatch({
    append_to_database(
        conn, cor, "cor",
        calcs_start_date, report_start_date, report_end_date = NULL)
    append_to_database(
        conn, sub, "sub",
        calcs_start_date, report_start_date, report_end_date = NULL)
    append_to_database(
        conn, sig, "sig",
        calcs_start_date, report_start_date, report_end_date = NULL)
},
finally = {
    dbDisconnect(conn)
})
closeAllConnections()
