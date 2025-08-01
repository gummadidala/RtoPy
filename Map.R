
points_to_line <- function(data, long, lat, id_field = NULL, sort_field = NULL) {

    # Convert to SpatialPointsDataFrame
    coordinates(data) <- c(long, lat)

    # If there is a sort field...
    if (!is.null(sort_field)) {
        if (!is.null(id_field)) {
            data <- data[order(data[[id_field]], data[[sort_field]]), ]
        } else {
            data <- data[order(data[[sort_field]]), ]
        }
    }

    # If there is only one path...
    if (is.null(id_field)) {

        lines <- SpatialLines(list(Lines(list(Line(data)), "id")))

        return(lines)

        # Now, if we have multiple lines...
    } else if (!is.null(id_field)) {

        # Split into a list by ID field
        paths <- sp::split(data, data[[id_field]])

        sp_lines <- SpatialLines(list(Lines(list(Line(paths[[1]])), "line1")))

        if (length(paths) > 1) {
            # I like for loops, what can I say...
            for (p in 2:length(paths)) {
                id <- paste0("line", as.character(p))
                l <- SpatialLines(list(Lines(list(Line(paths[[p]])), id)))
                sp_lines <- spRbind(sp_lines, l)
            }
        }

        return(sp_lines)
    }
}



get_tmc_coords <- function(coords_string) {
    coord2 <- str_extract(coords_string, pattern = "(?<=')(.*?)(?=')")
    coord_list <- str_split(str_split(coord2, ",")[[1]], " ")

    tmc_coords <- purrr::transpose(coord_list) %>%
        lapply(unlist) %>%
        as.data.frame(., col.names = c("longitude", "latitude")) %>%
        mutate(latitude = as.numeric(trimws(latitude)),
               longitude = as.numeric(trimws(longitude)))
    as_tibble(tmc_coords)
}
#----------------------------------------------------------
get_geom_coords <- function(coords_string) {
    if (!is.na(coords_string)) {
        coord_list <- str_split(unique(str_split(coords_string, ",|:")[[1]]), " ")

        geom_coords <- purrr::transpose(coord_list) %>%
            lapply(unlist) %>%
            as.data.frame(., col.names = c("longitude", "latitude")) %>%
            mutate(latitude = as.numeric(trimws(latitude)),
                   longitude = as.numeric(trimws(longitude)))
        as_tibble(geom_coords)
    }
}


get_signals_sp <- function(bucket, corridors) {

    corridor_palette <- c("#e41a1c", "#377eb8", "#4daf4a", "#984ea3",
                          "#ff7f00", "#ffff33", "#a65628", "#f781bf")

    rtop_corridors <- corridors %>%
        filter(grepl("^Zone", Zone)) %>%
        distinct(Corridor)

    num_corridors <- nrow(rtop_corridors)

    corridor_colors <- rtop_corridors %>%
        mutate(
            color = rep(corridor_palette, ceiling(num_corridors/8))[1:num_corridors]) %>%
        #color = rep(RColorBrewer::brewer.pal(7, "Dark2"),
        #            ceiling(num_corridors/7))[1:num_corridors]) %>%
        bind_rows(data.frame(Corridor = c("None"), color = GRAY)) %>%
        mutate(Corridor = factor(Corridor))

    corr_levels <- levels(corridor_colors$Corridor)
    ordered_levels <- fct_relevel(corridor_colors$Corridor, c("None", corr_levels[corr_levels != "None"]))
    corridor_colors <- corridor_colors %>%
        mutate(Corridor = factor(Corridor, levels = ordered_levels))


    most_recent_intersections_list_key <- max(
        aws.s3::get_bucket_df(
            bucket = bucket,
            prefix = "maxv_atspm_intersections")$Key)
    ints <- s3read_using(
        read_csv,
        col_types = cols(
            .default = col_double(),
            PrimaryName = col_character(),
            SecondaryName = col_character(),
            IPAddress = col_character(),
            Enabled = col_logical(),
            Note_atspm = col_character(),
            Start = col_datetime(format = ""),
            Name = col_character(),
            Note_maxv = col_character(),
            HostAddress = col_character()
        ),
        bucket = bucket,
        object = most_recent_intersections_list_key,
    ) %>%
        select(-X1) %>%
        filter(Latitude != 0, Longitude != 0) %>%
        mutate(SignalID = factor(SignalID))

    signals_sp <- left_join(ints, corridors, by = c("SignalID")) %>%
        mutate(
            Corridor = forcats::fct_explicit_na(Corridor, na_level = "None")) %>%
        left_join(corridor_colors, by = c("Corridor")) %>%
        mutate(SignalID = factor(SignalID), Corridor = factor(Corridor),
               Description = if_else(
                   is.na(Description),
                   glue("{as.character(SignalID)}: {PrimaryName} @ {SecondaryName}"),
                   Description)
        ) %>%

        mutate(
            # If part of a Zone (RTOP), fill is black, otherwise white
            fill_color = ifelse(grepl("^Z", Zone), BLACK, WHITE),
            # If not part of a corridor, gray outer color, otherwise black
            stroke_color = ifelse(Corridor == "None", GRAY, BLACK),
            # If part of a Zone (RTOP), override black outer to corridor color
            stroke_color = ifelse(grepl("^Z", Zone), color, stroke_color))
    Encoding(signals_sp$Description) <- "utf-8"
    signals_sp
}



get_map_data <- function(conf) {

    BLACK <- "#000000"
    WHITE <- "#FFFFFF"
    GRAY <- "#D0D0D0"
    DARK_GRAY <- "#7A7A7A"
    DARK_DARK_GRAY <- "#494949"

    corridor_palette <- c("#e41a1c", "#377eb8", "#4daf4a", "#984ea3",
                          "#ff7f00", "#ffff33", "#a65628", "#f781bf")

    corridors <- s3read_using(
        read_feather,
        bucket = conf$bucket,
        object = "all_Corridors_Latest.feather")

    rtop_corridors <- corridors %>%
        filter(grepl("^Zone", Zone)) %>%
        distinct(Corridor)

    num_corridors <- nrow(rtop_corridors)

    corridor_colors <- rtop_corridors %>%
        mutate(
            color = rep(corridor_palette, ceiling(num_corridors/8))[1:num_corridors]) %>%
        bind_rows(data.frame(Corridor = c("None"), color = GRAY)) %>%
        mutate(Corridor = factor(Corridor))

    corr_levels <- levels(corridor_colors$Corridor)
    ordered_levels <- fct_relevel(corridor_colors$Corridor, c("None", corr_levels[corr_levels != "None"]))
    corridor_colors <- corridor_colors %>%
        mutate(Corridor = factor(Corridor, levels = ordered_levels))

    zone_colors <- data.frame(
        zone = glue("Zone {seq_len(8)}"),
        color = corridor_palette) %>%
        mutate(color = if_else(color=="#ffff33", "#f7f733", as.character(color)))

    # this takes a while: sp::coordinates is slow
    tmcs <- s3read_using(
        read_excel,
        bucket = conf$bucket,
        object = "Corridor_TMCs_Latest.xlsx") %>%
        mutate(
            Corridor = forcats::fct_explicit_na(Corridor, na_level = "None")) %>%
        left_join(corridor_colors, by = "Corridor") %>%
        mutate(
            Corridor = factor(Corridor),
            color = if_else(Corridor!="None" & is.na(color), DARK_DARK_GRAY, color),
            tmc_coords = purrr::map(coordinates, get_tmc_coords),
            sp_data = purrr::map(
                tmc_coords, function(y) {
                    points_to_line(data = y, long = "longitude", lat = "latitude")
                })
        )

    corridors_sp <- do.call(rbind, tmcs$sp_data) %>%
        SpatialLinesDataFrame(tmcs, match.ID = FALSE)

    subcor_tmcs <- tmcs %>%
        filter(!is.na(Subcorridor))

    subcorridors_sp <- do.call(rbind, subcor_tmcs$sp_data) %>%
        SpatialLinesDataFrame(tmcs, match.ID = FALSE)

    signals_sp <- get_signals_sp()

    map_data <- list(
        corridors_sp = corridors_sp,
        subcorridors_sp = subcorridors_sp,
        signals_sp = signals_sp)

    map_data
}
