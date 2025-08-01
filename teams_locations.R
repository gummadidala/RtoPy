library(maps)
library(sf)
library(tidyverse)

# Download Teams Locations Report from TEAMS Application as Excel
locs <- readxl::read_excel("TEAMS-Locations-MVID_2021-07-01.xlsx") %>%
    rename(Latitude = "Latitude...16",
           Longitude = "Longitude...17",
           `Custom Identifier` = "Custom Identifier...8") %>%
    replace_na(list(
        `Route 1 Designation` = "",
        `Route 1 Cardinality` = "",
        `Route 2 Designation` = "",
        `Route 2 Cardinality` = "")) %>%
    mutate(Route1 = stringr::str_trim(paste(`Route 1 Name`, `Route 1 Designation`, `Route 1 Cardinality`)),
           Route2 = stringr::str_trim(paste(`Route 2 Name`, `Route 2 Designation`, `Route 2 Cardinality`)))
locs <- locs[c("DB Id", "Custom Identifier", "Route1", "Route2", "Assummed MVID", "Latitude", "Longitude")]

locs[locs$Longitude > 1, "Longitude"] <- locs[locs$Longitude > 1, "Longitude"] * -1

# Spatial Join with a Polygon of the state of Georgia. Either in or out.
US <- st_as_sf(map("state", plot = FALSE, fill = TRUE))
SE <- subset(US, ID %in% c("georgia", "alabama", "florida", "south carolina"))
teamsPoints <- st_as_sf(locs, coords = c("Longitude", "Latitude"), crs = st_crs(SE))
st_join(teamsPoints, SE)
#

# Read sigops corridors file from S3
sigops <- readxl::read_excel("SigOpsRegions_20210628_Edited.xlsx")

# Calculate pairwise distance matrix
dists <- geodist::geodist(locs, sigops)
dists[is.na(dists)] = 999

# Reduce pairwise matrix to vector of minimum distance value
minids <- unlist(apply(dists, 1, which.min))
mins <- apply(dists, 1, min)



locs$dist <- mins
locations <- dplyr::bind_cols(
    rename(locs, Latitude_teams = Latitude, Longitude_teams = Longitude),
    sigops[minids,]) %>%
    arrange(`Assummed MVID`, dist)
readr::write_csv(locations, "teams_locations2.csv")

l <- locations %>%
    group_by(MVID = as.integer(`Assumed MVID`)) %>%
    summarize(
        n = n(),
        mindist = min(dist),
        maxdist = max(dist)) %>%
    ungroup()
sigops2 <- left_join(mutate(sigops, MVID = as.integer(MVID)), l, by = c("MVID"))

readr::write_csv(sigops2, "sigops2.csv")

leaflet::leaflet() %>%
    leaflet::addProviderTiles(leaflet::providers$CartoDB.Positron) %>%
    leaflet::addCircleMarkers(
        data = locations,
        lat = ~Latitude_teams,
        lng = ~Longitude_teams,
        color = ~dist)
