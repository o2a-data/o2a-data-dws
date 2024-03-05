
library('httr')
library('jsonlite')
library('readr')

dws <- list()

dws$REGISTRY <- "https://registry.o2a-data.de/rest/v2"
dws$DWS <- "https://dashboard.awi.de/data/rest"


dws$download <- function(url) {
    ## auxiliary function
    ## :url: externally created string to call by this fun
    response <- httr::GET(url)
    if (response$status_code != 200) {
        stop("Error loading data.")
    } else {
        return(jsonlite::fromJSON(content(response, "text")))
    }
}

dws$testAggregate <- function(pattern, string) {
    ## :pattern: aggretation pattern to check on
    ## :string: string to be tested
    p <- paste0("[", pattern, "]\\w+")
    a <- grepl(p, tolower(string))
    return(a)
}

dws$items <- function(pattern = NULL) {
    ## Loads availble sensors from the data service. The optional
    ## pattern allows * wildcards and can be used to search for sensors.
    ## See https://dashboard.awi.de/data/ for documentation.
    ## :pattern: is parameter urn(s)
    url <- paste0(dws$DWS, "/sensors")
    if (!is.null(pattern)) {
        url <- paste0(url, "?pattern=", pattern)
    }
    j <- dws$download(url)
    return(as.list(j))
}

dws$downloadDataFromDWS <- function(itemUrns,
                                    begin,
                                    end,
                                    aggregate,
                                    aggregateFunctions) {
    ## Loads data from the data service for given sensors
    ## in the given time range and selected aggregate.
    ## See https://dashboard.awi.de/data/ for documentation.
    ## :items: parameter urn(s)
    ## :begin: YYYY-MM-DDTHH:MM:SS string
    ## :end: YYYY-MM-DDTHH:MM:SS string
    ## :aggregate: second, minute, hour, day
    ## :aggregateFunctions: min, max, mean, median, std, count
    if (is.null(itemUrns) || length(itemUrns) == 0) {
        stop("Item(s) must be defined.")
    }
    if (is.null(begin)) {
        stop("Begin timestamp must be defined.")
    }
    if (is.null(end)) {
        stop("End timestamp must be defined.")
    }
    if (grepl(",", itemUrns)) {
        itemUrns <- gsub(" ", "", itemUrns)
        itemUrns <- gsub(",", "&sensors=", itemUrns)
    }
    secondTest <- dws$testAggregate("sec", aggregate)
    minTest <- dws$testAggregate("min", aggregate)
    hourTest <- dws$testAggregate("hour", aggregate)
    dayTest <- dws$testAggregate("day", aggregate)
    baseLink <- paste0(
        dws$DWS,
        "/data?sensors=",
        itemUrns,
        "&beginDate=",
        as.character(begin),
        "&endDate=",
        as.character(end),
        "&aggregate="
    )
    if (secondTest == TRUE) {
        response <- GET(
            paste0(
                baseLink,
                "second&streamit=true&withQualityFlags=false&withLogicalCode=false"
            )
        )
    }
    if (minTest == TRUE) {
        response <- GET(
            paste0(
                baseLink,
                "minute&aggregateFunctions=",
                aggregateFunctions,
                "&streamit=true&withQualityFlags=false&withLogicalCode=false"
            )
        )
    }
    if (hourTest == TRUE) {
        response <- GET(
            paste0(
                baseLink,
                "hour&aggregateFunctions=",
                aggregateFunctions,
                "&streamit=true&withQualityFlags=false&withLogicalCode=false"
            )
        )
    }
    if (dayTest == TRUE) {
        response <- GET(
            paste0(
                baseLink,
                "day&aggregateFunctions=",
                aggregateFunctions,
                "&streamit=true&withQualityFlags=false&withLogicalCode=false"
            )
        )
    }
    if (response$status_code != 200) {
        stop(sprintf("Error loading data: %s", response$reason))
    }
    ##   
    df <- as.data.frame(content(response)) #, sep = "\t")
    return(df)
}


### ---



#dws$items(itemUrns)
#itemUrns <- "vessel:polarstern:pco2_go_ps:pre_fco, vessel:polarstern:pco2_go_ps:pre_xco"
#begin <- "2024-02-22T00:00:00"
#end <- "2024-02-25T00:00:00"
#agg <- "day"
#aggfun <- "q75"

#a <- dws$downloadDataFromDWS(itemUrns, begin = begin, end = end, aggregate = agg, aggregateFunctions = aggfun)



## eof
