
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
item <- function(code) {
    ## Request and parse item properties for a given item urn as "code"
    ## :code: item unique resource number (urn) or ID
    if (is.character(code)) {
        url <- paste0(dws$REGISTRY, "/items?where=code=LIKE=", code)
        j <- dws$download(url)$records
    } else if (is.numeric(code)) {
        url <- paste0(dws$REGISTRY, "/items/", code)
        j <- dws$download(url)
    } else {
        stop("provide item urn or item ID")
  }
    url <- paste0(dws$REGISTRY, "/items/", j$id, "/properties")
    k <- dws$download(url)$records
    if (length(k) == 0) {
        newk <- NA
    } else {
        kk <- split(k,seq(nrow(k)))
        newk <- list()
        for (l in 1:length(kk)){
            e <- list()
            for (i in 1:ncol(kk[[l]])) {
                e[[i]] <- kk[[l]][ ,i]
            }
            names(e) <- colnames(kk[[l]])
            newk[[l]] <- e
        }
    }
    j$itemProperties <- newk
    return(j)
}

#### ---

parameters <- function(code){
    ## Request....
    ## :code: item ID or urn 
    if (is.character(code)) {
        item <- item(code)
        code <- item$id
    } else if (is.numeric(code)) {
        code <- code
    } else {
        stop("provide item urn or item ID")
    }
    ##
    url <- paste0(dws$REGISTRY, "/items/", code, "/parameters")
    k <- dws$download(url)$records
    urn <- NA
    for (i in 1:nrow(k)) {
        urn[i] <- paste0(item$code, ":", k$shortName[i])
    }
    k <- cbind(k, urn)
    return(k)
}

#parameters(4044)

#code <- "vessel:polarstern:pco2_go_ps"
##a <- item(code)
#code <- 'vessel:polarstern:pco2_go_ps'
#code <- 'vessel:polarstern'
#code <- 5085
#dws$items(itemUrns)
#itemUrns <- "vessel:polarstern:pco2_go_ps:pre_fco, vessel:polarstern:pco2_go_ps:pre_xco"
#begin <- "2024-02-22T00:00:00"
#end <- "2024-02-25T00:00:00"
#agg <- "day"
#aggfun <- "q75"

#a <- dws$downloadDataFromDWS(itemUrns, begin = begin, end = end, aggregate = agg, aggregateFunctions = aggfun)



## eof
