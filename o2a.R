
library("httr2")
library("jsonlite")
library("readr")

REGISTRY <- "https://registry.o2a-data.de/rest/v2"
DWS <- "https://dashboard.awi.de/data/rest"

download <- function(url) {
    response <- req_perform(request(url))
    if (response$status_code != 200) {
        stop("Error loading data.")
    } else {
        return(resp_body_json(response))
    }
}

## ---------------------------  ¬!"£$%^&*()_+ --------------------------- ##
testAggregate <- function(pattern, string) {
    ## :pattern: aggretation pattern to check on
    ## :string: string to be tested
    p <- paste0("[", pattern, "]\\w+")
    a <- grepl(p, string, ignore.case = TRUE)
    return(a)
}

## ---------------------------  ¬!"£$%^&*()_+ --------------------------- ##
items <- function(pattern = NULL) {
    ## Loads availble sensors from the data service. The optional
    ## pattern allows * wildcards and can be used to search for sensors.
    ## See https://dashboard.awi.de/data/ for documentation.
    ## :pattern: is parameter urn(s)
    url <- paste0(DWS, "/sensors")
    if (!is.null(pattern)) {
        url <- paste0(url, "?pattern=", pattern)
    }
    itemsJson <- download(url)
    return(itemsJson)
}


## ---------------------------  ¬!"£$%^&*()_+ --------------------------- ##
downloadDataFromDWS <- function(itemUrns,
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
    secondTest <- testAggregate("sec", aggregate)
    minTest <- testAggregate("min", aggregate)
    hourTest <- testAggregate("hour", aggregate)
    dayTest <- testAggregate("day", aggregate)
    ##
    baseLink <- paste0(
        DWS,
        "/data?sensors=",
        itemUrns,
        "&beginDate=",
        as.character(begin),
        "&endDate=",
        as.character(end),
        "&aggregate="
    )
    if (secondTest == TRUE) {
        response <- req_perform(
            request(
                paste0(baseLink,
                "second&streamit=true&withQualityFlags=false",
                "&withLogicalCode=false")
            )
        )
    }
    if (minTest == TRUE) {
        response <- req_perform(
            request(
                paste0(
                baseLink,
                "minute&aggregateFunctions=",
                aggregateFunctions,
                "&streamit=true&withQualityFlags=false",
                "&withLogicalCode=false")
            )
        )
    }
    if (hourTest == TRUE) {
        response <- req_perform(
            request(
                paste0(
                baseLink,
                "hour&aggregateFunctions=",
                aggregateFunctions,
                "&streamit=true&withQualityFlags=false",
                "&withLogicalCode=false")
            )
        )
    }
    if (dayTest == TRUE) {
        response <- req_perform(
            request(
                paste0(
                baseLink,
                "day&aggregateFunctions=",
                aggregateFunctions,
                "&streamit=true&withQualityFlags=false&withLogicalCode=false")
            )
        )
    }
    if (response$status_code != 200) {
        stop(sprintf("Error loading data: %s", response$reason))
    }
    ##
    respBody <- resp_body_string(response)
    df <- read.table(text = respBody, header = TRUE, sep = "\t")
    return(df)
}


## ---------------------------  ¬!"£$%^&*()_+ --------------------------- ##

item <- function(code) {
    ## Request and parse item properties for a given item urn as "code"
    ## :code: item unique resource number (urn) or ID
    if (is.character(code)) {
        url <- paste0(REGISTRY, "/items/", code)
        itemJson <- download(url)
    } else if (is.numeric(code)) {
        url <- paste0(REGISTRY, "/items/", code)
        itemJson <- download(url)
    } else {
        stop("provide item urn or item ID")
  }
    ## Adding the items porperties to the Json
    url <- paste0(REGISTRY, "/items/", itemJson$id, "/properties")
    propertiesJson <- download(url)$records
    if (length(propertiesJson) == 0) { #### if no properties
        properties <- NA
    } else {
        # create look up lists
        unitList <- list()
        typeList <- list()
        for (propertyRecord in propertiesJson) {
            propertyUnit <- propertyRecord$unit
            if (length(propertyUnit) > 1) {
                uuid <- propertyUnit$`@uuid`
                unitList[uuid] <- list(propertyUnit)
            }
        }
        for (propertyRecord in propertiesJson) {
            propertyType <- propertyRecord$type
            if (length(propertyType) > 1) {
                uuid <- propertyType$`@uuid`
                typeList[uuid] <- list(propertyType)
            }
        }
        # enhance propertiesJson
        i <- 0
        for (propertyRecord in propertiesJson) {
            i <- i + 1
            propertyUnit <- propertyRecord$unit #uuid
            if (length(propertyUnit) == 1) {
                fullUnit <- unlist(unitList[propertyUnit],
                                    recursive = FALSE,
                                    use.names = FALSE)
                names(fullUnit) <- names(unitList[[1]])
                propertiesJson[[i]]$unit <- fullUnit
            }
        }
        j <- 0
        for (propertyRecord in propertiesJson) {
            j <- j + 1
            propertyType <- propertyRecord$type #uuid
            if (length(propertyType) == 1) {
                fullType <- unlist(typeList[propertyType],
                                    recursive = FALSE,
                                    use.names = FALSE)
                names(fullType) <- names(typeList[[1]])
                propertiesJson[[j]]$type <- fullType
            }
        }
    }
    itemJson$properties <- propertiesJson
    return(itemJson)
}

## ---------------------------  ¬!"£$%^&*()_+ --------------------------- ##
parameters <- function(code) {
    ## Request....
    ## :code: item ID or urn
    if (is.character(code)) {
        item2 <- item(code)
        code <- item2$id
    } else if (is.numeric(code)) {
        code <- code
        item2 <- item(code)
    } else {
        stop("provide item urn or item ID")
    }
    ##
    url <- paste0(REGISTRY, "/items/", code, "/parameters")
    parameterJson <- download(url)$records
    parameterUrn <- NA
    for (parameterRecord in 1:length(parameterJson)) {
        parameterUrn <- paste0(item2$code, ":",
                                parameterJson[[parameterRecord]]$shortName)
        parameterJson[[parameterRecord]]$urn <- parameterUrn
    }
    return(parameterJson)
}

## ---------------------------  ¬!"£$%^&*()_+ --------------------------- ##
#code <- 8566
events <- function(code, geo = FALSE) {
    ## Requests all events of an item, returns as dict
    ## :code: registry id of item
    ## :geo: TRUE == only with valid coordinates, FALSE == all events
    if (is.character(code)) {
        code <- item(code)$id
    } else if (is.numeric(code)) {
        code <- code
    } else {
        stop("provide item urn or item ID")
    }
    ##
    if (geo == TRUE) {
        url <- paste0(REGISTRY, "/items/",
                      as.character(code),
                      "/events?where=",
                      "latitude%3E%3D-90%20and%20latitude%3C%3D90%20",
                      "and%20longitude%3E%3D-180%20and%20longitude%3C%3D180"
                      )
    } else {
        url <- paste0(REGISTRY, "/items/", as.character(code), "/events")
    }
    #
    eventsJson <- download(url)$records
    ## create lookup table
    lut <- list()
    for (eventRecord in eventsJson) {
        eventType <- eventRecord$type
        if (length(eventType) > 1) {
            uuid <- eventType$`@uuid`
            lut[uuid] <- list(eventType)
        }
    }
    ## enrich event info
    i <- 0 # variable to count iterations of for loop
    for (eventRecord in eventsJson) {
        i <- i + 1
        if (length(eventRecord$type) == 1) {
            eventType <- eventRecord$type  #event type uuid from record in Json
            fullType <- unlist(lut[eventType],
                                recursive = FALSE,
                                use.names = FALSE)
            names(fullType) <- names(lut[[1]]) # reassign correct names
            eventsJson[[i]]$type <- fullType # paste into correct position in eventsJson
        }
    }
    return(eventsJson)
}



## ---------------------------  ¬!"£$%^&*()_+ --------------------------- ##

contacts <- function(code) {
    ## requests contacts per item
    ## :code: registry id or urn of item
    if (is.character(code)) {
        item2 <- item(code)
        code <- item2$id
    } else if (is.numeric(code)) {
        code <- code
    } else {
        stop("provide item urn or item ID")
    }
    ##
    url <- paste0(REGISTRY, "/items/", as.character(code), "/contacts")
    ##
    contactsJson <- download(url)$records
    ## create look up lists
    contactsList <- list()
    rolesList <- list()
    for (contactRecord in contactsJson) {
        contactContact <- contactRecord$contact
        if (length(contactContact) > 1) {
            uuid <- contactContact$`@uuid`
            contactsList[uuid] <- list(contactContact)
        }
        #
        contactRole <- contactRecord$role
        if (length(contactRole) > 1) {
            uuid <- contactRole$`@uuid`
            rolesList[uuid] <- list(contactRole)
        }
    }
    ## enricht contacts info
    i <- 0
    for (contactRecord in contactsJson) {
        i <- i + 1
        if (length(contactRecord$contact) == 1) {
            contactContact <- contactRecord$contact
            fullContact <- unlist(contactContact,
                                recursive = FALSE,
                                use.names = FALSE)
            names(fullContact) <- names(contactsList[[1]])
            contactsJson[[i]]$contact <- fullContact
        }
        #
        if (length(contactRecord$role) == 1) {
            contactRole <- contactRecord$role
            fullRole <- unlist(rolesList[contactRole],
                                recursive = FALSE,
                                use.names = FALSE)
            names(fullRole) <- names(rolesList[[1]])
            contactsJson[[i]]$role <- fullRole
        }
    }
    return(contactsJson)
}

## ---------------------------  ¬!"£$%^&*()_+ --------------------------- ##
code <- 9215
subitems <- function(code) {
    ## retrieve subitems via code/urn by parent ID
    ## :code: item unique resource number (urn) or ID
    if (is.character(code)) {
        item2 <- item(code)
        code <- item2$id
    } else if (is.numeric(code)) {
        code <- code
    } else {
        stop("provide item urn or item ID")
    }
    ##
    url <- paste0(REGISTRY,
                  "/items?where=parent.id==",
                  as.character(code))
    ##
    subitemsJson <- download(url)$records
    ##
    subitemsTypes <- list()
    for (subitemRecord in subitemsJson) {
        subitemType <- subitemRecord$type
        if (length(subitemType) > 1) {
            uuid <- subitemType$`@uuid`
            subitemsTypes[uuid] <- list(subitemType)
        }
    }
    ## enrich info
    i <- 0
    for (subitemRecord in subitemsJson) {
        i <- i + 1
        subitemType <- subitemRecord$type
        if (length(subitemType) == 1) {
            fullType <- unlist(subitemsTypes[subitemType],
                                recursive = FALSE,
                                use.names = FALSE)
        names(fullType) <- names(subitemsTypes[[1]])
        subitemsJson[[i]]$type <- fullType
        }
    }
    return(subitemsJson)
}


## eof
