# dws.R
# version 0.1
# rkoppe

if (!"jsonlite" %in% installed.packages()) install.packages("jsonlite")
require(jsonlite)
if (!"httr" %in% installed.packages()) install.packages("httr")
require(httr)


dws.SENSOR_BASE_URL <- "https://sensor.awi.de/rest"
dws.DATA_BASE_URL <- "https://dashboard.awi.de/data-xxl/rest"



#' Loads availble sensors from the data service. The optional
#' \code{pattern} allows * wildcards and can be used to search for sensors.
#' 
#' See https://dashboard.awi.de/data/ for documentation.
#' 
dws.sensors <- function(pattern = NULL) {
  query = paste(
    dws.DATA_BASE_URL, "/sensors",
    sep = ""
  )
  
  if (!is.null(pattern)) query = paste(query, "?pattern=", pattern, sep = "")
  
  ls = readLines(query, warn = FALSE)
  t = fromJSON(ls)
  t
}



#' Loads data from the data service for given \code{sensors}
#' in the given time range and selected aggregate.
#' See https://dashboard.awi.de/data/ for documentation.
#' 
dws.get <- function(sensors, begin, end, aggregate = "hour", aggregateFunctions = NULL, qualityFlags = NULL, withQualityFlags = FALSE, withLogicalCode = FALSE) {
  # validation
  if (missing(sensors)) {
    stop("Sensor(s) must be defined.")
  }
  if (missing(begin)) {
    stop("Begin timestamp must be defined.")
  }
  if (missing(end)) {
    stop("End timestamp must be defined.")
  }
  if (missing(aggregate)) {
    aggregate = 'hour'
  }
  
  
  # transform sensor string to list
  if (is.character(sensors)) {
    sensors = c(sensors);
  }
  
  if (length(sensors) < 1) {
    stop("Sensor(s) must be defined.")
  }
  
  if (is.null(aggregateFunctions)) {
    aggregateFunctions = list()
  } else if (is.character(aggregateFunctions)) {
    aggregateFunctions = list(aggregateFunctions)
  }
  
  if (!is.null(qualityFlags) && (is.numeric(qualityFlags))) {
    qualityFlags = list(qualityFlags)
  }

  # build query
  if (nchar(begin) == 10) begin = paste(begin, "T00:00:00", sep = "")
  if (nchar(end) == 10) end = paste(end, "T23:59:59", sep = "")

  j = paste('{',
    '"beginDate": "', begin, '", ',
    '"endDate": "', end, '", ', 
    '"sensors": ["', paste(unlist(sensors), collapse = '", "'), '"], ',
    '"aggregate": "', toupper(aggregate), '", ',
    ifelse(length(aggregateFunctions) > 0, paste('"aggregateFunctions": ["', paste(unlist(aggregateFunctions), collapse = '", "'), '"], ', sep = ''), ''), 
    '"qualityFlags": [', paste(unlist(qualityFlags), collapse = ','), '], ',
    '"withQualityFlags": ', ifelse(withQualityFlags, 'true', 'false'), ', ',
    '"withLogicalCode": ', ifelse(withLogicalCode, 'true', 'false'),
    '}',
    sep = ''
  )
  
  url = paste(dws.DATA_BASE_URL, "/data/bulk?format=text/tab-separated-values", sep = "")

  # request data
  r = POST(url, body = j, content_type_json())
  stop_for_status(r)
  c = content(r, "parsed", "text/plain", encoding = "UTF-8")

  read.csv(text = c, sep = "\t", encoding = "UTF-8")
}



#' Loads data from the data service for given \code{sensors}
#' in the given time range and selected aggregate.
#' See https://dashboard.awi.de/data/ for documentation.
#' 
dws._get <- function(sensors, begin, end, aggregate = "hour", aggregateFunctions = NULL, qualityFlags = NULL, withQualityFlags = FALSE, withLogicalCode = FALSE) {
  # validation
  if (missing(sensors)) {
    stop("Sensor(s) must be defined.")
  }
  if (missing(begin)) {
    stop("Begin timestamp must be defined.")
  }
  if (missing(end)) {
    stop("End timestamp must be defined.")
  }
  if (missing(aggregate)) {
    aggregate = 'hour'
  }
  
  
  # transform sensor string to list
  if (is.character(sensors)) {
    sensors = c(sensors);
  }
  
  if (length(sensors) < 1) {
    stop("Sensor(s) must be defined.")
  }
  
  if (is.null(aggregateFunctions)) {
    aggregateFunctions = c()
  } else if (is.character(aggregateFunctions)) {
    aggregateFunctions = c(aggregateFunctions)
  }
  
  if (is.numeric(qualityFlags)) {
    qualityFlags = c(qualityFlags)
  }
  
  
  # build query
  if (nchar(begin) == 10) begin = paste(begin, "T00:00:00", sep = "")
  if (nchar(end) == 10) end = paste(end, "T23:59:59", sep = "")
  
  query = paste(
    dws.DATA_BASE_URL, "/data",
    "?format=text/tab-separated-values",
    "&beginDate=", URLencode(begin),
    "&endDate=", URLencode(end),
    '&aggregate=', toupper(aggregate),
    sep = "")
  
  for (sensor in sensors) {
    if (nchar(sensor) < 1) {
      stop("Empty sensor is invalid.")
    }
    query = paste(query, "&sensors=", URLencode(sensor), sep = "")
  }
  
  for (aggregateFunction in aggregateFunctions) {
    query = paste(query, "&aggregateFunctions=", aggregateFunction, sep = "")
  }
  
  for (qualityFlag in qualityFlags) {
    query = paste(query, "&qualityFlags=", qualityFlag, sep = "")
  }
  
  if (withQualityFlags) {
    query = paste(query, "&withQualityFlags=true", sep = "")
  }
  
  if (withLogicalCode) {
    query = paste(query, "&withLogicalCode=true", sep = "")
  }
  
  # request data
  read.csv(query, sep = "\t", encoding = "UTF-8")
}



#' Loads basic sensor metadata for the given \code{code} from sensor.awi.de.
#'
dws.sensor <- function(code) {
  # validation
  if (missing(code)) {
    stop("Code must be defined.")
  }

  query = paste(
    dws.SENSOR_BASE_URL, "/sensors/sensorOutputs/getSensorOutputByUrn/",
    URLencode(code),
    sep = "")
  print(query)
  ls = readLines(query, warn = FALSE)
  t = fromJSON(ls)
  
  r = list()
  r["id"] = t$id
  r["name"] = t$name
  r["type"] = t$sensorOutputType$generalName
  r["description"] = t$sensorOutputType$description
  r["definition"] = t$sensorOutputType$vocableValue
  r["unit"] = t$unitOfMeasurement$code
  r["properties"] = properties = list(list())

    
  # load measurement properties
  query = paste(
    dws.SENSOR_BASE_URL, "/sensors/measurementProperties/getSensorOutputMeasurementProperties/",
    r$id,
    sep = "")
  ls = readLines(query, warn = FALSE)
  t = fromJSON(ls)

  for (i in 1:nrow(t)) {
    r["properties"][[1]][[t[i, "measurementName"]]] = list(
      id = t[i, "id"],
      lower = t[i, "lowerBound"],
      upper = t[i, "upperBound"],
      unit = t[i, "unitOfMeasurement"]$code)
  }
    
  r
}



#' Loads basic platform metadata for the given \code{code} from sensor.awi.de
#' 
dws.platform <- function(code) {
  # validation
  if (missing(code)) {
    stop("Code must be defined.")
  }
  
  # strip code to platform format which is platformType:platform
  parts = strsplit(code, split = ":")
  if (length(parts[[1]]) < 2) {
    stop("Code is to short and can not be resolved to a platform code.")
  }

  # platformType:platform
  base = paste(parts[[1]][1:2], collapse = ":")
  
  # get the platform document
  query = paste(
    dws.SENSOR_BASE_URL, "/sensors/device/getDeviceByUrn/",
    URLencode(base),
    sep = "")
  
  ls = readLines(query, warn = FALSE)
  j = fromJSON(ls)
  
  r = list()
  r["id"] = j$ID
  r["code"] = j$urn
  r["shortName"] = j$shortName
  r["name"] = j$longName
  r["description"] = j$description
  r["definition"] = j$rootItemType$vocableValue
  r
}



#' Loads basic metadata of the platform with all sensors and measurement properties.
#' 
dws.meta <- function(code) {
  platform = dws.platform(code)
  
  # get the detailed document
  query = paste(
    dws.SENSOR_BASE_URL, "/sensors/device/getDetailedItem/",
    platform$id,
    "?includeChildren=true",
    sep = "")
  
  ls = readLines(query, warn = FALSE)
  j = fromJSON(ls)
  
  # parse children
  r = dws.parseItems(j$childItem)
  platform["children"] = list(r$items)
  
  # simple map for parameters and their properties
  platform["map"] = list(r$map)

  platform  
}
dws.parseItems <- function(sensorItems) {
  items = list()
  map = list()
  for (i in 1:nrow(sensorItems)) {
    sensorItem = sensorItems[i,]

    # basic infos
    item = list()
    item["id"] = sensorItem$ID
    item["code"] = sensorItem$urn
    item["shortName"] = sensorItem$shortName
    item["name"] = sensorItem$longName
    item["description"] = sensorItem$description
    item["definition"] = sensorItem$rootItemType$vocableValue
    
    # parameters
    parameters = list()
    sensorOutputs = sensorItem$sensorOutput_Item[[1]]$sensorOutput
    if (!is.null(sensorOutputs)) {
      for (j in 1:nrow(sensorOutputs)) {
        sensorOutput = sensorOutputs[j,]
  #      print(paste("sensorOutput...", sensorOutput$id))
        parameter = list()
        parameter["id"] = sensorOutput$id
        parameter["name"] = sensorOutput$name
        parameter["code"] = ifelse(sensorOutput$shortname != "", sensorOutput$shortname, sensorOutput$name)
        parameter["type"] = sensorOutput$sensorOutputType$generalName
        parameter["description"] = sensorOutput$sensorOutputType$description
        parameter["definition"] = sensorOutput$sensorOutputType$vocableValue
        parameter["unit"] = sensorOutput$unitOfMeasurement$code
  
        # properties
        sensorProperties = sensorOutput$measurementPropertySensorOutput

        properties = list()
        propertyMap = list(list())
        if (!is.null(sensorProperties) && (length(sensorProperties[[1]]) > 0)) {
          for (k in 1:nrow(sensorProperties[[1]])) {
            sensorProperty = sensorProperties[[1]][k,]
            property = list()
            property["name"] = sensorProperty$measurementProperty$measurementName
            property["lower"] = sensorProperty$measurementProperty$lowerBound
            property["upper"] = sensorProperty$measurementProperty$upperBound
            property["unit"] = sensorProperty$measurementProperty$unitOfMeasurement$code
            properties[[k]] = property
            
            propertyMap[gsub(" ", "_", tolower(property["name"]))] = list(property)
          }
        }
        parameter["properties"] = list(properties)
        
        parameters[[j]] = parameter
        
        # prepare parameter property map
        code = paste(item["code"], parameter["code"], sep = ":")
        m = parameter
        #m["properties"] = propertyMap
        map[[code]] = parameter
        map[[code]]["properties"] = list(propertyMap)
        
      }
    }
    item["parameters"] = list(parameters)
    
    # children
#    print(paste("parse children of", item["code"], "with", nrow(sensorItem$childItem[[1]]), "rows..."))
#    print(sensorItem$childItem$longName)
    
    children = list()
    if (!is.null(sensorItem$childItem) && (length(sensorItem$childItem[[1]]) > 0)) {
      r = dws.parseItems(sensorItem$childItem[[1]])
      children = r$items
      map = c(map, r$map)
    }
    item["children"] = list(children)
    
    items[[i]] = item
  }
  
  list(
    map = map,
    items = items
  )
}


#' Loads full metadata of the platform associated with the given \code{code} as JSON.
#' 
dws.meta.json <- function(code, pretty = FALSE) {
  platform = dws.platform(code)

  # get the detailed document
  query = paste(
    dws.SENSOR_BASE_URL, "/sensors/device/getDetailedItem/",
    platform$id,
    "?includeChildren=true",
    sep = "")
  
  ls = readLines(query, warn = FALSE)

  if (pretty) {
    toJSON(fromJSON(ls), pretty = TRUE)
  } else {
    ls
  }
}



#' Loads full metadata of the platform associated with the given \code{code} as SensorML.
#' 
dws.meta.sensorML <- function(code) {
  platform = dws.platform(code)
  
  query = paste(
    dws.SENSOR_BASE_URL, "/sensors/device/getDeviceAsSensorML/",
    platform$id,
    sep = "")
  
  ls = readLines(query, warn = FALSE)
  ls
}