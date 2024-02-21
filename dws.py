"""
import datetime
import json
import os.path
import re
import requests
import urllib
"""

from _io import StringIO
import requests
import pandas as pd
import datetime as dt
from datetime import date
import json

#class dws:
"""
This script abstracts access to metadata stored in sensor.awi.de and data
available via the data web service (dws).
Have a look to the documentation at https://spaces.awi.de/display/DM and
the API descriptions https://sensor.awi.de/api/ and https://dashboard.awi.de/data-xxl/api/
"""

REGISTRY = "https://registry.awi.de/api/" 
DWS = "https://dashboard.awi.de/data/rest"


@staticmethod
def items(pattern: str = None):
    """
    Loads availble sensors from the data service. The optional
    pattern allows * wildcards and can be used to search for sensors.

    See https://dashboard.awi.de/data-xxl/ for documentation.
    """
    url = DWS + "/sensors"
    if pattern != None:
        url += "?pattern=" + pattern
        
    response = requests.get(url, stream=True)

    if response.status_code != 200:
        raise Exception("Error loading sensors.".format(response.reason))

    j = json.loads(response.content)

    return(j)

## items('vessel:polarstern:pco2_go_ps:pre_xco')




@staticmethod
def get(items,
        begin: date,
        end: date,
        aggregate: str = "hour",
        aggregateFunctions: list = None,
        ):
    """
    Loads data from the data service for given sensors
    in the given time range and selected aggregate.
    See https://dashboard.awi.de/data/ for documentation.
    """
    if items == None or len(items) == 0:
        raise Exception("Item(s) must be defined.")

    if begin == None:
        raise Exception("Begin timestamp must be defined.")

    if end == None:
        raise Exception("End timestamp must be defined.")

    if items.count(',') > 0:
        items = items.replace(' ', '').replace(',', '&sensors=')

    if aggregate.lower() == "second": 
        response = requests.get(DWS + '/data?sensors='
                                + items 
                                + '&beginDate=' + str(begin)
                                + '&endDate=' + str(end)
                                + '&aggregate=' + aggregate
                                + '&streamit=true&withQualityFlags=false&withLogicalCode=false'
                                )
    elif aggregate.lower() == "minute" or aggregate.lower() == "hour" or aggregate.lower() == "day":
        response = requests.get(DWS + '/data?sensors='
                                + items 
                                + '&beginDate=' + str(begin)
                                + '&endDate=' + str(end)
                                + '&aggregate=' + aggregate
                                + '&aggregateFunctions=' + aggregateFunctions
                                ##+ '&limit=20'
                                + '&streamit=true&withQualityFlags=false&withLogicalCode=false'
                                )
    else:
        raise Exception("No valid aggregate defined, use 'second', 'minute', 'hour', or 'day'.")
        
    if response.status_code != 200:
        raise Exception("Error loading data.".format(response.reason))

    # build the data frame
    df = pd.read_csv(StringIO(response.text), sep="\t")
    df["datetime"] = pd.to_datetime(df["datetime"])
    return(df)

## get(items, begin, end, aggregate, aggFun)

print('')
print('')

import sys
sys.exit()

    @staticmethod
    def sensor(code: str, sys=None):
        """
        Request and parse sensor properties for a given sensor urn as "code"
        :param code: sensor unique resource number (urn)
        :param sys: switch for requesting at an alternative (under development) service
        :return: dictionary of sensor properties
        """
        if sys == "dev":
            url = (
                dws.SENSOR_DEV_URL
                + "/sensors/sensorOutputs/getSensorOutputByUrn/"
                + urllib.parse.quote_plus(code)
            )
        else:
            url = (
                dws.SENSOR_BASE_URL
                + "/sensors/sensorOutputs/getSensorOutputByUrn/"
                + urllib.parse.quote_plus(code)
            )

        response = requests.get(url)

        if response.status_code != 200:
            raise Exception("Error loading sensor metadata.")

        j = json.loads(response.content)

        r = {
            "id": j["id"],
            "name": j["name"],
            "type": j["sensorOutputType"]["generalName"],
            "description": j["sensorOutputType"]["description"],
            "definition": j["sensorOutputType"]["vocableValue"],
            "unit": j["unitOfMeasurement"]["code"],
        }

        url = (
            dws.SENSOR_BASE_URL
            + "/sensors/measurementProperties/getSensorOutputMeasurementProperties/"
            + str(r["id"])
        )
        response = requests.get(url)

        j = json.loads(response.content)

        # parse json
        uuid_map = {}
        dws._map_uuids(j, uuid_map)

        properties = {}
        for i in j:
            # get property name
            name = i["measurementPropertyType"]
            if isinstance(name, dict):
                name = name["generalName"].lower().replace(" ", "_")
            elif name in uuid_map:
                name = uuid_map[name]["generalName"].lower().replace(" ", "_")

            # get unit
            unit = i["unitOfMeasurement"]
            if isinstance(unit, dict):
                unit = unit["code"]
            elif unit in uuid_map:
                unit = uuid_map[unit]["code"]

            properties[name] = {
                "id": i["id"],
                "lower": i["lowerBound"],
                "upper": i["upperBound"],
                "unit": unit,
            }

        r["properties"] = properties

        return r

    @staticmethod
    def platform(code: str):
        """
        Request and parse attributes at platform level only.

        :param code: sensor unique resource number (urn)
        :return: dictionary of platform attributes
        """
        parts = code.split(":")

        if len(parts) < 2:
            raise Exception(
                "Code is to short and cannot be resolved to a platform code."
            )

        base = ":".join(parts[0:2])
        url = (
            dws.SENSOR_BASE_URL
            + "/sensors/item/getItemByUrn/"
            + urllib.parse.quote_plus(base)
        )
        response = requests.get(url, stream=True)

        if response.status_code != 200:
            raise Exception("Error loading platform metadata.")

        j = json.loads(response.content)

        r = {
            "id": j["ID"],
            "code": j["urn"],
            "shortName": j["shortName"],
            "longName": j["longName"],
            "description": j["description"],
            "definition": j["rootItemType"]["vocableValue"]
            if "rootItemType" in j
            else j["subItemType"]["vocableValue"],
        }

        return r

    @staticmethod
    def meta(code: str, cache=False):
        """
        Loads basic metadata of the platform with all sensors and measurement properties.

        :param code: sensor unique resource number (urn)
        :param cache: for local storage of json file
        :return:
        """
        platform = dws.platform(code)

        identifier = platform["id"]
        filename = str(identifier) + ".json"

        # json
        j = None

        # check last modified
        lastModified = None
        if cache:
            try:
                if os.path.isfile(filename):
                    with open(filename, "r") as f:
                        j = json.load(f)
                        lastModified = j["lastModified"]
            except:
                pass

        # request metadata
        url = (
            dws.SENSOR_BASE_URL
            + "/sensors/item/getDetailedItem/"
            + str(identifier)
            + "?includeChildren=true"
        )

        if lastModified:
            url += "&pointInTime=" + lastModified

        # print('Requesting ' + url)

        response = requests.get(url, stream=True)

        if response.status_code == 200:
            j = json.loads(response.content)

            # cache content
            if cache:
                try:
                    with open(filename, "wb") as f:
                        f.write(response.content)
                except:
                    pass

        elif response.status_code == 204:
            pass

        elif response.status_code != 200:
            raise Exception("Error loading detailed platform metadata.")

        # parse json
        uuid_map = {}
        dws._map_uuids(j, uuid_map)

        map = dws._parseItems([j], uuid_map)
        platform["children"] = map["items"]
        platform["map"] = map["map"]

        return platform

    @staticmethod
    def meta_json(code: str):
        """
        Loads full metadata of the platform associated with the given code as JSON.
        """
        platform = dws.platform(code)

        url = (
            dws.SENSOR_BASE_URL
            + "/sensors/item/getDetailedItem/"
            + str(platform["id"])
            + "?includeChildren=true"
        )
        response = requests.get(url, stream=True)

        if response.status_code != 200:
            raise Exception("Error loading detailed platform metadata.")

        j = json.loads(response.content)

        return j

    @staticmethod
    def meta_sensorML(code: str):
        """
        Loads full metadata of the platform associated with the given code as SensorML.
        """
        platform = dws.platform(code)

        url = (
            dws.SENSOR_BASE_URL
            + "/sensors/item/getItemAsSensorML/"
            + str(platform["id"])
        )
        response = requests.get(url, stream=True)

        if response.status_code != 200:
            raise Exception("Error loading detailed platform metadata.")

        return response.content

    @staticmethod
    def _map_uuids(obj, map: dict = {}):
        """
        Parse the uuids for all values and sub items in the obj dictionary that contains uuid.

        "obj" starts with the server response for getDetailedItem and includeChildren=true
        and _map_uuids is called recursively to map attributes that contain dict or uuid.
        The "key" variable becomes a dict where events are dict keys of the variable "key"

        :param obj: dictionary of items and sub items, starting with the response for getDetailedItem
        :param map: the dictionary forwardly referencing the uuids
        :return: None, but updates argument map
        """
        for key in obj:
            if isinstance(key, dict):
                dws._map_uuids(key, map)
            else:
                value = obj[key]
                if isinstance(value, str):
                    if re.match(
                        "^[0-9a-f]{8}-[0-9a-f]{4}-[0-5][0-9a-f]{3}-[089ab][0-9a-f]{3}-[0-9a-f]{12}$",
                        value,
                    ):
                        if key == "@uuid":
                            map[value] = obj
                elif isinstance(value, list):
                    for v in value:
                        dws._map_uuids(v, map)
                elif isinstance(value, dict):
                    dws._map_uuids(value, map)

    @staticmethod
    def _parseItems(sensorItems: list, uuid_map: dict = {}):
        """
        Parses the given sensor items and returns a simplified item object.
        """
        items = []
        map = {}

        for sensorItem in sensorItems:
            if isinstance(sensorItem, str):
                sensorItem = uuid_map[sensorItem]

            identifier = sensorItem["ID"] if "ID" in sensorItem else sensorItem["id"]
            item = {
                "id": identifier,
                "code": sensorItem["urn"],
                "shortName": sensorItem["shortName"],
                "longName": sensorItem["longName"],
                "description": sensorItem["description"],
                "definition": sensorItem["rootItemType"]["vocableValue"]
                if "rootItemType" in sensorItem
                else "",
            }

            parameters = []
            if "sensorOutput_Item" in sensorItem:
                for sensorOutputItem in sensorItem["sensorOutput_Item"]:
                    if isinstance(sensorOutputItem, str):
                        sensorOutputItem = uuid_map[sensorOutputItem]

                    sensorOutput = sensorOutputItem["sensorOutput"]

                    sensorOutputType = sensorOutput["sensorOutputType"]
                    if isinstance(sensorOutputType, str):
                        sensorOutputType = uuid_map[sensorOutputType]

                    unit = sensorOutput["unitOfMeasurement"]
                    if isinstance(unit, str):
                        unit = uuid_map[unit]

                    parameter = {
                        "id": sensorOutput["id"],
                        "name": sensorOutput["name"],
                        "code": sensorOutput["shortname"]
                        if sensorOutput["shortname"] != ""
                        else sensorOutput["name"],
                        "type": sensorOutputType["generalName"],
                        "description": sensorOutputType["description"],
                        "definition": sensorOutputType["vocableValue"],
                        "unit": unit["code"],
                    }

                    properties = []
                    propertyMap = {}
                    if "measurementPropertySensorOutputs" in sensorOutput:
                        for sensorProperty in sensorOutput[
                            "measurementPropertySensorOutputs"
                        ]:
                            unit = sensorProperty["measurementProperty"][
                                "unitOfMeasurement"
                            ]
                            if isinstance(unit, str):
                                unit = uuid_map[unit]

                            ptype = sensorProperty["measurementProperty"][
                                "measurementPropertyType"
                            ]
                            if isinstance(ptype, str):
                                ptype = uuid_map[ptype]

                            property = {
                                "name": sensorProperty["measurementProperty"][
                                    "measurementName"
                                ],
                                "lower": sensorProperty["measurementProperty"][
                                    "lowerBound"
                                ],
                                "upper": sensorProperty["measurementProperty"][
                                    "upperBound"
                                ],
                                "unit": unit["code"],
                                "type": ptype["generalName"],
                            }

                            properties.append(property)
                            propertyMap[
                                property["type"].lower().replace(" ", "_")
                            ] = property

                    parameter["properties"] = properties
                    parameters.append(parameter)

                    code = item["code"] + ":" + parameter["code"]
                    map[code] = parameter
                    map[code]["properties"] = propertyMap

            items.append(item)

            if "childItem" in sensorItem:
                r = dws._parseItems(sensorItem["childItem"], uuid_map)
                item["children"] = r["items"]
                map.update(r["map"])

        #            map = {**map, **r['map']}

        r = {"items": items, "map": map}
        return r

    @staticmethod
    def base(code: str, level: int = None):
        """
        Request and parse item of a given sensor urn. Same as platform, but not limited to the second level identifier
        """
        parts = code.split(":")

        if len(parts) < 2:
            raise Exception(
                "Code is to short and cannot be resolved to a platform code."
            )

        base = ":".join(parts[0:level])
        url = (
            dws.SENSOR_BASE_URL
            + "/sensors/item/getItemByUrn/"
            + urllib.parse.quote_plus(base)
        )
        response = requests.get(url, stream=True)

        if response.status_code != 200:
            raise Exception("Error loading platform metadata.")

        j = json.loads(response.content)

        r = {
            "id": j["ID"],
            "code": j["urn"],
            "shortName": j["shortName"],
            "longName": j["longName"],
            "description": j["description"],
            "definition": j["rootItemType"]["vocableValue"]
            if "rootItemType" in j
            else j["subItemType"]["vocableValue"],
        }

        return r

    @staticmethod
    def get_events(code: str):
        """
        Request and parse measurement events for the deployed sensor

        :param code: sensor unique resource number (urn)
        :return: measurement events
        """
        # platform = dws.platform(code)
        base = dws.base(code)

        # assume the URL id is the same at SENSOR and DATA
        url = dws.SENSOR_BASE_URL + "/sensors/events/getDeviceEvents/" + str(base["id"])

        response = requests.get(url, stream=True)

        if response.status_code != 200:
            raise Exception("Error loading detailed platform metadata.")

        j = json.loads(response.content)

        uuid_map = {}
        dws._map_uuids(j, uuid_map)

        r = dws._parseEvents(j, uuid_map)
        base["events"] = r["items"]

        return base

    @staticmethod
    def _parseEvents(eventItems: list, uuid_map: dict = {}):
        """
        Similar to _parseItems, but to parse items that contain events and return a simplified item object.

        :param eventItems: a list of items that should contain the key 'event'
        :param uuid_map: a dictionary that maps the uniform unique indices returned from server by dws._map_uuids
        :return: dictionary of parsed items each corresponding to an event
        """
        items = []

        eventItems = [e["event"] for e in eventItems]

        for eventItem in eventItems:
            identifier = eventItem["ID"] if "ID" in eventItem else eventItem["id"]

            event_type = eventItem["eventType"]
            if isinstance(event_type, str):
                event_type = uuid_map[event_type]

            # possible keys: 'generalName', 'systemName'
            key = "generalName"
            vocable_value = event_type[key] if key in event_type else ""

            item = {
                "id": identifier,
                "startDate": eventItem["startDate"],
                "endDate": eventItem["endDate"],
                "label": eventItem["label"],
                "latitude": eventItem["latitude"],
                "longitude": eventItem["longitude"],
                "elevation": eventItem["elevationInMeter"],
                "vocable": vocable_value,
                "vocabulary": eventItem["vocabularyID"]
                if "vocabularyID" in eventItem
                else "",
            }

            items.append(item)

        r = {
            "items": items,
        }
        return r

    @staticmethod
    def get_geolocation(code: str, vocable_list=None):
        """
        Get geolocation of sensor when the corresponding event contains a corresponding vocable

        :param code: sensor unique reference number
        :param vocable_list: list of events that contain coordinates, e.g. ['Mount', 'Deployment', 'Information']
        :return: geolocation coordinates latitude, longitude, and elevation
        """

        events_list = dws.get_events(code)["events"]

        # subset by event type / vocable
        if vocable_list is not None:
            j = []
            for i in range(0, len(events_list)):
                if events_list[i]["vocable"] not in vocable_list:
                    j.append(i)
            for i in sorted(j, reverse=True):
                del events_list[i]

        # get the newest
        events_list = sorted(events_list, key=lambda i: i["endDate"], reverse=True)
        if len(events_list) == 0:
            return None, None, None
        else:
            latitude = events_list[0]["latitude"]
            longitude = events_list[0]["longitude"]
            elevation = events_list[0]["elevation"]

        return latitude, longitude, elevation

    
