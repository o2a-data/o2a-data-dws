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

# class dws:
"""
This script abstracts access to metadata stored in sensor.awi.de and data
available via the data web service (dws).
Have a look to the documentation at https://spaces.awi.de/display/DM and
the API descriptions https://sensor.awi.de/api/ and https://dashboard.awi.de/data-xxl/api/
"""

REGISTRY = "https://registry.o2a-data.de/rest/v2"
DWS = "https://dashboard.awi.de/data/rest"


def download(url):
    """
    auxiliary function
    """
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception("Error loading data.".format(response.reason))
    else:
        return json.loads(response.content)


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

    j = download(url)
    return j


# items('vessel:polarstern:pco2_go_ps:pre_xco')


@staticmethod
def get(
    items,
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

    if items.count(",") > 0:
        items = items.replace(" ", "").replace(",", "&sensors=")

    if aggregate.lower() == "second":
        response = requests.get(
            DWS
            + "/data?sensors="
            + items
            + "&beginDate="
            + str(begin)
            + "&endDate="
            + str(end)
            + "&aggregate="
            + aggregate
            + "&streamit=true&withQualityFlags=false&withLogicalCode=false"
        )
    elif (
        aggregate.lower() == "minute"
        or aggregate.lower() == "hour"
        or aggregate.lower() == "day"
    ):
        response = requests.get(
            DWS
            + "/data?sensors="
            + items
            + "&beginDate="
            + str(begin)
            + "&endDate="
            + str(end)
            + "&aggregate="
            + aggregate
            + "&aggregateFunctions="
            + aggregateFunctions
            ##+ '&limit=20'
            + "&streamit=true&withQualityFlags=false&withLogicalCode=false"
        )
    else:
        raise Exception(
            "No valid aggregate defined, use 'second', 'minute', 'hour', or 'day'."
        )

    if response.status_code != 200:
        raise Exception("Error loading data.".format(response.reason))

    # build the data frame
    df = pd.read_csv(StringIO(response.text), sep="\t")
    df["datetime"] = pd.to_datetime(df["datetime"])
    return df


## get(items, begin, end, aggregate, aggFun)


# @staticmethod
def item(code: str):  # , sys=None):
    """
    Request and parse item properties for a given item urn as "code"
    :param code: item unique resource number (urn)
    :param sys: switch for requesting at an alternative (under development) service ## not implemented yet
    :return: full json
    """
    ##        if sys == "dev": ## later
    url = REGISTRY + "/items?where=code=LIKE=" + code
    j = download(url)["records"][0]

    ## ITEM properties
    url = "https://registry.o2a-data.de/rest/v2/items/" + str(j["id"]) + "/properties"
    k = download(url)["records"]
    j["itemProperties"] = k

    return j


def parameters(code: int):
    """
    Request....
    """
    url = REGISTRY + "/items/" + str(code) + "/parameters"
    j = download(url)
    return j


def events(code: int, geo=False):
    """
    Requests all events of an item, returns as dict
    :code: registry id of item
    :geo: true == only with valid coordinates, false == all events
    """
    if geo is True:
        url = (
            REGISTRY
            + "/items/"
            + str(code)
            + "/events?where=latitude>=-90 and latitude<=90 and longitude>=-180 and longitude<=180"
        )
    else:
        url = REGISTRY + "/items/" + str(code) + "/events"

    j = download(url)["records"]

    ## create lookup table
    lut = {}
    for i in j:
        if isinstance(i["type"], dict):
            lut[i["type"]["@uuid"]] = i["type"]

    ## enrich even info
    for i in range(len(j)):
        if not isinstance(j[i]["type"], dict):
            j[i]["type"] = lut.get(j[i]["type"])

    return j


import sys

sys.exit()
