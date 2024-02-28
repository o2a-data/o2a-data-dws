from _io import StringIO
import requests
import pandas as pd
import datetime as dt
from datetime import date
import json


class dws:
    """
    This script abstracts access to metadata stored in https://registry.o2a-data.de and data
    available via the data web service (https://dashboard.awi.de/data/).
    Have a look to the documentation at https://o2a-data.de/documentation and
    the API descriptions https://registry.o2a-data.de/api/ and https://dashboard.awi.de/data/api/.
    """

    ## ---------------------------  ¬!"£$%^&*()_+ --------------------------- ##
    def __init__(self):
        """
        lorem
        """
        self.REGISTRY = "https://registry.o2a-data.de/rest/v2"
        self.DWS = "https://dashboard.awi.de/data/rest"
        self.teststring = "https://registry.o2a-data.de/rest/v2/vocables/244"

    ## ---------------------------  ¬!"£$%^&*()_+ --------------------------- ##
    def _download(self, url):
        """
        auxiliary function
        :url: externally created string to call by this fun
        """
        response = requests.get(url)
        if response.status_code != 200:
            raise Exception("Error loading data.".format(response.reason))
        else:
            return json.loads(response.content)

    ## ---------------------------  ¬!"£$%^&*()_+ --------------------------- ##
    def test(self):
        #        print(self.REGISTRY)
        #        print(self.DWS)
        print(self.teststring)
        a = self._download(self.teststring)
        return a

    ## ---------------------------  ¬!"£$%^&*()_+ --------------------------- ##
    def items(self, pattern: str = None):
        """
        Loads availble sensors from the data service. The optional
        pattern allows * wildcards and can be used to search for sensors.

        See https://dashboard.awi.de/data-xxl/ for documentation.
        """
        url = self.DWS + "/sensors"
        if pattern != None:
            url += "?pattern=" + pattern

        j = self._download(url)
        return j

    ## ---------------------------  ¬!"£$%^&*()_+ --------------------------- ##
    def get(
        self,
        items,
        begin: date,
        end: date,
        aggregate: str = "hour",
        aggregateFunctions: str = "mean",
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
                self.DWS
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
                self.DWS
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


"""
code = "vessel:polarstern:pco2_go_ps:pre_xco"
s = "2024-02-19T00:00:00"
e = "2024-02-24T00:00:00"
agg = "hour"

# _download("https://registry.o2a-data.de/rest/v2/vocables/244")
a = dws()
a.test()
a.items(code)
a.get(items=code, begin=s, end=e)  # , aggregate = 'hour', aggregateFunctions = 'max')
"""

import sys

sys.exit()


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
    j = download(url)["records"]
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

    ## enrich event info
    for i in range(len(j)):
        if not isinstance(j[i]["type"], dict):
            j[i]["type"] = lut.get(j[i]["type"])

    return j


def contacts(code: int):
    """
    :code: registry id of item
    """
    url = REGISTRY + "/items/" + str(code) + "/contacts"

    j = download(url)["records"]

    contacts, roles = {}, {}
    for i in j:
        if isinstance(i["contact"], dict):
            contacts[i["contact"]["@uuid"]] = i["contact"]
        if isinstance(i["role"], dict):
            roles[i["role"]["@uuid"]] = i["role"]

    for i in range(len(j)):
        if not isinstance(j[i]["contact"], dict):
            j[i]["contact"] = contacts.get(j[i]["contact"])
        if not isinstance(j[i]["role"], dict):
            j[i]["role"] = roles.get(j[i]["role"])

    return j


def subitems(code: str):
    """
    retrieve subitems via base url and "*"
    :param code: item unique resource number (urn)
    :return: full json
    """
    url = REGISTRY + "/items?where=code=LIKE=" + code + ":*"
    j = download(url)["records"]
    return j


# a=subitems('vessel:mya_ii')
