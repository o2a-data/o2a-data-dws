from _io import StringIO
import requests
import re
import pandas as pd
from datetime import date
import json


class o2a:
    """
    This script abstracts access to metadata stored in
    https://registry.o2a-data.de and data available via the data web service
    (https://dashboard.awi.de/data/). Have a look to the documentation at
    https://o2a-data.de/documentation and the API descriptions
    https://registry.o2a-data.de/api/ and https://dashboard.awi.de/data/api/.
    """

    ## ---------------------------  ¬!"£$%^&*()_+ --------------------------- ##
    def __init__(self):
        """
        lorem
        """
        self.REGISTRY = "https://registry.o2a-data.de/rest/v2"
        self.STREAMS = "https://ingest.o2a-data.de/rest"

    ## ---------------------------  ¬!"£$%^&*()_+ --------------------------- ##
    def help(self):
        """ """
        print("**DWS**")
        print(
            "- x.items: retrieves info about a specific item if 'code'\n (item urn) is submitted otherwise it gives a de facto unmanagable\n data stream to std out. Info is derived from the data web service."
        )
        print(
            "- x.downloadDataFromDWS: retrieves data from data web service \ninput: i) string of parameter urns, multiple urns can be combined by comma, ii) beginning date (YYYY-MM-DDTHH:MM:SS), iii) end date (YYYY-MM-DDTHH:MM:SS), iv) aggregate level (second, minute, hour, day), defaults to 'hour', iv) aggregate function (min, max, mean, median, count, std), does not apply for 'seconds', defaults to mean"
        )
        print("")
        print("**REGISTRY**")
        print(
            "- x.item: full json of item (incl. item properties)\ninput: item ID or urn"
        )
        print("- x.contacts: full json of item contacts\ninput: item ID or urn")
        print(
            "- x.events: full json list of events \ninput: item ID or urn, optional 'geo = True' only events with coordinates are put out"
        )
        print("- x.subitems: output of all subitems \ninput: item ID or urn")
        print("- x.parameters: gives all parameters per item \ninput: item ID or urn")
        print(
            "- x.resources: return list of available resources\ninput: item ID or urn"
        )

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
    def _testAggregate(self, pattern, string):
        """
        :pattern: aggretation pattern to check on
        :string: string to be tested
        """
        p = "[" + pattern + "]\\" + "w+"
        a = re.match(p, string.upper())
        if a is None:
            return False
        else:
            return True

    ## ---------------------------  ¬!"£$%^&*()_+ --------------------------- ##
    def items(self, pattern: str = None):
        """
        Loads availble sensors from the data service. The optional
        pattern allows * wildcards and can be used to search for sensors.
        See https://dashboard.awi.de/data/ for documentation.
        :pattern: is parameter urn(s)
        """
        url = self.DWS + "/sensors"
        if pattern != None:
            url += "?pattern=" + pattern

        j = self._download(url)
        return j

    ## ---------------------------  ¬!"£$%^&*()_+ --------------------------- ##
    def downloadDataFromStreams(
        self,
        items,
        begin: date,
        end: date,
        aggregate: str = "HOUR",
        aggregateFunctions: str = "MEAN",
    ):
        """
        Loads data from the data service for given sensors
        in the given time range and selected aggregate.
        See https://ingest.o2a-data.de/api/ for documentation.
        :items: parameter urn(s)
        :begin: YYYY-MM-DDTHH:MM:SS string
        :end: YYYY-MM-DDTHH:MM:SS string
        :aggregate: second, minute, hour, day
        :aggregateFunctions: MIN, Q25, Q75, MAX, MEAN, MEDIAN, STDDEV, VAR, N, SUM, ...
        """
        if items == None or len(items) == 0:
            raise Exception("Item(s) must be defined.")

        if begin == None:
            raise Exception("datetimeMin must be defined.")

        if end == None:
            raise Exception("datetimeMax must be defined.")

        if items.count(",") > 0:
            items = items.replace(" ", "").replace(",", "&scodes=")

        secondTest = self._testAggregate("SECOND", aggregate)
        minTest = self._testAggregate("MINUTE", aggregate)
        hourTest = self._testAggregate("HOUR", aggregate)
        dayTest = self._testAggregate("DAY", aggregate)

        baseLink = (
            self.STREAMS
            + "/data?codes="
            + items
            + "&datetimeMin="
            + str(begin)
            + "&datetimeMax="
            + str(end)
            + "&aggregate="
        )

        if secondTest is True:
            response = requests.get(
                baseLink
                + "SECOND" 
            )

        if minTest is True:
            response = requests.get(
                baseLink
                + "MINUTE&aggregateFunctions="
                + aggregateFunctions
            )

        if hourTest is True:
            response = requests.get(
                baseLink
                + "HOUR&aggregateFunctions="
                + aggregateFunctions
            )

        if dayTest is True:
            response = requests.get(
                baseLink
                + "DAY&aggregateFunctions="
                + aggregateFunctions
            )

        if response.status_code != 200:
            raise Exception("Error loading data.".format(response.reason))
        
        # build the data frame
        df = pd.read_csv(StringIO(response.text), sep="\t")
        df["datetime"] = pd.to_datetime(df["datetime"])
        return df

    ## ---------------------------  ¬!"£$%^&*()_+ --------------------------- ##
    def item(self, code):
        """
        Request and parse item properties for a given item urn as "code"
        :code: item unique resource number (urn) or ID
        """
        ##        if sys == "dev": ## later

        if type(code) == str:
            url = self.REGISTRY + "/items?where=code=LIKE=" + code
            j = self._download(url)["records"][0]
        elif type(code) == int:
            url = self.REGISTRY + "/items/" + str(code)
            j = self._download(url)
        else:
            raise Exception("provide item urn or item ID")

        ## ITEM properties
        url = self.REGISTRY + "/items/" + str(j["id"]) + "/properties"
        k = self._download(url)["records"]
        j["itemProperties"] = k

        return j

    ## ---------------------------  ¬!"£$%^&*()_+ --------------------------- ##
    def parameters(self, code):
        """
        Request....
        :code: item ID or urn
        """
        item = self.item(code)
        if type(code) == str:
            code = item["id"]
        elif type(code) == int:
            code = code
        else:
            raise Exception("provide item urn or item ID")

        url = self.REGISTRY + "/items/" + str(code) + "/parameters"
        k = self._download(url)["records"]
        for i in range(len(k)):
            k[i]["urn"] = item["code"] + ":" + k[i]["shortName"]
        ##
        return k

    ## ---------------------------  ¬!"£$%^&*()_+ --------------------------- ##
    def events(self, code, geo=False):
        """
        Requests all events of an item, returns as dict
        :code: registry id of item
        :geo: true == only with valid coordinates, false == all events
        """
        if type(code) == str:
            item = self.item(code)
            code = item["id"]
        elif type(code) == int:
            code = code
        else:
            raise Exception("provide item urn or item ID")

        if geo is True:
            url = (
                self.REGISTRY
                + "/items/"
                + str(code)
                + "/events?where=latitude>=-90 and latitude<=90 and"
                + "longitude>=-180 and longitude<=180"
            )
        else:
            url = self.REGISTRY + "/items/" + str(code) + "/events"

        j = self._download(url)["records"]

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

    ## ---------------------------  ¬!"£$%^&*()_+ --------------------------- ##
    def contacts(self, code):
        """
        requests contacts per item
        :code: registry id or urn of item
        """
        if type(code) == str:
            item = self.item(code)
            code = item["id"]
        elif type(code) == int:
            code = code
        else:
            raise Exception("provide item urn or item ID")

        url = self.REGISTRY + "/items/" + str(code) + "/contacts"

        j = self._download(url)["records"]

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

    ## ---------------------------  ¬!"£$%^&*()_+ --------------------------- ##
    def subitems(self, code):
        """
        retrieve subitems via base url and "*"
        :code: item unique resource number (urn) or ID
        """
        if type(code) == str:
            item = self.item(code)
            code = item["id"]
        elif type(code) == int:
            code = code
        else:
            raise Exception("provide item urn or item ID")

        url = self.REGISTRY + "/items?where=parent.id==" + str(code)
        j = self._download(url)["records"]

        types = {}
        for i in j:
            if isinstance(i["type"], dict):
                types[i["type"]["@uuid"]] = i["type"]

        for i in range(len(j)):
            if not isinstance(j[i]["type"], dict):
                j[i]["type"] = types.get(j[i]["type"])
        return j

    ## ---------------------------  ¬!"£$%^&*()_+ --------------------------- ##
    def resources(self, code):
        """
        retrieve resources of item
        :code: item unique resource number (urn) or ID
        """
        if type(code) == str:
            url = self.REGISTRY + "/items?where=code=LIKE=" + code
            j = self._download(url)["records"][0]
        elif type(code) == int:
            url = self.REGISTRY + "/items/" + str(code)
            j = self._download(url)
        else:
            raise Exception("provide item urn or item ID")

        ## ITEM resources
        url = self.REGISTRY + "/items/" + str(j["id"]) + "/resources"
        k = self._download(url)["records"]

        resourceIds = [i["id"] for i in k]
        ##
        c = []
        for i in resourceIds:
            url = self.REGISTRY + "/items/" + str(j["id"]) + "/resources/" + str(i)
            l = self._download(url)
            d = {}
            d["name"] = l["name"]
            d["type"] = l["type"]["generalName"]
            d["description"] = l["description"] if "description" in l.keys() else ""
            d["link"] = l["linkage"] if "linkage" in l.keys() else url + "/payload"

            c.append(d)

        return c

    ## ---------------------------  ¬!"£$%^&*()_+ --------------------------- ##






## eof
