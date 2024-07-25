"""
+---------------------------------------------------------------------------+
|   /\        / /Alfred Wegener Institute
|  /__\  /\  / / Helmholtz Center for Polar and Marine Research
| /    \/  \/ /  Computing & Data Centre -- Data Logistics Support
+---------------------------------------------------------------------------+

@author: nanselm
contact: nanselm@awi.de || jejune@posteo.net

name: o2aAuth.py
purpose: class to create login token, e.g. for registry.o2a-data.de
comments: 
- API-token necessary -> creation at o2a-data.de
- local json required
- # no comment

date: 2024-07-02, rev. 2024-07-24
"""

## -----------------------------------
## LIBS
## -----------------------------------

import os
import json
import requests

## -----------------------------------
## VARIABLES
## -----------------------------------


class basicLogin:
    """
    class to  autheticate via token to o2a-services
    """

    ## ------------------------------
    def __init__(self, config=None, url="sandbox"):
        """well, init, you know"""
        ##
        if url == "sandbox":
            self.url = "https://registry.sandbox.o2a-data.de/rest/v2/"
            self.authurl = "https://registry.sandbox.o2a-data.de/rest/v2/auth/login"

        else:
            self.url = "https://registry.o2a-data.de/rest/v2/"
            self.authurl = "https://registry.o2a-data.de/auth/rest/login"
        ##
        self.config = None if config is None else config
        print(self.authurl)

    ## ------------------------------
    def _readToken(self):
        """aux fun to read local files"""
        if self.config is None:
            raise Exception("no config")
        else:
            with open(self.config) as f:
                token = json.loads(f.read())
            return token

    ## ------------------------------
    def getCookie(self):
        """
        returning cookie to autheticate POST/PUT/DELETE
        """
        x = requests.post(
            url=self.authurl,
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "accept": "*/*",
                ##"Authorization": "Bearer " + self._readToken()["token"],
            },
            data={
                "username": self._readToken()["email"],
                "password": self._readToken()["token"],
            },
        )
        if x.status_code == 200:
            return x.cookies["x-auth-token"]
        else:
            raise Exception(f"invalid: {x.status_code} != 200")


# eof
