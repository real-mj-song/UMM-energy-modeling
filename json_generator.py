import requests
import json
import collections
from ConfigParser import SafeConfigParser

"""
Python script to create a json file whose data is queried from OpenTSDB.
This is assumed to be executed at 12am (midnight) everyday by cronjob.

#### Sample json data: ####
[{"metric": "energy",
  "tags":{"source":"GDSP1"},
  "aggregateTags":[],
  "dps": {"1477299600": -19.6783418418743,
          "1477418400": 630.3984611062651,
          "1477407600": 247.08119177994905,
          "1477353600": 142.97691587907298}}]

#### Desired json data: ####
{
    "cols": [
        {"id":"","label":"Date","pattern":"","type":"date"},
        {"id":"","label":"Green Dorm Solarpanel","pattern":"","type":"number"},
        {"id":"","label":"title","pattern":"","type":"string"},
        {"id":"","label":"text","pattern":"","type":"string"}
    ],
    "rows": [
        {"c":[{"v":1477299600000},{"v":12400},{"v":undefined},{"v":undefined}]},
        {"c":[{"v":1477418400000},{"v":24045},{"v":undefined},{"v":undefined}]},
        {"c":[{"v":1477407600000},{"v":35022},{"v":undefined},{"v":undefined}]},
        {"c":[{"v":1477353600000)},{"v":0},{"v":undefined},{"v":undefined}]}
    ]
}
"""

col_template = [
    {"id":"","label":"Date","pattern":"","type":"date"},
    {"id":"","label":"Green Dorm Solar Panel Production","pattern":"","type":"number"},
    {"id":"","label":"title","pattern":"","type":"string"},
    {"id":"","label":"text","pattern":"","type":"string"}]


def main():
    # raw json data queried from the database
    raw_data = get_raw_data()

    pure_data = data_decoder(raw_data)

    # an empty data dict
    data = {"cols": col_template, "rows": []}

    # modify the json `data` object with raw_data
    modify_json(data, pure_data)

    # create/rewrite `data.json` file that contains the `data` dict
    with open('data.json', 'w') as outfile:
        json.dump(data, outfile)


def get_raw_data():
    """
    Returns raw data which is queried from the database
    """
    parser = SafeConfigParser()
    parser.read('config.ini')
    url = parser.get('DEFAULT', 'url')

    response = requests.get(url + '/api/query?start=3d-ago&m=sum:1h-avg:rate:energy{source=GDSP1|GDSP2|GDSP3}')
    raw_data = response.json()
    response.close()

    return raw_data


def data_decoder(qlist):
    """
    Takes a list of dicts (query results) and returns the extracted and
    combined pure data (all dicts combined). For instance, this method
    can be used to combine three green dorm solar panel "dps" values
    """
    pure_data = {}
    for key in qlist[0]["dps"]:
        # initialize the entry with 0
        pure_data[key] = 0
        # iterate over each tag dict
        for i in range(len(qlist)):
            pure_data[key] = pure_data[key] + qlist[i]["dps"][key]

    return pure_data


def modify_json(data, pure_data):
    """
    Takes a json format `data` and a dict `pure_data` and inserts values from
    pure_data into data that is applicable to Google Charts
    """
    ordered_pure_data = collections.OrderedDict(sorted(pure_data.items()))

    for key, val in ordered_pure_data.iteritems():
        # convert the number of seconds to the number of miliseconds
        entry = {"c":[{"v":int(key) * 1000},{"v":val},{"v":None},{"v":None}]}
        data["rows"].append(entry)


if __name__ == "__main__":
    main()
