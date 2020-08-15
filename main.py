import aiohttp
from aiohttp import ClientSession
import asyncio

from bs4 import BeautifulSoup
import pandas as pd

from collections import defaultdict
import json

# CONTANT VARIABLE
total_case = 4360 # number of case you would like to check, start from case 1
domain_url = "https://wars.vote4.hk/page-data/cases/"
domain_suffix = "/page-data.json"


def list_of_covid_url(total_case):
    """Generate the list of url to fetch the information for all patients."""
    list_url = []
    for i in range(1, total_case+1):
        url = domain_url + str(i) + domain_suffix
        list_url.append(url)
        
    return list_url

async def get_covid_json(url, session):
    """Fetch the json of a patient"""
    async with session.get(url) as response:
        if response.reason == "OK":
            return await response.json()
        else:
            print('fuxk up', response.status)
        

def extract_data(case_json):
    """Extract useful information from the payload.
    
    group_len (int): Size of the infection group
    group_name (str): Name of the infection group
    related_cases (list): List of related cases in the same infection group
    confirmation_date (str): Date of infection
    classification (str): type of the infection origin (local / imported)
    """
    # check if the patient is in any group
    try:
        group = case_json["result"]["pageContext"]["node"]["groups"]
    except Excpetion as e:
        print(e)
        print(case_json)
        return

    if group:
        group_json = group[0]
        case_number = case_json["result"]["pageContext"]["node"]["case_no"]
        confirm_date = case_json["result"]["pageContext"]["node"]["confirmation_date"]

        related_case = group_json["related_cases"].split(",")
        group_len = len(related_case)

        group_dict[case_number]["group_len"] = group_len
        group_dict[case_number]["group_name"] = group_json["name_zh"]
        group_dict[case_number]["related_cases"] = group_json["related_cases"].split(",")
        group_dict[case_number]["confirmation_date"] = confirm_date
        group_dict[case_number]["classification"] = case_json["result"]["pageContext"]["node"]["classification"]

        
async def pipeline(url, session):
    """main function to fetch the url and extract the data."""
    case_json = await get_covid_json(url, session)
    extract_data(case_json)


async def run_web_scrapping():
    # Get information for all cases from https://wars.vote4.hk/cases
    async with ClientSession() as session:
        await asyncio.gather(*[pipeline(url, session) for url in list_url])

if __name__ == "__main__":
    list_url = list_of_covid_url(total_case)

    # all the data will be stored in group_dict
    group_dict = defaultdict(dict)
    # line 79-80 will run the web scrapping process
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run_web_scrapping())

    # Instantiate some lists which represent each of the column in the final output
    """
    case_number: the number of the current case (index)
    max_related_group_len: Max size of related infection group, see the comment below
    is_origin: True if the current infection group occurs by itself, see the comment below
    group_names: Name of the group as declared by the HK gvnt
    confirm_dates: Date of infection
    classification: Type of infection (local / imported)
    """
    case_numbers = []
    max_related_group_len = []
    is_origin = []
    group_names = []
    confirm_dates = []
    classification = []

    for key, value_dict in group_dict.items():
        """For each case, check all the related cases in the same infection group

        if the related cases are infected from other (and bigger) infection group,
        we will link this patient to that particular infection group.
        It means that the current infection group is in fact originated from other big infection group.

        If the infection group is originated by itself, origin = True
        If the infection group is infected by other (and bigger) infection group, origin = False
        """
        if not value_dict:
            continue

        max_group_len = value_dict["group_len"]
        origin = True
        for case_number in value_dict["related_cases"]:
            cur_group_len = group_dict[case_number.strip()]["group_len"]
            if cur_group_len > max_group_len:
                max_group_len = cur_group_len
                origin = False

        case_numbers.append(key)
        max_related_group_len.append(max_group_len)
        is_origin.append(origin)
        group_names.append(value_dict["group_name"])
        confirm_dates.append(value_dict["confirmation_date"])
        classification.append(value_dict["classification"])

    df = pd.DataFrame({
        'case number': case_numbers,
        'max group size': max_related_group_len,
        'original group': is_origin,
        'group name': group_names,
        'confirm dates': confirm_dates,
        'classification': classification
    })

    df["confirm dates"] = pd.to_datetime(df["confirm dates"], format="%Y-%m-%d")

    # filter only local cases
    local_case = df[df["classification"]!= "imported"]

    # count the number of infection cases per each infection group size
    local_case.groupby(by="max group size")["original group"].count().to_frame().to_excel("input.xlsx")
