from bs4 import BeautifulSoup  # library to parse HTML documents
from datetime import datetime
from getpass import getpass
import pandas as pd  # library for data analysis
import requests  # library to handle requests
import os
import pywikibot
from datetime import datetime


def prepare_to_wikidata(cases_df, name="cases"):
    last_column = cases_df.columns[-1]
    # Get only country-level data
    country_level_df = cases_df[["Country/Region", last_column]][
        cases_df["Province/State"].isna()
    ]

    # Check which countries have only an state-level measure and aggregate
    state_level = set(cases_df["Country/Region"]) - set(
        country_level_df["Country/Region"]
    )
    cases_df_to_aggregate = cases_df[["Province/State", "Country/Region", last_column]][
        [a in state_level for a in cases_df["Country/Region"]]
    ]

    cases_df_to_aggregate = cases_df_to_aggregate.groupby(["Country/Region"]).sum()
    cases_df_to_aggregate["Country/Region"] = cases_df_to_aggregate.index
    country_level_df = country_level_df.append(
        cases_df_to_aggregate, ignore_index=True, sort=False
    )

    # Change names for readability
    country_level_df.columns = ["country", name]

    # Fix names to match Wikidata and add outbreak identifiers

    local_outbreak_items = pd.read_csv("reference.csv")

    template_to_label = {
        "Bahamas": "The Bahamas",
        "Burma": "Myanmar",
        "Cabo Verde": "Cape Verde",
        "China": "mainland China",
        "Congo (Brazzaville)": "Republic of the Congo",
        "Congo (Kinshasa)": "Democratic Republic of the Congo",
        "Cote d'Ivoire": "Ivory Coast",
        "Czechia": "Czech Republic",
        "Gambia": "The Gambia",
        "Korea, South": "South Korea",
        "Sao Tome and Principe": "São Tomé and Príncipe",
        "US": "United States of America",
        "Taiwan*": "Taiwan",
        "Timor-Leste": "East Timor",
    }

    print(country_level_df["country"])
    country_level_df["country"] = country_level_df["country"].replace(
        template_to_label, regex=False
    )

    reconciled = country_level_df.merge(
        local_outbreak_items, left_on="country", right_on="countryLabel"
    ).drop_duplicates()

    # Check which entries were not reconciled
    print("Not reconciled:")
    print(set(country_level_df["country"]) - set(reconciled["country"]))

    return reconciled


print("====== Pulling cases ======")
cases_df = pd.read_csv(
    "https://github.com/CSSEGISandData/COVID-19/raw/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv"
)

last_column = cases_df.columns[-1]

reconciled_cases = prepare_to_wikidata(cases_df, name="cases")

print("====== Pulling deaths ======")
deaths_df = pd.read_csv(
    "https://github.com/CSSEGISandData/COVID-19/raw/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv"
)

reconciled_deaths = prepare_to_wikidata(deaths_df, name="deaths")


reconciled = reconciled_cases.merge(reconciled_deaths).drop_duplicates()


print("====== Running bot for non-manually updated items ======")

manual_list = []

# - COVID-19 pandemic in Indonesia (Q86913546)
#
# Current mantainer: ?
# See: https://www.wikidata.org/wiki/User_talk:TiagoLubiana#Destructive_edit

indonesia = "Q86913546"
manual_list.append(indonesia)

# - 2020 COVID-19 pandemic in Luxembourg (Q87250860)
#
# Current mantainer: https://www.wikidata.org/wiki/User:Sultan_Edijingo
# See: https://www.wikidata.org/wiki/User_talk:TiagoLubiana#Destructive_edit
# and  https://www.wikidata.org/wiki/Wikidata:Requests_for_permissions/Bot/CovidDatahubBot#Q87250860%3A_destructive_edits

luxembourg = "Q87250860"
manual_list.append(luxembourg)


# - COVID-19 pandemic in Taiwan (Q84081307)
#
# Current mantainer: https://www.wikidata.org/wiki/User:Daxipedia
# See: https://www.wikidata.org/wiki/Wikidata:Requests_for_permissions/Bot/CovidDatahubBot#Q87250860%3A_destructive_edits

taiwan = "Q84081307"
manual_list.append(taiwan)


# - 2020 COVID-19 pandemic in Hungary (Q87119811)
#
# Current mantainer: https://www.wikidata.org/wiki/User:Bencemac
# See: https://www.wikidata.org/wiki/Wikidata:Requests_for_permissions/Bot/CovidDatahubBot#Q87250860%3A_destructive_edits

hungary = "Q87119811"
manual_list.append(hungary)

# - 2020 COVID-19 pandemic in Colombia (Q87483673)
#
# Current mantainer: https://www.wikidata.org/wiki/User:Juli%C3%A1n_L._P%C3%A1ez
# See: https://www.wikidata.org/wiki/User_talk:TiagoLubiana#CovidDatahubBot_request

colombia = "Q87483673"
manual_list.append(colombia)

# - COVID-19 pandemic in India (Q84055514)
#
# Current mantainer: https://www.wikidata.org/wiki/User:Dellux_mkd
# See: https://www.wikidata.org/wiki/Wikidata:Requests_for_permissions/Bot/CovidDatahubBot#Q87250860%3A_destructive_edits

india = "Q84055514"
manual_list.append(india)

last_column_cases = cases_df.columns[-1]
last_column_deaths = deaths_df.columns[-1]

if last_column_cases != last_column_deaths:
    raise Exception("Last columns of cases and deaths are different")

date_of_data = datetime.strptime(last_column_cases, "%m/%d/%y")


site = pywikibot.Site("wikidata", "wikidata")
repo = site.data_repository()

for i, row in reconciled.iterrows():

    try:
        print(row["item"])

        # Skip updates for items that are manually maintained
        if row["item"] in manual_list:
            continue

        item = pywikibot.ItemPage(repo, row["item"])
        item.get()  # Fetch all page data, and cache it.

        # Change rank of current deaths to normal
        # (code based on https://gist.github.com/urbanecm/f0b2bb95514f322207d04908cc4d8143)
        itemData = item.get()
        death_claims = itemData["claims"].get("P1120", [])
        for claim in death_claims:
            if claim.rank != "preferred":
                continue
            claim.setRank("normal")
            item.editEntity(
                {"claims": [claim.toJSON()]}, summary="Change P1120 rank to normal"
            )

        # Add number of deaths

        ## Value --> death count

        deaths_claim = pywikibot.Claim(
            repo, u"P1120"
        )  # Adding number of deaths (P1120)
        deaths_claim.setTarget(
            pywikibot.WbQuantity(row["deaths"])
        )  # Set the target value in the local object.
        deaths_claim.setRank("preferred")
        item.addClaim(deaths_claim, summary=u"Adding death count from template")

        ## Qualifier --> date

        qualifier = pywikibot.Claim(
            repo, u"P585"
        )  # Adding qualifier of point in time (P585)
        target = pywikibot.WbTime(
            year=int(date_of_data.strftime("%Y")),
            month=int(date_of_data.strftime("%m")),
            day=int(date_of_data.strftime("%d")),
        )
        qualifier.setTarget(target)
        deaths_claim.addQualifier(qualifier, summary=u"Adding date.")

        ## Source --> Wikipedia template URL
        wiki_url = pywikibot.Claim(repo, u"P854")  # reference URL (P854)
        wiki_url.setTarget("https://github.com/CSSEGISandData/COVID-19")

        ## Source --> Reference of the article that describes the data
        # This is a request on the GitHub repository of the datasource
        ref = pywikibot.Claim(repo, u"P248")  # stated in (P248)
        ref.setTarget(pywikibot.ItemPage(repo, "Q87456354"))

        ## Source --> Retrieved today
        today = datetime.today()  # Date today
        retrieved = pywikibot.Claim(
            repo, u"P813"
        )  # retrieved (P813). Data type: Point in time
        retrieved_target = pywikibot.WbTime(
            year=int(today.strftime("%Y")),
            month=int(today.strftime("%m")),
            day=int(today.strftime("%d")),
        )  # retrieved -> %DATE TODAY%. Example retrieved -> 29.11.2020
        retrieved.setTarget(retrieved_target)  # Inserting value

        deaths_claim.addSources([ref, wiki_url, retrieved], summary=u"Adding sources.")

        # Change rank of current deaths to normal
        # (code based on https://gist.github.com/urbanecm/f0b2bb95514f322207d04908cc4d8143)
        cases_claim = itemData["claims"].get("P1603", [])
        for claim in cases_claim:
            if claim.rank != "preferred":
                continue
            claim.setRank("normal")
            item.editEntity(
                {"claims": [claim.toJSON()]}, summary="Change P1603 rank to normal"
            )

        # Add number of cases

        ## Value --> case count

        cases_claim = pywikibot.Claim(repo, u"P1603")  # Adding number of cases (P1603)
        cases_claim.setTarget(
            pywikibot.WbQuantity(row["cases"])
        )  # Set the target value in the local object.
        cases_claim.setRank("preferred")
        item.addClaim(cases_claim, summary=u"Adding case count from template")

        ## Qualifier --> date

        qualifier = pywikibot.Claim(
            repo, u"P585"
        )  # Adding qualifier of point in time (P585)
        target = pywikibot.WbTime(
            year=int(date_of_data.strftime("%Y")),
            month=int(date_of_data.strftime("%m")),
            day=int(date_of_data.strftime("%d")),
        )
        qualifier.setTarget(target)
        cases_claim.addQualifier(qualifier, summary=u"Adding date.")

        ## Source --> Wikipedia template URL
        wiki_url = pywikibot.Claim(repo, u"P854")  # reference URL (P854)
        wiki_url.setTarget("https://github.com/CSSEGISandData/COVID-19")

        ## Source --> Reference of the article that describes the data
        # This is a request on the GitHub repository of the datasource
        ref = pywikibot.Claim(repo, u"P248")  # stated in (P248)
        ref.setTarget(pywikibot.ItemPage(repo, "Q87456354"))

        ## Source --> Retrieved today
        today = datetime.today()  # Date today
        retrieved = pywikibot.Claim(
            repo, u"P813"
        )  # retrieved (P813). Data type: Point in time
        retrieved_target = pywikibot.WbTime(
            year=int(today.strftime("%Y")),
            month=int(today.strftime("%m")),
            day=int(today.strftime("%d")),
        )  # retrieved -> %DATE TODAY%. Example retrieved -> 29.11.2020
        retrieved.setTarget(retrieved_target)  # Inserting value

        cases_claim.addSources([ref, wiki_url, retrieved], summary=u"Adding sources.")

    except Exception as e:
        print(e)

