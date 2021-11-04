import re
from typing import List

import requests
import pandas as pd
from datetime import datetime
import bs4
from bs4 import BeautifulSoup
from typing import Tuple, Union

BASE_URL = "https://www.marketbeat.com"
DATE_FORMAT = '%Y-%m-%d'


def getSoup(url: str) -> bs4.BeautifulSoup:
    """Retrieve page source and create BeautifulSoup object
    :param url: MarketBeat page URL
    :return: BeautifulSoup object
    """
    source = requests.get(url).text
    return BeautifulSoup(source, features="html.parser")


def readTable(url: str, search_string=None) -> Tuple[list, list]:
    """Read HTML table and return as list of dicts containing bs4 Tags
    :param url: MarketBeat page URL
    :param search_string: optional filter string to search in <h2> header tag
    :return: tuple containing list of header Tag objects and a list of lists of table data Tag objects
    """
    # Retrieve page source
    soup = getSoup(url)

    # If no search string, just look for the first table, otherwise, look in h2
    if search_string is None:
        root = soup.find("table")
    else:
        root = soup.find("h2", string=re.compile('.*{}.*'.format(search_string))).findNext("table")

    # Rows of this class are advertisements, remove them
    for junk in root.find_all("tr", {"class": "bottom-sort"}):
        junk.decompose()

    # Get header and row data
    thead = root.find("thead")
    tbody = root.find("tbody")
    headers = list(thead.find_all("th"))
    data = []
    for tr in tbody.find_all("tr"):
        data.append(list(tr.find_all("td")))

    # Ensure data consistency
    n_headers = len(headers)
    for i, row in enumerate(data):
        if len(row) != n_headers:
            print("Error with row: {}".format(i))

    # Clean up the DataFrame
    return headers, data, soup


def buildDataFrame(data: List[dict]) -> pd.DataFrame:
    """Create DataFrame from list of dictionaries.
    :param data: list of dictionaries containing rating data
    :return DataFrame containing analyst ratings data
    """
    df = pd.DataFrame(data)
    df.drop(['brokerage_code'], axis=1, errors='ignore', inplace=True)
    index = df[['date', 'symbol', 'brokerage']].apply(lambda row: '_'.join(row.values.astype(str)), axis=1)
    df.insert(0, 'uid', index)
    return df


def parseSymbolTag(tag: bs4.element.Tag) -> Tuple[str, str, str]:
    """Parse symbol/company tag.
    :param tag: html table data element
    :return: tuple containing (symbol, company, exchange)
    """
    symbol = tag.find("div", {"class": "ticker-area"}).text
    exchange = tag.find("a").get("href", default="").split('/')[2].strip()
    company = tag.find("div", {"class": "title-area"}).text
    return symbol, company, exchange


def parseBrokerageTag(tag: bs4.element.Tag) -> Tuple[str, Union[str, int]]:
    """Parse brokerage tag. The returned brokerage_code can be either then numeric or
    string version.
    :param tag: html table data element
    :return: tuple containing (brokerage, brokerage code)
    """
    # Getting brokerage code from href string similar to
    # /ratings/by-issuer/34/ OR /ratings/by-issuer/morgan-stanley-stock-recommendations
    href = tag.find("a").get("href")
    brokerage_code = re.match('/ratings/by-issuer/([-\w]+)', href).groups()[0].replace("-stock-recommendations", "")
    brokerage = tag.find("a").text
    return brokerage, brokerage_code


def parseAnalystTag(tag: bs4.element.Tag) -> str:
    """Parse analyst tag.
    :param tag: html table data element
    :return: analyst name
    """
    # Analyst name - should be first "a" section
    analyst_attr = tag.find("a")
    analyst = analyst_attr.text.strip() if analyst_attr is not None else None
    return analyst


def parsePriceTargetTag(tag: bs4.element.Tag) -> float:
    """Parse price target tag.
    :param tag: html table data element
    :return: price target
    """
    pt = tag.text.split(u"\u279D")[-1]
    price_target = float(pt.replace('$', '').replace(',', '').strip())
    return price_target


def parseRatingTag(tag: bs4.element.Tag) -> Tuple[str, int]:
    """Parse rating tag. If rating is not present, will return ("", -1)
    :param tag: html table data element
    :return: tuple containing (rating, rating code)
    """
    rating = tag.text.split(u"\u279D")[-1].strip()
    rating_code = int(tag.get("data-sort-value", default="-1"))
    return rating, rating_code


def parseDateTag(tag: bs4.element.Tag) -> str:
    """Parse date tag.
    :param tag: html table data element
    :return: date string
    """
    date = datetime.strptime(tag.text, '%m/%d/%Y').strftime(DATE_FORMAT)
    return date


def getDailyRatingsTable() -> pd.DataFrame:
    """Get current analyst ratings from MarketBeat
    :return: DataFrame containing analyst ratings for multiple symbols from today
    """

    def processRow() -> dict:
        # Column 0 - Ticker
        symbol, company, exchange = parseSymbolTag(row[0])

        # Column 1 - Action
        action = row[1].text.strip()

        # Column 2 - Brokerage
        brokerage, brokerage_code = parseBrokerageTag(row[2])

        # Column 3 - Analyst
        analyst = parseAnalystTag(row[3])

        # Column 5 - Price Target
        price_target = parsePriceTargetTag(row[5])

        # Column 6 - Rating
        rating, rating_code = parseRatingTag(row[6])

        return {
            "symbol": symbol,
            "date": date,
            "exchange": exchange,
            "action": action,
            "brokerage": brokerage,
            "brokerage_code": brokerage_code,
            "analyst": analyst,
            "price_target": price_target,
            "rating": rating,
            "rating_code": rating_code
        }

    # END processRow ----------------------------

    # Read table data, we don't need the headers
    url = BASE_URL + "/ratings/us/"
    _, data, soup = readTable(url)

    # Get date from page source, prefer to use date from page if possible
    DATE_RE = re.compile('\((\d{1,2}/\d{1,2}/\d{4})\)')
    date = datetime.today().strftime(DATE_FORMAT)
    tag = soup.find("div", string=DATE_RE)
    if tag is not None:
        date = DATE_RE.search(tag.text).groups()[0]
        date = datetime.strptime(date, '%m/%d/%Y').strftime(DATE_FORMAT)

    # Process each row and store
    row_data = []
    for row in data:
        try:
            row_data.append(processRow())
        except Exception:
            pass
    return buildDataFrame(row_data)


def getSymbolRatingsTable(symbol: str) -> pd.DataFrame:
    """Get historical analyst ratings for a single symbol from MarketBeat
    :return: DataFrame containing analyst ratings for a single symbol
    """

    def processRow() -> dict:

        # Column 0 - Date
        date = parseDateTag(row[0])

        # Column 1 - Brokerage
        brokerage, brokerage_code = parseBrokerageTag(row[1])

        # Column 2 - Analyst
        analyst = parseAnalystTag(row[2])

        # Column 3 - Action
        action = row[3].text.strip()

        # Column 4 - Rating
        rating, rating_code = parseRatingTag(row[4])

        # Column 5 - Price Target
        price_target = parsePriceTargetTag(row[5])

        return {
            "symbol": symbol,
            "date": date,
            "exchange": exchange,
            "action": action,
            "brokerage": brokerage,
            "brokerage_code": brokerage_code,
            "analyst": analyst,
            "price_target": price_target,
            "rating": rating,
            "rating_code": rating_code
        }

    # END processRow ----------------------------

    # Get main page to determine exchange
    # FIXME, make this a little cleaner
    main_url = BASE_URL + "/stocks/NASDAQ/{}".format(symbol)
    soup = getSoup(main_url)
    href = soup.find("li", {"id": "liAnalystRatings"}).find("a").get("href")

    # Get exchange from href
    exchange = re.match('/stocks/(\w+)', href).groups()[0]

    # Read table data, we don't need the headers
    url = BASE_URL + href
    _, data, _ = readTable(url, search_string="Ratings History")

    # Process each row and store
    row_data = []
    for row in data:
        try:
            row_data.append(processRow())
        except Exception:
            pass
    return buildDataFrame(row_data)
