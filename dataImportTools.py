import urllib.request
import requests
import json
import psycopg2
from psycopg2.extras import DictCursor, RealDictCursor
from bs4 import BeautifulSoup as bs
from bs4 import NavigableString
import time
import datetime
import re

verbose = False

def connect_db():
    conn = psycopg2.connect("dbname=nrhp user=xyzzy", cursor_factory=DictCursor)
    return conn

# from https://stackoverflow.com/a/312466/719690
def IterChunks(sequence, chunk_size):
    res = []
    for item in sequence:
        res.append(item)
        if len(res) >= chunk_size:
            yield res
            res = []
    if res:
        yield res  # yield the last, incomplete, portion

def grab_weekly_updates_17():
    root = 'https://www.nps.gov/nr/'
    start_url = root + '2017nrlist.htm'
    start_page = requests.get(start_url).text
    soup = bs(start_page, features="html.parser")
    art = soup.findAll('div', class_='article')[0]
    table = art.contents[3].tbody
    with connect_db() as conn:
        cursor = conn.cursor()
        for row in table.children:
            try:
                columns = row.find_all('td')
                link_col = columns[1]
                link = link_col.a.attrs['href']
                grab_one_weekly_update(root+link, cursor)
            except:
                continue
        cursor.close()

def grab_latest_weekly_update():
    root = 'https://www.nps.gov'
    start_url = root + '/subjects/nationalregister/weekly-list.htm'
    start_page = requests.get(start_url).text
    soup = bs(start_page, features="html.parser")

    with connect_db() as conn:
        cursor = conn.cursor()
        for row in soup.findAll('div', class_='ArticleTextGroup')[3].children:
            try:
                link = row.attrs['href']
                print(link)
                print(root + link)
                grab_one_weekly_update(root+link, cursor)
            except:
                print(row)
                continue
            break
        cursor.close()

def grab_weekly_updates_early2019():
    root = 'https://www.nps.gov'
    start_url = root + '/subjects/nationalregister/weekly-list.htm'
    start_page = requests.get(start_url).text
    soup = bs(start_page, features="html.parser")

    with connect_db() as conn:
        cursor = conn.cursor()
        for row in soup.findAll('div', class_='ArticleTextGroup')[3].children:
            print(row)
            try:
                link = row.attrs['href']
                print(link)
                print(root + link)
                grab_one_weekly_update(root+link, cursor)
            except:
                print("exception: ", row)
                continue
        cursor.close()

def test_weekly_updates_url(limit=3):
    root = 'https://www.nps.gov'
    start_url = root + '/subjects/nationalregister/weekly-list.htm'
    start_page = requests.get(start_url).text
    soup = bs(start_page, features="html.parser")

    with connect_db() as conn:
        count = 0
        cursor = conn.cursor()
        rows = soup.find_all('div', class_='ArticleTextGroup')[3]
        for row in rows.find_all('a'):
            print("----")
            link = row.attrs['href']
            print(link)
            print(root + link)
            count += 1
            print("count", count, limit)
            if count >= limit:
                return
        cursor.close()

def grab_weekly_updates(limit=20):
    root = 'https://www.nps.gov'
    start_url = root + '/subjects/nationalregister/weekly-list.htm'
    start_page = requests.get(start_url).text
    soup = bs(start_page, features="html.parser")

    with connect_db() as conn:
        count = 0
        cursor = conn.cursor()
        rows = soup.find_all('div', class_='ArticleTextGroup')[3]
        for row in rows.find_all('a'):
            link = row.attrs['href']
            print(link)
            print(root + link)
            grab_one_weekly_update(root+link, cursor)
            count += 1
            print("count", count, limit)
            if count >= limit:
                return
        cursor.close()

def grab_20190517_20190510_updates():
    root = 'https://www.nps.gov'
    start_url = root + '/subjects/nationalregister/weekly-list.htm'
    start_page = requests.get(start_url).text
    soup = bs(start_page, features="html.parser")

    with connect_db() as conn:
        cursor = conn.cursor()
        for link in ('/subjects/nationalregister/weekly-list-20190510.htm', '/subjects/nationalregister/weekly-list-20190517.htm'):
            try:
                print(link)
                print(root + link)
                grab_one_weekly_update(root+link, cursor)
            except:
                print("exception: ", row)
                continue
        cursor.close()

def grab_20190524_20190531_updates():
    root = 'https://www.nps.gov'
    start_url = root + '/subjects/nationalregister/weekly-list.htm'
    start_page = requests.get(start_url).text
    soup = bs(start_page, features="html.parser")

    with connect_db() as conn:
        cursor = conn.cursor()
        for link in ('/subjects/nationalregister/weekly-list-20190524.htm', '/subjects/nationalregister/weekly-list-20190531.htm'):
            try:
                print(link)
                print(root + link)
                grab_one_weekly_update(root+link, cursor)
            except:
                print("exception: ", row)
                continue
        cursor.close()

def grab_20190607_updates():
    root = 'https://www.nps.gov'
    start_url = root + '/subjects/nationalregister/weekly-list.htm'
    start_page = requests.get(start_url).text
    soup = bs(start_page, features="html.parser")

    with connect_db() as conn:
        cursor = conn.cursor()
        for link in ('/subjects/nationalregister/weekly-list-20190607.htm',):
            try:
                print(link)
                print(root + link)
                grab_one_weekly_update(root+link, cursor)
            except:
                print("exception: ", link)
                continue
        cursor.close()

def grab_one_weekly_update(link, cursor):
    print(link)
    page = requests.get(link).text
    pubdate = date_from_weekly_url(link)
    update = "INSERT INTO weekly_updates (url, text, published) VALUES (%s, %s, %s) ON CONFLICT ON CONSTRAINT weekly_updates_url_key DO UPDATE SET url = %s, text = %s, published = %s"
    values = (link, page, pubdate, link, page, pubdate)
    # cursor.execute(update, values)
    print(cursor.mogrify(update, values))

def dictionaries_for_weekly_updates(limit=2):
    with connect_db() as conn:
        rcursor = conn.cursor()
        fetch = "SELECT update_id, text, url, published FROM weekly_updates WHERE published > '2018-01-01' ORDER BY published DESC LIMIT {}".format(limit)
        #fetch = "SELECT update_id, text, url FROM weekly_updates WHERE url IN ('https://www.nps.gov/subjects/nationalregister/weekly-list-20181130.htm', 'https://www.nps.gov/subjects/nationalregister/weekly-list-20180518.htm', 'https://www.nps.gov/subjects/nationalregister/weekly-list-20180420.htm')"
        rcursor.execute(fetch)
        all_entries_dictionaries = []
        for upd in rcursor:
            update_id = upd[0]
            page = upd[1]
            url = upd[2]
            entries = parse_weekly_update(update_id, page, url)
            updates_dict = {'update_id': update_id, 'updates': entries}
            all_entries_dictionaries.append(updates_dict)
        print(len(all_entries_dictionaries), 'weekly update dictionaries')
    return all_entries_dictionaries

def insert_properties_for_dictionaries(updates_dict_array):
    prop_insert = "INSERT INTO properties (refnum, resname, address, city, county, state, certdate, multname) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT ON CONSTRAINT properties_pkey DO NOTHING"
    weekly_insert = "INSERT INTO weekly_properties (refnum, update_id) VALUES (%s, %s) ON CONFLICT ON CONSTRAINT weekly_properties_update_id_nris_key DO NOTHING"
    with connect_db() as conn:
        wcursor = conn.cursor()
        for updates_dict in updates_dict_array:
            update_id = updates_dict['update_id']
            dicts = updates_dict['updates']
            for prop in [x for x in dicts if x != None]:
                if prop['status'] == 'LISTED':
                    values = (prop['refnum'], prop['resname'], prop['address'], prop['city'], prop['county'], prop['state'], prop['acdate'], prop['multname'])
                    wvalues = (prop['refnum'], update_id)
                    # wcursor.execute(prop_insert, values)
                    # wcursor.execute(weekly_insert, wvalues)
                    print(wcursor.mogrify(prop_insert, values))
                    print(wcursor.mogrify(weekly_insert, wvalues))
        wcursor.close()
        
def date_from_weekly_url(url):
    # samples:
    #     https://www.nps.gov/subjects/nationalregister/weekly-list-20190307.htm
    #     https://www.nps.gov/subjects/nationalregister/weekly-list-2018-02-09.htm
    datestring = url[:-4].replace('-','')[-8:]
    year = int(datestring[:4])
    month = int(datestring[4:6])
    day = int(datestring[6:8])
    print (datestring, year, month, day)
    pubdate = datetime.datetime(year, month, day, 12)
    return pubdate
    
def parse_weekly_update(update_id, page, url):
    soup = bs(page, features="html.parser")
    article_text = soup.findAll('div', class_='ArticleTextGroup')[0]
    plain_text = (x.strip() for x in  article_text.getText().splitlines())
    #for x in (plain_text):
    #   print(x)
    #print(type(article_text.getText()), article_text.getText())
    paras = article_text.findAll('p')
    entries_dictionaries = []
    if len(paras) > 10:
        in_listings = False
        for para in paras:
            if in_listings:
                property_info = parse_property_para(para)
                entries_dictionaries.append(property_info)
            elif para.text.startswith('KEY: '):
                in_listings = True
        print(url, 'paragraphs', update_id, len(entries_dictionaries))
    else:
        in_listings = False
        in_single_listing = False
        single_listing_lines = []
        if len(paras) >= 5:
            # https://www.nps.gov/subjects/nationalregister/weekly-list-20180427.htm has multiple <p> tags under ArticleTextGroup
            all_lines = paras[-1]
        else:
            all_lines = article_text.children
        # needed for https://www.nps.gov/subjects/nationalregister/weekly-list-20190215.htm
        all_lines = map(lambda x: x.strip(), filter(lambda x: isinstance(x, NavigableString), all_lines))
        if url in {'https://www.nps.gov/subjects/nationalregister/weekly-list-20181130.htm', 'https://www.nps.gov/subjects/nationalregister/weekly-list-20180518.htm', 'https://www.nps.gov/subjects/nationalregister/weekly-list-20180420.htm'}: # these have fonts attached to each individual element
            all_lines = plain_text
        for line in all_lines:
            if in_listings:
                if len(line) > 0 and not (line.startswith('WEEKLY LIST ')) and not (line.startswith('KEY: ')):
                    single_listing_lines.append(line)
                elif len(single_listing_lines) >= 4:  # zero-length line found, stop searching and parse
                    property_info = parse_property_lines(single_listing_lines)
                    entries_dictionaries.append(property_info)
                    single_listing_lines = []
            elif (line.startswith('KEY: ')):
                in_listings = True
        if len(single_listing_lines) > 0:
            property_info = parse_property_lines(single_listing_lines)
            entries_dictionaries.append(property_info)
        print(url, 'line by line', update_id, len(entries_dictionaries))
    return entries_dictionaries
    
def load_and_process_all_weekly_updates(count=1):
    grab_weekly_updates(count)
    insert_properties_for_dictionaries(dictionaries_for_weekly_updates(count))
    
def load_and_process_latest_weekly_update():
    grab_latest_weekly_update()
    insert_properties_for_dictionaries(dictionaries_for_weekly_updates(1))
    
def parse_property_para(para):
    lines = [x.strip() for x in para.text.splitlines()]
    return parse_property_lines(lines)

def parse_property_lines(lines):
    if len(lines) < 5 or len(lines) > 6:
        return None
    if lines[0] == '':
        lines = lines[1:]
    result = None
    try:
        state_county =  lines[0].split(',')
        state = state_county[0].strip().title()
        if len(state_county) > 1:
            county = state_county[1].strip()
            if county.endswith(' COUNTY'):
                county = county[:-len(' COUNTY')]
                county = county.title()
        else:
            county = None
        resname = lines[1].rstrip(',')
        address = lines[2].rstrip(',')
        city_numcode = lines[3].rsplit(',', 2)
        city = city_numcode[0].strip().title()
        numcode = city_numcode[1].strip()
        action_code = numcode[0:2]
        refnum = numcode[2:]
        status, action_date = lines[4].split(',')
        action_date = action_date.strip()
        month, day, year = action_date.split('/')
        acdate = datetime.datetime(int(year), int(month), int(day), 12)
        multname = None
        if len(lines) > 5:
            multname = lines[5][1:-1]
        result = {'resname': resname, 'state': state, 'city': city, 'refnum': refnum, 'action_code': action_code, 'nris': numcode, 'county': county, 'address': address, 'status': status, 'date': action_date, 'acdate': acdate, 'multname': multname}
    
    except:
        print(lines)
    return result

                                                                                                                                                                                                                                                                                                                    
                
