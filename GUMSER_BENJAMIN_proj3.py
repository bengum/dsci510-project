from bs4 import BeautifulSoup
import requests
import time
# import path
import re
import os.path
from tqdm import tqdm
import csv
import pickle
import json
import argparse
import math
import datetime
from dateutil.parser import parse


class Globals():
    """ 
    I don't know if this is best practice, but this makes it very easy to
    find the individual classes from this set of dictionaries as well as 
    pickle this single object for reuse.
    """
    def __init__(self):
        self.sites = {}
        self.authors = {}
        self.states = {}
        self.articles = {}
        self.officials = {}

    def __str__(self):
        n_sites = len(self.sites)
        n_authors = len(self.authors)
        n_articles = len(self.articles)
        n_officials = len(self.officials)
        r_string = (f'{n_sites} Sites, '
                  + f'{n_articles} Articles, '
                  + f'{n_authors} Authors, '
                  + f'{n_officials} Officials')
        return r_string
    
    def __repr__(self):
        return 'Globals object'

    def short_site_to_site(self, short_site):
        # Used for converting part of url to site name
        self.site_name_dict = getattr(self, 'site_name_dict', {})
        if len(self.site_name_dict) == 0:
            # Create this if it doesn't yet exist
            for site in self.sites.values():
                name_shrt = site.name_shrt
                self.site_name_dict[name_shrt] = site.name
        return self.site_name_dict[short_site]

        
class Site():
    """
    Holds information about one of the sites from Metric Media
    """
    def __init__(self, site_name, site_url, site_state):
        self.name = site_name
        self.url = site_url
        # Makes it easy to build file names
        self.name_shrt = self.url.lower().split('https://')[1].split('.com')[0]
        self.state = site_state
        self.postal_state = None
        self.article_set = set()
        self.has_html = False
        self.locales = None
        self.address = None
        self.zip_code = None
        self.lat = None
        self.lng = None
        self.file_name = 'data/sites/' + self.name_shrt + '.txt'
        self.officials = set()
        self.local_articles = set()
        self.local_written_articles = set()
        self.local_recent_politics = set()
        
    def __str__(self):
        return self.name
    
    # def __repr__(self):
    #     return 'Site object - ' + self.name

    def to_dict(self, g):
        self.mercator()
        return {
            'name' : self.name,
            'state' : self.postal_state,
            'mlat' : self.mlat,
            'mlng' : self.mlng,
            'local_written_articles' : len(self.local_written_articles),
            'local_recent_politics' : len(self.local_recent_politics)   
            }

    def mercator(self):
        if self.lat and self.lng:
            # Calculations for converting into web mercator coordinates
            r_major = 6378137.0
            x = r_major * math.radians(self.lng)
            scale = x/self.lng
            y = 180.0/math.pi * math.log(math.tan(math.pi/4.0
                + self.lat * (math.pi/180.0)/2.0)) * scale
            self.mlat = y
            self.mlng = x
        else:
            self.mlat = None
            self.mlng = None
    
    def update_postal_state(self, states_dict):
        # Uses the postal state dict to update this attribute
        postal_state = states_dict[self.state].postal_state
        self.postal_state = postal_state

    def find_zip(self, use_local_first=True):
        # Finding a good physical location for each site is a pain
        # This was my second iteration and still isn't a 100% solution
        file_name = 'data/sites/' + self.name_shrt + '9.txt'
        locale_set = set()
        if use_local_first and os.path.isfile(file_name) == True:
            # Already have html for the site
            pass
        else:
            # The business page on each site tends to have zip codes
            bus_url = self.url + '/stories/tag/9-business'
            html = get_html(bus_url)
            if html == False:
                self.locales = locale_set.add(self.state)
                return None
            write_html_to_txt(file_name, html)
        with open(file_name, encoding='utf-8') as r:
            text = str(r.read())
        zip_string = ''
        zip_potential_list = text.lower().split('zip ')[1:]
        # If the word "zip " is found on the page
        if len(zip_potential_list) > 1:
            for zip_potential in zip_potential_list:
                # Only going to look at the next ~15 characters of text
                zip_string += zip_potential[:15] + ' '
            # Find all combinations of exactly 5 digits in a row
            zip_code_list = re.findall('[0-9]{5,5}', zip_string)
            # Quickly remove duplicates
            locale_set = set(zip_code_list)
        else:
            # Can't find a zip code, use the state instead
            locale = ' '.join(self.name.split()[:-1]) + ' ' + self.state
            locale_set.add(locale)
        self.locales = locale_set
        return self.locales


    def get_civic_info(self, use_local_first=True):
        # 100 queries per user per 100 seconds, 25000 per day
        # Tough to get one good zip code from a site, so a set is used
        # Keeping this outside Site class lets the same local file be reused
        good_file = None
        file_name = 'data/sites/civic_' + self.name_shrt + '.txt'
        if use_local_first:
            if os.path.isfile(file_name):
                good_file = file_name
        if use_local_first and good_file:
            # Read local file first, no need to query API
            pass
        else:
            # Need to query API
            # Sleep to stay within quota
            time.sleep(1)
            api_url = 'https://www.googleapis.com/civicinfo/v2/representatives'
            # This key is restricted to this API
            api_key = 'AIzaSyDi_t4Mz-QOTOiyHEZuePk3g2ilaBtGeHI'
            # Will query for Senators and U.S. Representatives
            rep_info = requests.get(api_url, params={
                'key': api_key,
                'address': self.address,
                'roles': ('legislatorLowerBody', 'legislatorUpperBody')}
                )
            if rep_info.status_code == 200:
                # Good response, write to file
                json_file = rep_info.json()
                with open(file_name, 'w') as w:
                    json.dump(json_file, w)
                good_file = file_name
            else:
                # Bad response, try another zip code
                pass    
        # Uses the good file we already had or the one we just downloaded
        with open(good_file) as j:
            civic_info = json.load(j)
        return civic_info


    def get_lat_lng(self, use_local_first=True):
        # Will query Google geocode API for address, zip, lat, long, etc.
        file_name = 'data/sites/locale_' + self.name_shrt + '.txt'
        if use_local_first and os.path.isfile(file_name) == True:
            # Already have json for location
            pass
        else:
            api_url = 'https://maps.googleapis.com/maps/api/geocode/json'
            # This key is restricted to this API
            api_key = 'AIzaSyClAvR4jYdE_AsuFadjL_yHL1_TJWFr0To'
            # Stupid sites sometimes take a few tries to load
            attempt = 1
            while self.locales == None or attempt < 4:
                # Exits loop if locale found
                self.find_zip(use_local_first)
                attempt += 1
            for locale in self.locales:
                # For each zip code
                geo_info = requests.get(api_url, params={
                        'key': api_key,
                        'address': locale}
                        )
                if geo_info.status_code == 200:
                    # Good response, write to file
                    json_file = geo_info.json()
                    with open(file_name, 'w') as w:
                        json.dump(json_file, w)
                        break
                else:
                    pass
        # Always reads from file, even if good response found above
        with open(file_name) as j:
            geo_json = json.load(j)
        return geo_json


class Article():
    """
    Information about an article. The URL serves as the entry in the Globals
    dictionary.
    """
    def __init__(self, article_title, article_url, home):
        self.author = None
        self.title = article_title
        self.url = article_url
        self.site_set = set()
        self.home = home
        self.number = str(self.url.split('.com/stories/')[1][:9])
        self.date = None
        
    def __repr__(self):
        return 'Article object - ' + self.number

    def __str__(self):
        return self.url
        
    
class State():
    """
    Contains information about states.
    """
    def __init__(self, state_name, postal_state, lat, lng):
        self.name = state_name
        self.postal_state = postal_state
        self.lat = lat
        self.lng = lng
        # Calculations for web mercator conversion
        r_major = 6378137.0
        x = r_major * math.radians(self.lng)
        scale = x/self.lng
        y = 180.0/math.pi * math.log(math.tan(math.pi/4.0
            + self.lat * (math.pi/180.0)/2.0)) * scale
        self.mlat = y
        self.mlng = x
        self.local_recent_politics = 0

    def __repr__(self):
        return 'State object - ' + self.name

    def __str__(self):
        return self.postal_state + ' / ' + self.name

    def to_dict(self, g):
        # Used to create dictionary for easy dataframe creation in notebook
        return {
            'name' : self.name,
            'mlat' : self.mlat,
            'mlng' : self.mlng,
            'local_recent_politics' : self.local_recent_politics
            }
        

class Official():
    """
    Info about an elected official. Currently, only Senators and
    Representatives are extracted from API queries. They can be found in the
    Globals.officials dictionary under ['CA First Last']
    """
    def __init__(self, state, name, role, district, party):
        self.state = state
        self.name = name
        self.role = role
        self.district = district
        self.party = party

    def __repr__(self):
        return 'Official object - ' + self.name

    def __str__(self):
        return self.role + ' ' + self.state + ' ' + self.name
    
    
class Author():
    """
    Only authors cited on the front page of one of the sites are scraped.
    Article set is a list of URLs that correspond to Article objects in the 
    Globals.articles dictionary.
    """
    def __init__(self, author_name):
        self.name = author_name
        # Article_list format is url of article
        self.article_set = set()

    def __repr__(self):
        return 'Author object - ' + self.name

    def __str__(self):
        return self.name

    def to_dict(self, g):
        return {
            'name' : self.name,
            'article_count' : len(self.article_set),
            }

    def network(self, g):
        # Create a dictionary where the author's articles appear
        # Sites that are listed multiple times can be grouped in a pd dataframe
        self.site_list = getattr(self, 'site_list', [])
        self.mlat_list = getattr(self, 'mlat_list', [])
        self.mlng_list = getattr(self, 'mlng_list', [])
        self.site_count_list = getattr(self, 'site_count_list', [])
        if len(self.mlat_list) > 0:
            # No need to run this again
            pass
        else:
            for article in self.article_set:
                # site_short is the shortened domain of the article url
                site_short = g.articles[article].home
                try:
                    # Test for external site
                    site_name = g.short_site_to_site(site_short)
                except KeyError:
                    # Probably external site link
                    continue
                if getattr(g.sites[site_name], 'mlat', None) == None:
                    # Run the mercator conversion
                    g.sites[site_name].mercator()
                mlat = g.sites[site_name].mlat
                mlng = g.sites[site_name].mlng
                self.mlat_list.append(mlat)
                self.mlng_list.append(mlng)
                self.site_list.append(site_name)
                self.site_count_list.append(1)
        return {
            'site_name' : self.site_list,
            'mlat' : self.mlat_list,
            'mlng' : self.mlng_list,
            'count' : self.site_count_list
            }


def get_html(url):
    # Open a webpage and return the html
    try:
        content = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, 
            timeout=3.05)
        return content.text
    except requests.exceptions.ConnectionError:
        pass
    except requests.exceptions.Timeout:
        pass
    # Try twice because some of the sites are junk
    try:
        content = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, 
            timeout=3.05)
        return content.text
    except requests.exceptions.ConnectionError:
        return False
    except requests.exceptions.Timeout:
        return False
    

def write_html_to_txt(file_name, html):
    # Write html to a new txt file
    if html == None or html == False:
        return False
    with open(file_name, 'w', encoding='utf-8') as t:
        t.write(str(html))
        
        
def file_to_soup(file_name):
    # Open txt file and convert to soup
    try:
        with open(file_name, encoding='utf-8') as r:
            soup = BeautifulSoup(r.read(), 'lxml')
        return soup
    except FileNotFoundError:
        return None
    
    
def make_states_dict():
    # Populate the dict of states from a saved csv file
    with open('data/states.csv') as s:
        state_list = list(csv.reader(s, delimiter=','))
    states_dict = {}
    for st in state_list[1:]:
        state_name = st[0]
        postal_state = st[1]
        lat = float(st[2])
        lng = float(st[3])
        states_dict[state_name] = states_dict.get(
            state_name, State(state_name, postal_state, lat, lng)
            )
    return states_dict
    
    
def all_links_to_site_class():
    # From the main website, scrape all the sites and urls
    soup = file_to_soup('data/metricmedianews.txt')
    sites_dict = {}
    # Finds each <a> link to site
    all_sites = soup.find_all('a', target='_blank')
    for link in all_sites:
        site_name = link['title']
        # At least one of the urls starts with 'https:/' and this fixes it
        site_url = 'https://' + link['href'].lower().split('/')[-1]
        # The state for each is on a row above the individual sites
        site_state = link.find_previous('b').text   
        sites_dict[site_name] = Site(site_name, site_url, site_state)
    return sites_dict
    
    
def get_html_for_site(site, use_local_first=True):
    # Saves lots of time if using the local file first
    if use_local_first and os.path.isfile(site.file_name) == True:
        # Already have the file and want to use the local file first
        # Changes flag in Site class
        site.has_html = True
        pass
    else:
        # Either don't already have or remote source selected
        html = get_html(site.url)
        if html == False:
            # Error occurred, print error and move on
            print('network error', site.url)
            pass
        else:
            # Downloaded good html, write to txt file, change flag in Site
            write_html_to_txt(site.file_name, html)
            site.has_html = True       


def scrape_articles_from_txt(site, g, soup=False):
    # Adds all the articles from a site main page to a central dictionary
    # If FileNotFound error
    if soup == None: return None
    # If soup isn't passed directly in argument
    if soup == False:
        soup = file_to_soup(site.file_name)
    all_articles = soup.find_all(href=True, title=True, string=True)
    for article in set(all_articles):
        article_title = article['title'].strip()
        raw_article_url = article['href'].strip().lower()
        if raw_article_url.startswith('https://'):
            # Article is linked to an outside site
            article_url = raw_article_url
            is_local = False
        else:
            # Article has home on this site, but has base url removed
            article_url = site.url.lower() + raw_article_url
            is_local = True
        # Add all the articles to the global dictionary
        article_home = article_url.split('https://')[1].split('.com')[0]
        g.articles[article_url] = g.articles.get(
            article_url, Article(article_title, article_url, article_home)
            )
        # Add the site that hosted a link to that article dictionary
        g.articles[article_url].site_set.add(site.name)
        # Add the article to a set on the site itself
        site.article_set.add(article_url)
        if is_local:
            site.local_articles.add(article_url)
                
                
def scrape_authors_from_txt(site, g, soup=False):
    # Finds all the authors that are mentioned on the main site page
    if soup == None: return None
    if soup == False:
        soup = file_to_soup(site.file_name)
    # Finds stories with authors named on front page
    all_authors = soup.find_all(class_='card-author')
    for author in all_authors:
        author_name = author.text.strip().split('By ')[-1]
        if author_name == None or '':
            continue
        # We found the author, and this finds the article they wrote
        article_parent = author.parent.find('a', title=True)
        raw_article_url = article_parent['href'].strip().lower()
        if raw_article_url.startswith('https://'):
            # Article is linked to an outside site
            article_url = raw_article_url
        else:
            # Article has home on this site, but has base url removed
            article_url = site.url.lower() + raw_article_url
        # Associate the article with this author
        g.articles[article_url].author = author_name
        # Creates a new Author in the global dictionary
        g.authors[author_name] = g.authors.get(
            author_name, Author(author_name)
            )
        # Adds an article to the Author
        g.authors[author_name].article_set.add(article_url)

def get_politics(site, g, use_local_first=True):
    file_name = 'data/sites/politics_' + site.name_shrt + '.txt'
    if use_local_first and os.path.isfile(file_name) == True:
        # Already have html
        pass
    else:
        pol_url = site.url + '/stories/tag/126-politics'
        html = get_html(pol_url)
        if html == False:
            print('bad politics', site.url)
            return None
        write_html_to_txt(file_name, html)
    soup = file_to_soup(file_name)
    bad_authors = ['Metric Media News Service', 'Press release submission']
    for article in soup.find_all(class_='card-title title'):
        try:
            article_sib = article.next_sibling.next_sibling
            if 'card-author' in str(article_sib):
                author = article_sib.a.text
                if author in bad_authors:
                    continue
                date = article_sib.find_all(class_='grey')[0].text
            elif 'card-author' in str(article_sib.next_sibling.next_sibling):
                author = article_sib.next_sibling.next_sibling.a.text
                if author in bad_authors:
                    continue
                date = article_sib.next_sibling.next_sibling.find_all(
                        class_='grey')[0].text
            if parse(date).date() < datetime.date(2020, 9, 1):
                continue
            title = article.text.strip()
            url = article.a['href']
            if url.startswith('/stories/'):
                local = True
                url = site.url + url
                home = site.name_shrt
            else:
                local = False
                home = url.split('https://')[1].split('.com')[0]
            g.articles[url] = g.articles.get(url, Article(title, url, home))
            g.articles[url].date = date
            g.authors[author] = g.authors.get(author, Author(author))
            g.authors[author].article_set.add(url)
            g.sites[site.name].article_set.add(url)
            if local:
                g.sites[site.name].local_articles.add(url)
                g.sites[site.name].local_written_articles.add(url)
                if parse(date).date() > datetime.date(2020, 9, 1):
                    g.sites[site.name].local_recent_politics.add(url)
        except AttributeError:
            # Reached end of articles
            pass  
        


def populate_civic_info(civic_info, g, site):
    try:
        civic_info['offices']
    except KeyError:
        # Bad address from Lake Tahoe resulted in junk civic info
        return None
    # Process the JSON from the API
    for office in civic_info['offices']:
        role = office['name']
        division = office['divisionId']
        state = division.partition('state:')[2][:2].upper()
        district = division.partition('cd:')[2][:2]
        for i in office['officialIndices']:
            name = civic_info['officials'][i]['name']
            if name.lower() == 'vacant':
                g.sites[site.name].officials.add('vacant')
                continue
            try:
                # Some don't belong to a political party
                party = civic_info['officials'][i]['party']
            except KeyError:
                party = None
            dict_entry = state + ' ' + name
            # Add official to the global dictionary
            g.officials[dict_entry] = g.officials.get(
                dict_entry, Official(state, name, role, district, party)
                )
            # Add that official to the Site set
            g.sites[site.name].officials.add(dict_entry)


def json_to_location_info(json):
    # Process the JSON from the API
    zip_code = None
    lat = None
    lng = None
    for result in json['results']:
        for part in result['address_components']:
            if 'postal_code' in part['types']:
                zip_code = part['long_name']
                break
        address = result['formatted_address']
        lat = result['geometry']['location']['lat']
        lng = result['geometry']['location']['lng']
        return address, zip_code, lat, lng


def main():
    parser = argparse.ArgumentParser(description="Metric Media is unsettling.")

    parser.add_argument("--source", 
                        choices=["remote", "local"], 
                        required=False,
                        type=str, 
                        help="""Remote downloads fresh everything and is slow.
                            Local is way faster.""")

    parser.add_argument("--grade",
                        required=False,
                        action='store_true',
                        help="Populates 3 sites from remote data.")

    args = parser.parse_args()
    source = args.source
    grade_mode = args.grade
    
    if source == 'local':
        local_first = True
    elif source == 'remote':
        local_first = False
    elif source == None:
        # Defaults to local storage first because remote takes a long time
        local_first = True

    if grade_mode == True:
        s_start = 0
        s_stop = 3
        local_first = False
        pickle_file = 'data/grade.pickle'
    else:
        s_start = None
        s_stop = None
        local_first = True
        pickle_file = 'data/g.pickle'

    g = Globals()
    # These links are known bad links
    broken_link_list = [
        'https://forsmithtimes.com', 'https://northsacramentotoday.com',
        'https://southsacramentotoday.com', 'https://vincennestoday.com',
        'https://midcoastimes.com', 'https://kentcountytimes.com',
        'https://segeorgianew.com', 'https://fonddulacnews.com'
        ]
    # Populate the Globals with states, was old functionality I may reuse
    g.states = make_states_dict()
    # Main page for links to all the Metric Media sites
    all_sites_url = 'https://metricmedianews.com'
    write_html_to_txt('data/metricmedianews.txt', get_html(all_sites_url))
    g.sites = all_links_to_site_class()

    for site in tqdm(list(g.sites.values())[s_start:s_stop]):
        if site.url in broken_link_list:
            # Skip bad link
            continue
        site.update_postal_state(g.states)
        # Goes to the main site page, downloads html, writes to file
        get_html_for_site(site, use_local_first=local_first)
        if not site.has_html:
            # Site didn't work, so can't really do more with the site
            continue
        # Use BeautifulSoup to scrape site loaded from file
        soup = file_to_soup(site.file_name)
        # Get list of all articles on site and create Article objects if needed
        scrape_articles_from_txt(site, g, soup)
        # Create Authors cited on main site page
        scrape_authors_from_txt(site, g, soup)

        get_politics(site, g, use_local_first=local_first)
        # Try to scrape (regex, actually) a zip code from the Business section
        site.find_zip(use_local_first=local_first)
        # Turn a zip code or the site name and state via API call
        geo_json = site.get_lat_lng(use_local_first=local_first)
        # Read the saved json file and extract address and such if found
        address, zip_code, lat, lng = json_to_location_info(geo_json)
        site.address = address
        site.zip_code = zip_code
        site.lat = lat
        site.lng = lng
        # If the site has a valid address, get Official info
        if len(site.officials) == 0 and site.address != None:
            # Get civic info
            civic_info = site.get_civic_info(use_local_first=local_first)
            populate_civic_info(civic_info, g, site)

    # Save the Globals object containing all other objects to a pickle file
    # Name depends on --grade flag
    with open(pickle_file, 'wb') as p:
        pickle.dump(g, p)


if __name__ == '__main__':
    main()
        

