from typing import List
import os
import collections
import pandas as pd
from contextlib import suppress
import datetime as dt
import re
import logging
from static_resources.table_schema import table_schema
# Parsing Imports
from bs4 import BeautifulSoup
import requests
import json
import time
from os import _exit

from google.cloud import bigquery

headers = json.load(open('static_resources/headers.json', 'r'))

def flatten(d, parent_key='', sep='_'):
	items = []
	for k, v in d.items():
		new_key = k
		if isinstance(v, collections.MutableMapping):
			items.extend(flatten(v, new_key, sep=sep).items())
		else:
			items.append((new_key, v))
	return dict(items)


def clean_results(listings: List[dict]) -> List[dict]:
	"""
	Input: the search results list; should be 40 listings per list (assuming full capacity), each
	entry to the list is a dictionary for each listing. Ideally this will be the output of parseSearchPage.

	This function:
		1. Flattens the nested dictionary to one layer
		2. Converts the post date and date sold from milliseconds to seconds
		3. Removes redundant variables 'beds', 'baths','addressCity','addressState', 'addressStreet', 'addressZipcode', 'countryCurrency', 'daysOnZillow', 'text', and 'zpid'
		4. Removes links to images where there is no image
	
	Returns: a cleaned list of listings
	"""
	for index, listing in enumerate(listings):
		listing = flatten(listing, parent_key=False)
		redundants = ['beds', 'baths','addressCity','addressState', 'addressStreet', 'addressZipcode', 'countryCurrency', 'daysOnZillow', 'zpid', 'badgeInfo', 'providerListingId']
		for var in redundants:
			with suppress(KeyError):
				del listing[var]

		listing['id'] = int(listing['id'])
		listing['zipcode'] = int(listing['zipcode'])

		#Calculate listing date
		if 'day' in listing['text']:
			n_days_on_zillow = int(re.match('\d', listing['text']).group(0))
			listing_date = (dt.datetime.now() - dt.timedelta(days=n_days_on_zillow)).strftime('%Y-%m-%d')
		elif 'hour' in listing['text']:
			n_hours_on_zillow = int(re.match('\d', listing['text']).group(0))
			listing_date = (dt.datetime.now() - dt.timedelta(hours=n_hours_on_zillow)).strftime('%Y-%m-%d')
		else:
			listing_date = None
		listing['listDate'] = listing_date

		listings[index] = listing
		return listings


def format_table(table):
	"""Coerce any other table into our required schema"""
	with suppress(KeyError): 
		for col_name, col_type in table_schema.items():
			table[col_name]  = table[col_name].astype(col_type)
	return table


def scrape(city_or_zipcode: str, state: str, rent=False):
	"""Gathers the listing data from a search page
	
	[Marc 1/17/23]: I know this function is such a clusterfuck. I can barely
	read it myself. I need to refactor it so it's actually understandable,
	but for now, ¯\_(ツ)_/¯
	"""
	region = f"{city_or_zipcode},-{state}"
	search_url = f"https://www.zillow.com/{region}/"
	pages = []

	if rent: search_url += 'rent/'
	listings = []
	pages = [i for i in range(1, 26)] if not pages else pages
	total_listings = 0

	for page in pages:
		logging.info(f"Working on page {page}")
		try:
			resp = requests.get(f"{search_url}{page}_p/", headers=headers)
			logging.info("Request finished")
			soup = BeautifulSoup(resp.content, "html.parser")
			if "Please verify you're a human to continue." in resp.text:
				return "Captcha blocked"

			listings = soup.find(
			"script",
			attrs={"data-zrr-shared-data-key": "mobileSearchPageStore"})
			listings = listings.string.replace("<!--", "").replace("-->", "")
			listings = json.loads(listings)["cat1"]["searchResults"]["listResults"]
			if len(listings) == 0:
				logging.info("No listings found. Going to next zip code.")
				break
			# If page redirected back to valid page break
			if page == 1:
				first_addy = listings[0]['addressStreet']
			else:
				if listings[0]['addressStreet'] == first_addy: break
			total_listings += len(listings)
			logging.info(f"Page {page} done, {len(listings)} listings found. Inserting into dataframe...")
			
			#Create new table with defined schema:
			table = pd.DataFrame({key: pd.Series(dtype=val) for key, val in table_schema.items()})
			#And a new table with the new data
			new_data = pd.DataFrame(clean_results(listings))
			#Get only the columns the new data has in common with schema
			valid_cols = set(new_data.columns).intersection(set(table.columns))
			new_valid_data = new_data[valid_cols]
			#Format columns:
			new_formatted_data = format_table(new_valid_data)
			#Put formatted new data into established schema
			table = table.append(new_formatted_data)
			#Add to database
			#result = db.insert_rows_from_dataframe(table_ref, table)
			#logging.info(f"Insert result: {result}")
		
			time.sleep(pd.DataFrame([1,2,3]).sample().values[0][0])
			
		#ERROR HANDLING
		except (ValueError):
			logging.info(f'Capatcha encountered at {page} on {region}')
			last_page = page
			with open('parse_cache.json', 'w') as cache:
				cache.write(json.dumps(f"'last_page':{last_page}"))
				_exit(1)
		
		#Append to larger dataframe
		if page == 1:
			all_data = table.copy()
		else:
			all_data = all_data.append(table)
	
	all_data.reset_index(inplace=True, drop=True)

	return all_data


def deploy(request):
	"""
	This function actually exists as a Google Cloud Function to help prevent
	us from getting blocked by Zillow. If Zillow sees a ton of requests all coming
	from the same machine, it's more likely they'll block us out. But by using
	Google Cloud Functions, we can run the code on different machines and hopefully
	work around their blocks.
	
	The majority of this code was prewritten by Google. Their docstring starts here.
	=================================================================================
	Responds to any HTTP request.

	Args:
		request (flask.Request): HTTP request object.
	Returns:
		The response text or any set of values that can be turned into a
		Response object using
		`make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
	"""
	request_json = request.get_json()
	if request.args and 'zip' in request.args:
		tot_ls = scrape(request.args.get('zip'), request.args.get('st'))
		if tot_ls == "Captcha blocked":
			raise Exception("Captcha blocked")
		else:
			return  f'Done. {tot_ls} listings found'
	elif request_json and 'zip' in request_json:
		tot_ls = scrape(request_json['zip'], request_json['st'])
		if tot_ls == "Captcha blocked":
			raise Exception("Captcha blocked")
		else:
			return f'Done. {tot_ls} listings found'
	else:
		return f'Zip not found'


if __name__ == '__main__':
	# [Marc 1/17/23]: This section looks like it doesn't have any effect...
	os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "function_source/firestore_credentials.json"
	db = bigquery.Client(project='webscraper-329918')
	table_ref = db.dataset('listings').table('listings')
	table_ref = db.get_table(table_ref)
