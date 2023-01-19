"""
[Marc 1/17/23]:
This was my first attempt at making this app a long time ago. It was very buggy at best,
and it's currently not working anymore. (╯°□°)╯︵ ┻━┻
It really wouldn't be that bad to completely overhaul this if we want given 
how awful it looks. I also used Dash, not Streamlit (not that that's a reason to overhaul it).
"""

from bs4 import BeautifulSoup
import dash_bootstrap_components as dbc
from dash import Dash, Input, Output, State, dcc, html
from dash.exceptions import PreventUpdate
from dash_extensions.enrich import Trigger, FileSystemCache
import json
import plotly.express as px
import os
import pandas as pd
import requests
import time
from google.cloud import firestore

from main import clean_results, format_table


os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "function_source/firestore_credentials.json"
db = firestore.Client(project='webscraper-329918')

#Import static resources
table_schema = json.load(open('static_resources/table_schema.json' , 'r'))
headers = json.load(open('static_resources/headers.json', 'r'))

app = Dash(__name__) #, suppress_callback_exceptions=True)
app = Dash(external_stylesheets=[dbc.themes.BOOTSTRAP])

# Create a server side resource.
fsc = FileSystemCache("cache_dir")
fsc.set("progress", None)
fsc.set("label", None)

app.layout = dbc.Container([
    
    dcc.Store(id='data'),
    
    html.H1('Zillow Scraper'),
    
    html.P('Search for a city or zipcode'),
    
    dbc.Row([
        dcc.Input(id='search', type='text', debounce=True),
        dbc.Progress(id='progress', style={'display':'None'}, animated=False, striped=True),
        dcc.Interval(id="interval", interval=100, disabled=True)
    ]),
    
    html.P(id='testing'),
    
    dbc.Row([
        dbc.Col([
            dcc.Graph(id='price-zestimate')
        ]),
        dbc.Col([
            dbc.Card([
                    dbc.CardImg(id='listing-image', top=True),
                    dbc.CardBody(id='listing-preview')
            ])
        ]),
    
        dbc.Row([
            dbc.Table()
        ])
    ], id='results-content', style={'display':'None'}),

])


#----------------------------------
#Activate Interval Trigger
#----------------------------------
@app.callback(
    Output('interval', 'disabled'),
    Output('progress', 'style'),
    [Input('search', 'value')],
    State('data', 'data'),
    Trigger("interval", "n_intervals"))
def activate_trigger(text, data, trg):
    if text is None and data is None:
        raise PreventUpdate
    elif text is not None and data is None:
        app.logger.info("Activating trigger")
        return False, {'display':'block'} #may be inline instead of block
    else: #if text is not None and data is not None
        return True, {'display':'None'} #disable the trigger and hide the progress bar


#----------------------------------
# Update progress bar
#----------------------------------
@app.callback(
    Output("progress", "value"), 
    Trigger("interval", "n_intervals"))
def update_progress(arg):
    value = fsc.get("progress")  # get progress
    if value is None:
        raise PreventUpdate
    #app.logger.info(f"Progress is at {str(int(fsc.get('progress')))}")
    return int(fsc.get('progress'))


#----------------------------------
#Search for city/zip to analyze
#----------------------------------
@app.callback(
    Output('data', 'data'),
    Output('results-content', 'style'),
    [Input('search', 'value')])
def get_data(text):
    if text is None:
        raise PreventUpdate
    if len(text) == 5 and text.isnumeric(): #If text is a zipcode
        try:
            ref = db.collection(u'zipcodes').where('Zipcode', '==', text).stream()
            app.logger.info(f"Trying {text} as zipcode search")
            #Get corresponding state
            for doc in ref:
                state = doc.to_dict()['State']
            params = {"zip":text, "st":state}
        except:
            pass
    else:
        if "," in text: #If state is included, parse it
            city = text[:text.find(", ")]
            state = text[text.find(","):].replace(",","").replace(" ", "")
            app.logger.info(f"Searching for city: {city}, state:{state}")
            params = {"zip":city, "st":state}

        else:
            ref = db.collection(u'zipcodes').where('City', '==', text.upper()).order_by('EstimatedPopulation', direction=firestore.Query.DESCENDING).limit(1).stream()
            for doc in ref:
                state = doc.to_dict()['State']
            app.logger.info(f"Assuming state to be {state}")
            params = {"zip":text, "st":state}


    region = f"{params['zip']},-{params['st']}"
    search_url = f"https://www.zillow.com/{region}/"
    rent = False
    pages = []
    start = 0

    if rent: search_url += 'rent/'
    listings = []
    pages = [i for i in range(1, 26)] if not pages else pages
    all_addresses = set()
    all_listings = []
    total_listings = 0

    for page in pages:
        print(f"Working on page {page}")
        try:
            resp = requests.get(f"{search_url}{page}_p/", headers=headers)
            print("Request finished")
            soup = BeautifulSoup(resp.content, "html.parser")
            if "Please verify you're a human to continue." in resp.text:
                return "Captcha blocked"

            #Get the number of listings
            nListings = soup.find_all("div", class_="total-text")
            nListings = int(nListings[0].text)
            
            listings = soup.find(
                            "script",
                            attrs={"data-zrr-shared-data-key": "mobileSearchPageStore"})
            listings = listings.string.replace("<!--", "").replace("-->", "")
            listings = json.loads(listings)["cat1"]["searchResults"]["listResults"]
            if len(listings) == 0:
                print("No listings found. Going to next zip code.")
                break
            print("Listings found")
            # If page redirected back to valid page break
            if page == 1:
                first_addy = listings[0]['addressStreet']
            else:
                if listings[0]['addressStreet'] == first_addy:
                    break
                
            #Update progress bar
            total_listings += len(listings)
            app.logger.info(f"Setting progress to {str(int(total_listings/nListings * 100))}")
            fsc.set("progress", int(total_listings/nListings * 100))
            
            print(
                f"Page {page} done, {len(listings)} listings found. Inserting into dataframe...")
            #Create new table with defined schema:
            table = pd.DataFrame({key: pd.Series(dtype=val) for key, val in table_schema.items()})
            #And a new table with the new data
            new_data = pd.DataFrame(clean_results(listings))
            #Get only the columns the new data has in common with schema
            valid_cols = set(new_data.columns).intersection(set(table.columns))
            new_data = new_data[valid_cols]
            #Format columns:
            new_data = format_table(new_data)
            #Put formatted new data into established schema
            table = table.append(new_data)
            #Add to database
            #result = db.insert_rows_from_dataframe(table_ref, table)
            #print(f"Insert result: {result}")

            time.sleep(pd.DataFrame([1, 2, 3]).sample().values[0][0])

        #ERROR HANDLING
        except (ValueError):
            print(f'Capatcha encountered at {page} on {region}')
            last_page = page
            with open('parse_cache.json', 'w') as cache:
                cache.write(json.dumps(f"'last_page':{last_page}"))

        #Append to larger dataframe
        if page == 1:
            all_data = table.copy()
        else:
            all_data = all_data.append(table)

    all_data.reset_index(inplace=True, drop=True)
    app.logger.info("Saving dataframe into browser memory")
    
    return all_data.to_json(), {'display':'None'} 

#----------------------------------
#Generate plot
#----------------------------------
@app.callback(
    Output('price-zestimate', 'figure'),
    #Output('testing', 'children'),
    Input('data', 'data')
)
def generate_plot(df):
    #Convert df from json to dataframe
    df = pd.DataFrame(json.loads(df))
    
    fig = px.scatter(df, x='zestimate', y='price', title='Price vs Zestimate', custom_data=["id"]) #custom_data used for hoverdata
    fig.update_layout(clickmode='event+select')

    return fig


#----------------------------------
#Show preview next to scatterplot
#----------------------------------
@app.callback(
    Output('listing-image', 'src'),
    Output('listing-preview', 'children'),
    Input('price-zestimate', 'hoverData'),
    State('data', 'data')
    )
def display_hover_data(hoverData, df):
    if hoverData is None:
        raise PreventUpdate
    
    #Convert df from json to dataframe
    df = pd.DataFrame(json.loads(df))
    
    #Get id of property
    hoverData = json.loads(json.dumps(hoverData))
    id = hoverData['points'][0]['customdata'][0]
    
    #Get url of image
    src_url = df.loc[df['id'] == id, 'imgSrc'].head(1)
    
      #Get data for property id
    listing_df = df.loc[df['id'] == id, ['streetAddress', 'city', 'state', 'zipcode', 'price', 'lotAreaValue']].head(1).transpose()
    listing_df['Attribute'] = listing_df.index
    listing_df = listing_df[['Attribute', listing_df.columns[0]]] #Switch column order
    listing_df.columns = ["",""]
    return src_url, dbc.Table.from_dataframe(listing_df)



if __name__ == "__main__":
    app.run_server(debug=True)
