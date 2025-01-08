#!/usr/bin/env python
# coding: utf-8

# In[4]:
#Read data
import xml.etree.ElementTree as ET
import pandas as pd
from sodapy import Socrata
from dotenv import load_dotenv
import os

# Extract & Transform
import kagglehub
import ast
import re
import uuid

# Normalize locations
from geopy.exc import GeocoderTimedOut, GeocoderQuotaExceeded
from geopy.geocoders import Nominatim
import time

#Load
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
import psycopg2

# In[5]:
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

logger.info("Starting data ingestion pipeline...")

# # Data Collection

# In[5]:
load_dotenv()
app_token = os.getenv("APP_TOKEN")
username = os.getenv("OPEN_DATA_NYC_USERNAME")
password = os.getenv("OPEN_DATA_NYC_PASSWORD")


# In[6]:
def fetch_restaurant_data(app_token, username, password, dataset_id="pitm-atqc", limit=1000):
    """
    Fetch restaurant data from the NYC Open Data API.

    Parameters:
        app_token (str): Your application token for the API.
        username (str): Your username for the API (email).
        password (str): Your password for the API.
        dataset_id (str): The dataset identifier in Socrata.
        limit (int): The maximum number of results to fetch (default is 1000).

    Returns:
        pd.DataFrame: A pandas DataFrame containing the restaurant data.
    """
    # Initialize the Socrata client
    client = Socrata("data.cityofnewyork.us", app_token, username=username, password=password)

    # Fetch data
    results = client.get(dataset_id, limit=limit)

    # Convert results to a pandas DataFrame
    df_restaurants = pd.DataFrame.from_records(results)
    print(f"Fetched {len(df_restaurants)} rows from the API.")

    return df_restaurants


# In[7]:


def parse_selected_sheets(file_path, sheet_names, header_row_mapping):
    """
    Parse selected worksheets from an XML-based Excel workbook.

    Parameters:
        file_path (str): Path to the XML file.
        sheet_names (list): List of worksheet names to parse.
        header_row_mapping (dict): A mapping of sheet names to header row indices.

    Returns:
        list: A list of pandas DataFrames corresponding to the selected sheets.
    """
    def extract_row_data(cells, expected_columns):
        """
        Helper function to extract row data and ensure it matches the number of headers.
        """
        row_data = []
        for i in range(expected_columns):
            try:
                cell = cells[i].find(".//ss:Data", ns)
                row_data.append(cell.text.strip() if cell is not None else None)
            except IndexError:
                row_data.append(None)  # Append None if the column is missing
        return row_data

    # Parse the XML file
    tree = ET.parse(file_path)
    root = tree.getroot()

    # Namespace dictionary for handling XML namespaces
    ns = {'ss': 'urn:schemas-microsoft-com:office:spreadsheet'}

    # Retrieve all worksheets
    worksheets = root.findall(".//ss:Worksheet", ns)

    # List to store DataFrames for selected sheets
    dataframes = []

    # Loop through each worksheet
    for sheet in worksheets:
        sheet_name = sheet.attrib.get(f"{{{ns['ss']}}}Name")  # Get the sheet name
        if sheet_name not in sheet_names:
            continue  # Skip sheets that are not in the specified list

        rows = sheet.findall(".//ss:Row", ns)  # Find all rows in the sheet

        if not rows:
            print(f"Sheet '{sheet_name}' is empty. Skipping...")
            continue

        # Determine the header row index (default to 0 if not specified)
        header_row_index = header_row_mapping.get(sheet_name, 0)
        if header_row_index >= len(rows):
            print(f"Invalid header row index for sheet: {sheet_name}. Skipping...")
            continue

        # Extract headers from the specified row
        header_row = rows[header_row_index]
        headers = []
        for cell in header_row.findall(".//ss:Cell", ns):
            data = cell.find(".//ss:Data", ns)
            headers.append(data.text.strip() if data is not None else None)
        expected_columns = len(headers)

        # Extract data (skip up to the header row)
        data = []
        for row in rows[header_row_index + 1:]:  # Start after the header row
            cells = row.findall(".//ss:Cell", ns)
            row_data = extract_row_data(cells, expected_columns)
            data.append(row_data)

        # Create a DataFrame for the sheet
        df = pd.DataFrame(data, columns=headers)
        dataframes.append(df)  # Add the DataFrame to the list

        print(f"Processed sheet: {sheet_name} with {len(df)} rows")

    return dataframes

def fetch_fliming_locations_data(xml_file_path):
    """
    Fetch the "Full Map List" worksheet as a pandas DataFrame.

    Parameters:
        xml_file_path (str): Path to the XML file.

    Returns:
        pd.DataFrame: A pandas DataFrame containing data from the "Full Map List" worksheet.
    """
    # Sheet names of interest
    selected_sheets = ['Full Map List']

    # Header row mapping for each sheet
    header_row_mapping = {
        'Full Map List': 1
    }

    # Parse the selected sheets
    dfs = parse_selected_sheets(xml_file_path, selected_sheets, header_row_mapping)
    df_filming_locations = dfs[0]  # Extract the DataFrame for the 'Full Map List' sheet

    return df_filming_locations


# In[8]:


def fetch_movies_data(kaggle_dataset, filename="25k IMDb movie Dataset.csv"):
    """
    Download the latest version of a Kaggle dataset and return the movies DataFrame.

    Parameters:
        kaggle_dataset (str): The Kaggle dataset identifier (e.g., "utsh0dey/25k-movie-dataset").
        filename (str): The name of the CSV file to load (default is "25k IMDb movie Dataset.csv").

    Returns:
        pd.DataFrame: A pandas DataFrame containing the movies data.
    """
    # Download the latest version of the dataset
    path = kagglehub.dataset_download(kaggle_dataset)
    print("Path to dataset files:", path)

    # Construct the full path to the CSV file
    csv_path = f"{path}/{filename}"

    # Load the dataset into a pandas DataFrame
    df_movies = pd.read_csv(csv_path)

    return df_movies


# In[9]:
logger.info(f"Reading Restaurant data from External API")
df_restaurants_raw = fetch_restaurant_data(app_token, username, password)
logger.info("Restaurant data fetched successfully.")    



# In[10]:

current_dir = os.path.dirname(os.path.abspath(__file__))
xml_file_path = os.path.join(current_dir, "datasets", "Interactive_Map_Data.xml")
logger.info(f"Reading XML file from: {xml_file_path}")
df_fl_raw = fetch_fliming_locations_data(xml_file_path)
logger.info("Filming locations data fetched successfully.")


# In[11]:
kaggle_dataset = "utsh0dey/25k-movie-dataset"
logger.info(f"Reading Movies data from Kaggle dataset: {kaggle_dataset}")
df_movies_raw = fetch_movies_data(kaggle_dataset)
logger.info("Movies data fetched successfully.")


# # Data preparation and transformation

# ## Movies

# In[12]:


def extract_movie_id(df, path_column='path', new_column='imdb_id'):
    """
    Extract the unique movie ID from the 'path' field and add it as a new column.

    Parameters:
        df (pd.DataFrame): The input DataFrame containing the 'path' column.
        path_column (str): The name of the column containing the path (default: 'path').
        new_column (str): The name of the new column for the extracted movie ID (default: 'movie_id').

    Returns:
        pd.DataFrame: The updated DataFrame with the extracted movie ID column.
    """
    # Check if the path column exists
    if path_column not in df.columns:
        raise ValueError(f"Column '{path_column}' not found in the DataFrame.")

    # Use regex to extract the movie ID from the path
    df[new_column] = df[path_column].str.extract(r'/title/(tt\d+)/')

    return df


# In[13]:


def extract_year(df, column_name='year'):
    """
    Extract four-digit year from a given column in the DataFrame.

    Parameters:
        df (pd.DataFrame): The DataFrame containing the year column.
        column_name (str): The name of the column to process.

    Returns:
        pd.DataFrame: Updated DataFrame with the year column cleaned.
    """
    # Extract the four-digit year using regex
    df[column_name] = df[column_name].str.extract(r'(\b\d{4}\b)', expand=False)

    return df


# In[14]:


def prepare_column(df, old_column_name, new_column_name):
    """
    Rename a column and convert its values from strings to Python lists.

    Parameters:
        df (pd.DataFrame): The DataFrame containing the column.
        old_column_name (str): The current name of the column.
        new_column_name (str): The new name for the column.

    Returns:
        pd.DataFrame: The updated DataFrame with the renamed and properly formatted column.

    Raises:
        ValueError: If the old_column_name is not found in the DataFrame.
    """
    if old_column_name not in df.columns:
        raise ValueError(f"Column '{old_column_name}' not found in the DataFrame.")

    # Rename the column
    df = df.rename(columns={old_column_name: new_column_name})

    # Convert column values from string to list
    df[new_column_name] = df[new_column_name].apply(ast.literal_eval)  # Safely convert string to list

    return df


def create_lookup_table(df, column_name, id_column_name, value_column_name):
    """
    Create a lookup table with unique values and their IDs from a column in the DataFrame.

    Parameters:
        df (pd.DataFrame): The DataFrame containing the column.
        column_name (str): The name of the column to extract unique values from.
        id_column_name (str): The name of the ID column in the lookup table.
        value_column_name (str): The name of the value column in the lookup table.

    Returns:
        pd.DataFrame: A lookup table with unique values and their IDs.
    """
    # Extract unique values and assign IDs
    unique_values = set(value for values_list in df[column_name] for value in values_list)
    lookup_table = pd.DataFrame({value_column_name: sorted(unique_values)})
    lookup_table[id_column_name] = lookup_table.index + 1  # Assign unique IDs starting from 1

    return lookup_table


def create_link_table(df, lookup_table, column_name, id_column_name, movie_id_column, value_column_name):
    """
    Create a link table connecting movies to values (e.g., genres, actors) by their IDs.

    Parameters:
        df (pd.DataFrame): The movies DataFrame.
        lookup_table (pd.DataFrame): The lookup table with unique values and their IDs.
        column_name (str): The name of the column in the movies DataFrame to link.
        id_column_name (str): The name of the ID column in the link table.
        movie_id_column (str): The name of the unique movie identifier column in the movies DataFrame.
        value_column_name (str): The name of the value column in the lookup table.

    Returns:
        pd.DataFrame: A link table connecting movies (movie_id) to values (e.g., genres, actors) by their IDs.
    """
    # Explode the column into separate rows
    df_expanded = df.explode(column_name)

    # Map values to their IDs using the lookup table
    link_table = (df_expanded[[movie_id_column, column_name]]
                  .merge(lookup_table, left_on=column_name, right_on=value_column_name)
                  .rename(columns={id_column_name: id_column_name})
                  )

    # Drop unnecessary columns and return the link table
    return link_table[[movie_id_column, id_column_name]]

def clean_and_reorder_movies(df):
    """
    Clean and reorder the movies DataFrame by renaming, dropping, and reordering columns.

    Parameters:
        df (pd.DataFrame): The input movies DataFrame.

    Returns:
        pd.DataFrame: The cleaned and reordered movies DataFrame.
    """
    # Rename columns, drop unuseful columns, make lowercase, and reorder
    df = (
        df.rename(columns={
            'movie title': 'title',
            'User Rating': 'nb_users_ratings',
            'Rating': 'rating'
        })
        .drop(columns=['Run Time', 'genres', 'Plot Kyeword', 'actors', 'path'])
        .pipe(lambda x: x.set_axis(x.columns.str.lower(), axis=1))
        .loc[:, ['imdb_id', 'title', 'year', 'director', 'writer', 'overview', 'rating', 'nb_users_ratings']]
    )

    # Replace "no-rating" with None (equivalent to NULL in databases)
    df['rating'] = df['rating'].replace('no-rating', None)
    
    return df

def process_movie_data(df_movies):
    """
    Process the movie dataset through the entire data pipeline:
    1. Prepare and clean the genres column.
    2. Extract the 4-digit year.
    3. Create the Genres lookup table.
    4. Create the Movies_Genres link table.
    5. Prepare and clean the actors column.
    6. Create the Actors lookup table.
    7. Create the Movies_Actors link table.
    8. Clean and reorder the movies DataFrame.

    Parameters:
        df_movies (pd.DataFrame): The raw movies DataFrame.

    Returns:
        tuple: A tuple containing:
            - df_movies (pd.DataFrame): Cleaned and reordered movies DataFrame.
            - df_genres (pd.DataFrame): Genres lookup table.
            - df_movies_genres (pd.DataFrame): Movies_Genres link table.
            - df_actors (pd.DataFrame): Actors lookup table.
            - df_movies_actors (pd.DataFrame): Movies_Actors link table.
    """
    df_movies = extract_movie_id(df_movies, path_column='path', new_column='imdb_id')
    df_movies = prepare_column(df_movies, old_column_name='Generes', new_column_name='genres')
    df_movies = extract_year(df_movies, column_name='year')

    df_genres = create_lookup_table(df_movies, column_name='genres', id_column_name='genre_id', value_column_name='genre')
    df_genres = df_genres.drop_duplicates(subset=['genre'])

    df_movies_genres = create_link_table(
        df_movies,
        df_genres,
        column_name='genres',
        id_column_name='genre_id',
        movie_id_column='imdb_id',
        value_column_name='genre'
    )
    df_movies_genres = df_movies_genres.drop_duplicates(subset=['genre_id', 'imdb_id'])

    df_movies = prepare_column(df_movies, old_column_name='Top 5 Casts', new_column_name='actors')
    df_movies = df_movies.drop_duplicates(subset=['imdb_id'])

    df_actors = create_lookup_table(
        df_movies, column_name='actors', id_column_name='actor_id', value_column_name='actor_name'
    )
    df_actors = df_actors.drop_duplicates(subset=['actor_name'])
    df_movies_actors = create_link_table(
        df_movies,
        df_actors,
        column_name='actors',
        id_column_name='actor_id',
        movie_id_column='imdb_id',
        value_column_name='actor_name'
    )
    df_movies_actors = df_movies_actors.drop_duplicates(subset=['actor_id', 'imdb_id'])

    df_movies = clean_and_reorder_movies(df_movies)

    return df_movies, df_genres, df_movies_genres, df_actors, df_movies_actors



# In[15]:

logger.info("Starting data transformation...")

df_movies = df_movies_raw
df_movies_cleaned, df_genres, df_movies_genres, df_actors, df_movies_actors = process_movie_data(df_movies)


# ## Filming Locations

# In[16]:


def clean_location_text(location_text):
    """
    Clean the Location Display Text field by removing HTML tags, extra spaces, and newlines.

    Parameters:
        location_text (str): The raw location text.

    Returns:
        str: The cleaned location text.
    """
    if not isinstance(location_text, str):
        return location_text  # Return as is if not a string

    # Remove HTML tags (e.g., <br>)
    cleaned_text = re.sub(r'<[^>]*>', ' ', location_text)  # Match any HTML-like tag

    # Replace newlines and excessive whitespace with a single space
    cleaned_text = re.sub(r'\s+', ' ', cleaned_text)

    # Strip leading and trailing whitespace
    cleaned_text = cleaned_text.strip()

    return cleaned_text


def process_filming_locations(df):
    """
   Process the filming locations dataset, including cleaning the location text.

   Parameters:
       df (pd.DataFrame): The raw filming locations DataFrame.

   Returns:
       tuple: A tuple containing two DataFrames:
           - df_filming_locations: Contains metadata and IMDB details.
           - df_filming_locations_movies: Contains location details.
   """
    # Add a unique identifier for each row
    df['location_id'] = range(1, len(df) + 1) #[str(uuid.uuid4()) for _ in range(len(df))]
    
    # Extract 'imdb_id' from the URL
    df = df.rename(columns={'IMDB LINK': 'imdb_id'})
    df['imdb_id'] = df['imdb_id'].apply(
        lambda x: re.search(r'tt\d+', x).group() if isinstance(x, str) and re.search(r'tt\d+', x) else None
    )
    
    # Extract the Director/Filmmaker ID from the 'Director/Filmmaker IMDB Link' column
    df['director_imdb_id'] = df['Director/Filmmaker IMDB Link'].apply(
        lambda x: re.search(r'nm\d+', x).group() if isinstance(x, str) and re.search(r'nm\d+', x) else None
    )

    # Clean the field from HTML Tags, extra spaces, newline characters
    df['Location Display Text'] = df['Location Display Text'].apply(clean_location_text)
    df['Client or book location indicator'] = df['Client or book location indicator'].apply(clean_location_text)

    # Normalize naming
    df.columns = df.columns.str.lower()
    df = df.rename(columns={
        'movie title': 'title',
        'location display text': 'address',
        'client or book location indicator': 'address_indicator'
    })

    df_locations = df[['location_id', 'address', 'address_indicator', 'latitude',
                               'longitude', 'borough', 'neighborhood']].copy()

    df_locations_movies = df[['location_id', 'imdb_id']].copy()
    
    return df_locations, df_locations_movies


# In[17]:


df_locations_m, df_locations_movies = process_filming_locations(df_fl_raw)


# ## Restaurants

# In[18]:


def process_restaurant_data(df):
    """
    Process the restaurant dataset by cleaning, splitting into relevant DataFrames,
    adding unique identifiers, and transforming the landmarkdistrict_terms column.

    Parameters:
        df (pd.DataFrame): The original restaurant DataFrame.

    Returns:
        tuple: (df_restaurants, df_locations, df_restaurants_locations)
    """

    columns_to_drop = [
        'sidewalk_dimensions_length', 'sidewalk_dimensions_width', 'sidewalk_dimensions_area',
        'approved_for_sidewalk_seating', 'approved_for_roadway_seating', 'qualify_alcohol',
        'sla_serial_number', 'sla_license_type', 'landmark_district_or_building',
        'healthcompliance_terms', 'time_of_submission', 'community_board', 'council_district',
        'census_tract', 'bin', 'bbl', 'roadway_dimensions_length', 'roadway_dimensions_width',
        'roadway_dimensions_area', 'globalid', 'objectid', 'food_service_establishment',
    ]
    df = df.drop(columns=columns_to_drop, errors='ignore')  # Safeguard against missing columns

    # Add unique identifiers
    df['restaurant_id'] = range(1, len(df) + 1)
    df['location_id'] = range(234, 234 + len(df)) #[str(uuid.uuid4()) for _ in range(len(df))]

    # Transform the landmarkdistrict_terms column to boolean
    df['landmarkdistrict_terms'] = df['landmarkdistrict_terms'].fillna('false')  # Replace NaN with 'false'
    df['landmarkdistrict_terms'] = df['landmarkdistrict_terms'].str.lower().map({'yes': True, 'false': False})

    # Fix typo
    df = df.rename(columns={
        'bulding_number': 'building_number', 
    })

    df_restaurants = df[['restaurant_id', 'location_id', 'restaurant_name', 'legal_business_name', 'doing_business_as_dba', 'seating_interest_sidewalk', 'landmarkdistrict_terms']].copy()

    df_locations = df[['location_id', 'building_number', 'street', 'borough', 'zip',
                       'business_address', 'latitude', 'longitude', 'nta']].copy()

    return df_restaurants, df_locations


# In[19]:


df_restaurants, df_locations_r = process_restaurant_data(df_restaurants_raw)


# ## Locations (filming and restaurants)

# In[20]:


def merge_locations(df_filming_locations, df_restaurant_locations):
    """
    Merge two location dataframes (filming and restaurants) into a unified locations dataframe.

    Parameters:
        df_filming_locations (pd.DataFrame): DataFrame containing filming location data.
        df_restaurant_locations (pd.DataFrame): DataFrame containing restaurant location data.

    Returns:
        pd.DataFrame: A unified locations DataFrame.
    """
    # Standardize column names
    df_filming_locations = df_filming_locations.rename(columns={
        'neighborhood': 'neighborhood_or_nta'
    })

    df_restaurant_locations = df_restaurant_locations.rename(columns={
        'nta': 'neighborhood_or_nta',
        'business_address': 'address'
    })

    # Add source type column
    df_filming_locations['source_type'] = 'filming'
    df_restaurant_locations['source_type'] = 'restaurant'

    # Select relevant columns
    df_filming_locations = df_filming_locations[[
        'location_id', 'address', 'address_indicator', 'borough', 'neighborhood_or_nta', 'latitude', 'longitude', 'source_type'
    ]]
    df_restaurant_locations = df_restaurant_locations[[
        'location_id', 'building_number', 'street', 'zip', 'borough', 'address', 'neighborhood_or_nta', 'latitude', 'longitude', 'source_type'
    ]]

    # Concatenate the two dataframes
    df_locations = pd.concat([df_filming_locations, df_restaurant_locations], ignore_index=True).loc[:, ['location_id', 'building_number', 'street', 'zip', 'borough', 'address', 'address_indicator', 'neighborhood_or_nta', 'latitude', 'longitude', 'source_type']]

    # Deduplicate locations based on lat/lon
    #df_locations = df_locations.drop_duplicates(subset=['lat', 'lon']).reset_index(drop=True)

    return df_locations


# In[21]:

logger.info("Starting Location Geocode transformation...")
df_unified_locations = merge_locations(df_locations_m, df_locations_r)

#  TODO : Figure out how to handle duplicates locations
#df_unified_locations = df_unified_locations.drop_duplicates(subset=['latitude', 'longitude']).reset_index(drop=True)


# ### Normalize locations with Geopy

# In[22]:


def geocode_location(geolocator, row):
    """Determine whether to perform reverse or forward geocoding."""
    if pd.notna(row['latitude']) and pd.notna(row['longitude']):
        # Reverse geocoding
        result = reverse_geocode_geopy(geolocator, row['latitude'], row['longitude'])
        if result:
            return process_geocode_result(row, result, status="success")
    elif pd.isna(row['latitude']) or pd.isna(row['longitude']):
        # Forward geocoding (try multiple address formats)
        address_formats = [
            f"{row['street']} {row['zip']} {row['borough']}",
            f"{row['building_number']} {row['street']} {row['zip']} {row['borough']}"
        ]
        for address in address_formats:
            result = forward_geocode_geopy(geolocator, address)
            if result:
                return process_geocode_result(row, result, status="success")
    # If all geocoding attempts fail
    return process_geocode_result(row, None, status="failed")

def reverse_geocode_geopy(geolocator, lat, lon, retries=1, delay=1):
    """Perform reverse geocoding using Geopy with error handling."""
    for attempt in range(retries):
        try:
            location = geolocator.reverse((lat, lon), addressdetails=True, timeout=10)
            if location:
                return location.raw
        except (GeocoderTimedOut, GeocoderQuotaExceeded) as e:
            print(f"Error: {e}. Retrying reverse geocode... ({attempt + 1}/{retries})")
        time.sleep(delay * (attempt + 1))
    return None

def forward_geocode_geopy(geolocator, address, retries=1, delay=1):
    """Perform forward geocoding using Geopy with error handling."""
    for attempt in range(retries):
        try:
            location = geolocator.geocode(address, addressdetails=True, timeout=10)
            if location:
                return location.raw
        except (GeocoderTimedOut, GeocoderQuotaExceeded) as e:
            print(f"Error: {e}. Retrying forward geocode... ({attempt + 1}/{retries})")
        time.sleep(delay * (attempt + 1))
    return None

def process_geocode_result(row, result, status):
    """Process the geocoding result and return a standardized dictionary."""
    if status == "success" and result:
        address_details = result.get('address', {})
        return {
            'location_id': row['location_id'],
            'source_type': row['source_type'],
            'status': status,
            'failure_reason': None,
            'address_type': result.get('type'),
            'name': result.get('name'),
            'display_name': result.get('display_name'),
            'latitude': result.get('lat'),
            'longitude': result.get('lon'),
            'house_number': address_details.get('house_number'),
            'road': address_details.get('road'),
            'neighbourhood': address_details.get('neighbourhood'),
            'suburb': address_details.get('suburb'),
            'county': address_details.get('county'),
            'city': address_details.get('city'),
            'state': address_details.get('state'),
            'ISO3166-2-lvl4': address_details.get('ISO3166-2-lvl4'),
            'postcode': address_details.get('postcode'),
            'country': address_details.get('country'),
            'country_code': address_details.get('country_code')
        }
    else:
        # Handle failed geocoding
        return {
            'location_id': row['location_id'],
            'source_type': row['source_type'],
            'status': status,
            'failure_reason': 'No response or invalid data' if status == "failed" else None,
            'address_type': None,
            'name': None,
            'display_name': None,
            'latitude': row['latitude'],
            'longitude': row['longitude'],
            'house_number': row.get('building_number'),
            'road': row.get('street'),
            'neighbourhood': row.get('neighborhood_or_nta'),
            'suburb': None,
            'county': None,
            'city': None,
            'state': None,
            'ISO3166-2-lvl4': None,
            'postcode': row.get('zip'),
            'country': None,
            'country_code': None
        }

def process_locations_with_geopy(df):
    """Process all locations in the DataFrame using Geopy."""
    geolocator = Nominatim(user_agent="food-and-the-city")
    results = [geocode_location(geolocator, row) for _, row in df.iterrows()]
    return pd.DataFrame(results)


# In[23]:
current_dir = os.path.dirname(os.path.abspath(__file__))
output_file_path = os.path.join(current_dir, "outputs", "standardized_locations.csv")

if os.path.exists(output_file_path):
    print(f"File '{output_file_path}' exists. Reading data from the file...")
    df_standardized_locations = pd.read_csv(output_file_path)
else:
    print(f"File '{output_file_path}' does not exist. Processing data...")
    df_standardized_locations = process_locations_with_geopy(df_unified_locations)
    df_standardized_locations.to_csv(output_file_path, index=False)
    print(f"Processed data saved to '{output_file_path}'.")
logger.info("Location Geocode transformation completed.")
logger.info("Data transformation completed.")

# code to control if all the location have been well transfered from one dataframe to another after transformation
# ```python
# def check_missing_ids(source_df, target_df, column_name):
#     """
#     Check if all values in the specified column of the source DataFrame are present in the target DataFrame.
# 
#     Parameters:
#         source_df (pd.DataFrame): The DataFrame containing the source column to check.
#         target_df (pd.DataFrame): The DataFrame where the values should be found.
#         column_name (str): The name of the column to check.
# 
#     Returns:
#         None: Prints the result of the check.
#     """
#     # Find missing IDs
#     missing_ids = set(source_df[column_name]) - set(target_df[column_name])
# 
#     # Print results
#     if missing_ids:
#         print(f"Count of missing {column_name}: {len(missing_ids)}")
#         print(f"These {column_name} values are missing: {missing_ids}")
#     else:
#         print(f"All {column_name} values from the source DataFrame are present in the target DataFrame.")
# 
# # Check for missing location IDs
# check_missing_ids(df_locations_m, df_unified_locations, 'location_id')
# check_missing_ids(df_locations_r, df_unified_locations, 'location_id')
# check_missing_ids(df_restaurants, df_locations_r, 'location_id')
# check_missing_ids(df_locations_m, df_unified_locations, 'location_id')
# 
# # Check for missing location IDs
# check_missing_ids(df_locations_m, df_standardized_locations, 'location_id')
# check_missing_ids(df_locations_r, df_standardized_locations, 'location_id')
# check_missing_ids(df_locations_m, df_standardized_locations, 'location_id')
# check_missing_ids(df_unified_locations, df_standardized_locations, 'location_id')
# ```

# # Data loading

# ## Data Fields name Mapping

# In[25]:

logger.info("Starting data loading into the database...")
# Mapping for each table
mapping_fc_locations = {
    'location_id': 'loc_id',
    'source_type': 'loc_source_type',
    'status': 'loc_status',
    'failure_reason': 'loc_failure_reason',
    'address_type': 'loc_address_type',
    'name': 'loc_name',
    'display_name': 'loc_display_name',
    'latitude': 'loc_latitude',
    'longitude': 'loc_longitude',
    'house_number': 'loc_house_number',
    'road': 'loc_road',
    'neighbourhood': 'loc_neighborhood',
    'suburb': 'loc_suburb',
    'county': 'loc_county',
    'city': 'loc_city',
    'state': 'loc_state',
    'ISO3166-2-lvl4': 'loc_iso3166_2_lvl4',
    'postcode': 'loc_postcode',
    'country': 'loc_country',
    'country_code': 'loc_country_code',
}

mapping_fc_filming_locations = {
    'location_id': 'fl_location_id',
    'imdb_id': 'fl_imdb_id',
}

mapping_fc_restaurants = {
    'restaurant_id': 'res_id',
    'restaurant_name': 'res_name',
    'legal_business_name': 'res_legal_business_name',
    'doing_business_as_dba': 'res_doing_business_as_dba',
    'seating_interest_sidewalk': 'res_seating_interest_sidewalk',
    'landmarkdistrict_terms': 'res_landmarkdistrict_terms',
    'location_id': 'res_location_id',
}

mapping_fc_actors = {
    'actor_id': 'act_id',
    'actor_name': 'act_name',
}

mapping_fc_genre = {
    'genre_id': 'gen_id',
    'genre': 'gen_name',
}

mapping_fc_genres_movies = {
    'genre_id': 'gm_genre_id',
    'imdb_id': 'gm_imdb_id',
}

mapping_fc_actors_movies = {
    'actor_id': 'am_actor_id',
    'imdb_id': 'am_imdb_id',
}

mapping_fc_movies = {
    'imdb_id': 'mov_imdb_id',
    'title': 'mov_title',
    'year': 'mov_year',
    'director': 'mov_director',
    'writer': 'mov_writer',
    'overview': 'mov_overview',
    'rating': 'mov_rating',
    'nb_users_ratings': 'mov_nb_users_ratings',
}



# In[26]:
def rename_columns(df, column_mapping):
    """
    Rename DataFrame columns using a mapping dictionary.

    Parameters:
        df (pd.DataFrame): The DataFrame to rename.
        column_mapping (dict): A dictionary mapping old column names to new ones.

    Returns:
        pd.DataFrame: A DataFrame with renamed columns.
    """
    return df.rename(columns=column_mapping)


# In[27]:
df_locations_renamed = rename_columns(df_standardized_locations, mapping_fc_locations)
df_filming_locations_renamed = rename_columns(df_locations_movies, mapping_fc_filming_locations)
df_restaurants_renamed = rename_columns(df_restaurants, mapping_fc_restaurants)
df_actors_renamed = rename_columns(df_actors, mapping_fc_actors)
df_genres_renamed = rename_columns(df_genres, mapping_fc_genre)
df_movies_genres_renamed = rename_columns(df_movies_genres, mapping_fc_genres_movies)
df_movies_actors_renamed = rename_columns(df_movies_actors, mapping_fc_actors_movies)
df_movies_cleaned_renamed = rename_columns(df_movies_cleaned, mapping_fc_movies)



# ## Data Loading (db connection + INSERT)

# In[28]:
DATABASE_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@" \
               f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
engine = create_engine(DATABASE_URL)


# In[29]:


def load_dataframe_to_postgres(df, table_name, engine, if_exists='append', primary_key_column=None):
    """
    Load a pandas DataFrame into a PostgreSQL database, logging errors and continuing processing.

    Parameters:
        df (pd.DataFrame): The DataFrame to load.
        table_name (str): The target table name.
        engine: The SQLAlchemy database engine.
        if_exists (str): Behavior when the table exists. Options: 'fail', 'replace', 'append'.
        primary_key_column (str): The name of the primary key column to log failing rows.
    """
    if primary_key_column and primary_key_column not in df.columns:
        raise ValueError(f"Primary key column '{primary_key_column}' not found in the DataFrame.")

    successful_rows = []
    failed_rows = []

    for index, row in df.iterrows():
        try:
            # Insert each row individually
            row_df = pd.DataFrame([row])
            row_df.to_sql(table_name, engine, index=False, if_exists=if_exists, method="multi")
            successful_rows.append(row[primary_key_column] if primary_key_column else index)
        except IntegrityError as e:
            engine.dispose()  # Dispose the engine to avoid locked connections
            #print(f"IntegrityError: {e}")
            failed_rows.append({
                "row": row.to_dict(),
                "primary_key": row[primary_key_column] if primary_key_column else index,
                "error": str(e)
            })
            continue

    print(f"Successfully inserted rows: {len(successful_rows)}")
    print(f"Failed rows: {len(failed_rows)}")
    #if failed_rows:
    #    for failed_row in failed_rows:
    #        print(f"Failed primary key: {failed_row['primary_key']}, Error: {failed_row['error']}")

    return successful_rows, failed_rows

def load_dataframe_to_postgres_batch(df, table_name, engine, if_exists='append', batch_size=1000):
    """
    Load a pandas DataFrame into a PostgreSQL database, logging and returning failed batches.

    Parameters:
        df (pd.DataFrame): The DataFrame to load.
        table_name (str): The target table name.
        engine: The SQLAlchemy database engine.
        if_exists (str): Behavior when the table exists. Options: 'fail', 'replace', 'append'.
        batch_size (int): The number of rows to insert in each batch.

    Returns:
        tuple: (successful_rows, failed_batches)
    """
    successful_rows = 0
    failed_batches = []

    # Divide the DataFrame into batches
    batches = [df[i:i + batch_size] for i in range(0, len(df), batch_size)]

    for i, batch in enumerate(batches):
        try:
            # Batch insert using to_sql
            batch.to_sql(
                table_name,
                engine,
                index=False,
                if_exists=if_exists,
                method="multi"  # Use the "multi" method for batch inserts
            )
            successful_rows += len(batch)
        except IntegrityError as e:
            engine.dispose()  # Dispose of the engine to avoid locked connections
            #print(f"Error during batch {i}: {e}")
            failed_batches.append({"batch_index": i, "rows": batch, "error": str(e)})

    return successful_rows, failed_batches

def reprocess_failed_batches(failed_batches, table_name, engine):
    """
    Reprocess rows from failed batches by inserting them individually into the database.

    Parameters:
        failed_batches (list): A list of dictionaries containing failed batch details.
        table_name (str): The target table name.
        engine: The SQLAlchemy database engine.

    Returns:
        tuple: (successful_rows, retry_failed_rows)
    """
    successful_rows = 0
    retry_failed_rows = []

    for failed_batch in failed_batches:
        batch_index = failed_batch["batch_index"]
        batch_rows = failed_batch["rows"]

        print(f"Retrying rows from failed batch {batch_index} individually...")
        for _, row in batch_rows.iterrows():
            try:
                row_df = pd.DataFrame([row])
                row_df.to_sql(
                    table_name,
                    engine,
                    index=False,
                    if_exists='append',
                    method="multi"
                )
                successful_rows += 1
            except IntegrityError as e:
                retry_failed_rows.append({
                    "row": row.to_dict(),
                    "batch_index": batch_index,
                    "error": str(e)
                })
                print(f"Error inserting row in batch {batch_index}: {e}")

    return successful_rows, retry_failed_rows



# In[30]:
dataframes = {
    "fc_locations": df_locations_renamed,
    "fc_filming_locations": df_filming_locations_renamed,
    "fc_restaurants": df_restaurants_renamed,
    "fc_actors": df_actors_renamed,
    "fc_genres": df_genres_renamed,
    "fc_genres_movies": df_movies_genres_renamed,
    "fc_actors_movies": df_movies_actors_renamed,
    "fc_movies": df_movies_cleaned_renamed,
}

for name, dataframe in dataframes.items():
    print(f"Number of rows in {name}: {len(dataframe)}")


# In[31]:
print("\nLoading movies...")
successful_movies, failed_movies = load_dataframe_to_postgres_batch(df_movies_cleaned_renamed,'fc_movies',engine,batch_size=500)

print("Loading locations...")
successful_locations, failed_locations = load_dataframe_to_postgres_batch(df_locations_renamed,'fc_locations',engine,batch_size=50)

print("\nLoading restaurants...")
successful_restaurants, failed_restaurants = load_dataframe_to_postgres_batch(df_restaurants_renamed,'fc_restaurants',engine,batch_size=20)

print("\nLoading Genres...")
successful_genres, failed_genres = load_dataframe_to_postgres_batch(df_genres_renamed,'fc_genres',engine,batch_size=5)

print("\nLoading Actors...")
successful_actors, failed_actors = load_dataframe_to_postgres_batch(df_actors_renamed,'fc_actors',engine,batch_size=5000)

print("\nLoading Movies-Genres...")
successful_genres_movies, failed_genres_movies = load_dataframe_to_postgres_batch(df_movies_genres_renamed, 'fc_genres_movies', engine, batch_size=800)

print("\nLoading Movies-Actors...")
successful_movies_actors, failed_movies_actors = load_dataframe_to_postgres_batch(df_movies_actors_renamed, 'fc_actors_movies', engine, batch_size=8000)

print("\nLoading Filming Locations...")
successful_filming_locations, failed_filming_locations = load_dataframe_to_postgres_batch(df_filming_locations_renamed, 'fc_filming_locations', engine, batch_size=50)


# In[32]:


# Summary Logs
print(f"Locations: {(successful_locations)} inserted, {len(failed_locations)} failed")
print(f"Restaurants: {(successful_restaurants)} inserted, {len(failed_restaurants)} failed")
print(f"Movies: {(successful_movies)} inserted, {len(failed_movies)} failed")
print(f"Genres: {(successful_genres)} inserted, {len(failed_genres)} failed")
print(f"Actors: {(successful_actors)} inserted, {len(failed_actors)} failed")
print(f"Movies-Genres: {(successful_genres_movies)} inserted, {len(failed_genres_movies)} failed")
print(f"Movies-Actors: {(successful_movies_actors)} inserted, {len(failed_movies_actors)} failed")
print(f"Filming Locations: {(successful_filming_locations)} inserted, {len(failed_filming_locations)} failed")


# Code to reprocess failed batches
# ```python
# # Reprocess failed batches later
# if failed_locations:
#         reprocess_successful, retry_failed_rows = reprocess_failed_batches(failed_locations, "fc_locations", engine)
# ```
# 

# In[33]:
def update_geography(engine):
    """
    Updates the loc_geography column in fc_locations table using ST_SetSRID and ST_MakePoint.
    """

    update_query = """
    UPDATE fc_locations
    SET
        loc_geography = ST_SetSRID(ST_MakePoint(loc_longitude, loc_latitude), 4326)::geography
    WHERE
        loc_latitude IS NOT NULL
      AND loc_longitude IS NOT NULL;
    """
    try:
        with engine.begin() as connection:
            connection.execute(text(update_query))

    except SQLAlchemyError as e:
        print(f"An error occurred while updating loc_geography: {e}")

update_geography(engine)

logger.info("Data loaded successfully into the database.")
logger.info("Data ingestion pipeline completed successfully!")