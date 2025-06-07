
import os
import requests
import pandas as pd

def extract_and_transform():
    """
    Fetch and process air quality measurements from the OpenAQ API.

    Returns
    -------
    df : pandas.DataFrame or None
        Dataframe containing the fetched measurements, or None if no data is fetched.
    """
        
    # It is worth noting that this sensor corresponds to one located in Logroño
    # It reports hourly measures of carbone monoxide mass, and the units are \mug / m^3
    # Its latest data (and some others from the same station) can be also 
    # retrieved from https://explore.openaq.org/locations/2162523

    api_url = "https://api.openaq.org/v3/sensors/7773515"
    
    header={"x-api-key" : os.environ['OPENAQ_KEY']}
    # Define the query parameters to API
    params = {
        "limit": 1000,
    }
    
    try:
        # Make the GET request
        response = requests.get(api_url, params=params, headers=header, timeout=120)
        response.raise_for_status()  # Raise exception for HTTP errors
        
        if response.status_code == 200:
            data = response.json()
            output = pd.json_normalize(data['results'])
            df = pd.DataFrame(output)
            
            if df.empty:
                print("Extracted dataframe is empty. No data to load.")
                return None

            # We convert the datetime information of last measure to recognisable format:
            df['latest.datetime.utc'] = pd.to_datetime(df['latest.datetime.utc'], errors='coerce')
            df['latest.datetime.local'] = pd.to_datetime(df['latest.datetime.local'], errors='coerce').dt.tz_convert('Europe/Madrid')
            
            return df

    except requests.exceptions.HTTPError as errh:
        print("Http Error:", errh)
    except requests.exceptions.ConnectionError as errc:
        print("Error Connecting:", errc)
    except requests.exceptions.Timeout as errt:
        print("Timeout Error:", errt)
    except requests.exceptions.RequestException as err:
        print("Error: Something Else", err)

if __name__ == "__main__":
    df = extract_and_transform()

    data_directory = os.path.join(os.getcwd(), "data")
    if not os.path.exists(data_directory):
        os.makedirs(data_directory)

    data_file = os.path.join(data_directory, "air_data.csv")

    if not os.path.exists (data_file):
        df.to_csv(data_file, index=False)
    else:
        df.to_csv(data_file, mode='a', header=False, index=False)
