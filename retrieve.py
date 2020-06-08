import pandas as pd
import requests


def get_count(api_url, credentials):
    """ Finds the cardinality of the dataset 
    
        api_url: str, in this case intakes or outcomes link
        
        credentials: dict, Socrata public and private tokens
        
        returns int
		
    """
    payload = {"$select": "count(*)"}
    return int(
        requests.get(api_url, headers=credentials, params=payload).json()[0]["count"]
    )


def get_full_dataset(api_url, credentials, limit=10000):
    """ Recreates whole dataset in a pandas DataFrame
    
        api_url: str, in this case intakes or outcomes link
        
        credentials: dict, Socrata public and private tokens
        
        limit: int, number of "results" per "page". 10k is least amount without overloading API and getting weird feature abberations from Socrata
        
    """

    offset = 0
    status_code = 200
    df = pd.DataFrame()

    payload = {"$order": ":id", "$limit": limit, "$offset": offset}

    while status_code == 200 and offset < get_count(api_url, credentials):

        print(f"offset={offset}, status={status_code}, shape={df.shape}")

        r = requests.get(intakes_api, headers=header, params=payload)
        df = pd.concat([df, pd.DataFrame(r.json())], ignore_index=True)

        offset += limit
        payload.update({"$offset": offset})
        status_code = r.status_code

    return df
