import os
import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials


def authenticate():
    """
    Authenticates the application with the Google Sheets API using a service account
    and returns an authenticated gspread client.

    .. rubric:: Behavior
    - Reads the service account credentials from the JSON keyfile specified in the environment variable `KEYFILE_PATH`.
    - Uses the credentials to authorize the application for accessing Google Sheets and Google Drive.
    - Returns an authenticated `gspread` client that can be used to interact with Google Sheets.

    Notes:
        - The service account credentials must be stored in a JSON file, and the path to this file should be set in the environment variable `KEYFILE_PATH`.
        - This function requires access to both the Google Sheets API and the Google Drive API.

    Returns:
        gspread.client.Client: An authenticated gspread client object, which can be used to interact with Google Sheets.
    """
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(os.getenv('KEYFILE_PATH'), scope)
    client = gspread.authorize(creds)
    return client


def read_data_from_worksheet(start_row=1, custom_column=1, sheet_number=1):
    """
    Authenticates with Google Sheets, retrieves data from the first worksheet of a specified spreadsheet,
    and returns it as a pandas DataFrame.

    .. rubric:: Behavior
    - Calls the `authenticate()` function to get an authenticated gspread client.
    - Opens the Google Spreadsheet specified by the environment variable `SPREADSHEET_NAME`.
    - Retrieves all values from the first worksheet of the spreadsheet.
    - Skips the first three rows and uses the fourth row as the column headers.
    - Converts the resulting list of lists into a pandas DataFrame.

    Notes:
        - The environment variable `SPREADSHEET_NAME` must be set with the name of the spreadsheet to be accessed.
        - The data starting from the fourth row (index 3) is used, with the column headers taken from the same row.
        - This function assumes that the first three rows are not part of the data and should be skipped.

    Returns:
        pandas.DataFrame: A DataFrame containing the data from the worksheet, with the appropriate column headers.
    """
    client = authenticate()
    spreadsheet = client.open(os.getenv('SPREADSHEET_NAME'))
    worksheet = spreadsheet.get_worksheet(sheet_number-1)

    data = worksheet.get_all_values()

    # Convert the list of lists into a pandas DataFrame and skip unnecessary rows
    df = pd.DataFrame(data[start_row-1:], columns=data[custom_column-1])

    return df


def process_table_policies(pd, p_no, i, ds, deal_id):
    import time
    from helper import fetch_non_api_data

    results = pd.Get.details_of_deal(deal_id)

    if not results:
        print(f"#{i + 1} P_NO: {p_no} => Policy not found in the table.")
        return

    for idx, (deal_id, title, client_name, status) in enumerate(results, start=1):
        print(f"#{i + 1}.{idx} P_NO: {p_no} => Processing deal ID {deal_id}")

        info = fetch_non_api_data(p_no, ds[0], ds[1], ds[2], client_name)
        pd.Update.deal_custom_fields(deal_id, info, status)

        time.sleep(0.2)
