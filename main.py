import datetime
import time
import os
import http.client
import traceback

from dotenv import load_dotenv
from pipedrive import Pipedrive
from insly import get_customer_policy, get_customer_list, is_it_fully_paid, is_it_expired
from helper import retry_requests, fetch_non_api_data
from spreadsheet_communication import read_data_from_worksheet, process_table_policies

def process_customer(pd, oid, counter):
    """
    Processes a customer's data by retrieving policies, creating or updating records in Pipedrive,
    and handling associated notes.

    Args:
        pd (Pipedrive): An instance of the Pipedrive API client.
        oid (int): The unique identifier of the customer.
        counter (int): A counter used for tracking the processing sequence.

    .. rubric:: Behavior
    - Calls `get_customer_policy(oid, counter)` to retrieve the customer's policies, address,
      and related objects from the Insly API.
    - If no customer data is found, the function terminates.
    - Determines if the customer is a company or an individual.
        If a company:
            - Searches for an existing organization in Pipedrive.
            - If found, updates it; otherwise, creates a new organization.
        If an individual:
            - Searches for an existing person in Pipedrive.
            - If found, updates it; otherwise, creates a new person.
    - Associates the retrieved policies with the identified organization/person:
        Searches for an existing deal in Pipedrive.\n
        If found, updates the deal; otherwise, creates a new deal.
    - Manages policy-related notes:
        Searches for an existing note linked to the deal.\n
        If found, updates it; otherwise, creates a new note.
    - Implements a retry mechanism for handling transient errors:
        Catches `http.client.RemoteDisconnected` errors and other unexpected exceptions.
        Uses an exponential backoff strategy, retrying the operation with increasing delays up to 60 seconds.

    Notes:
        - The function ensures that all customer policies and related objects are properly reflected in Pipedrive.
        - The retry mechanism prevents failures due to temporary API issues.
        - Customers without policies are skipped.
        - Pipedrive records (organizations, persons, deals, and notes) are either updated or created as needed.
        - Only policies that are already closed or ending within 21 days are processed.

    Returns:
        None: The function processes and updates records but does not return a value.
    """
    retry_requests()
    retry_delay = 5
    retry_attempts = 0
    max_retry_attempts = 2

    while max_retry_attempts != retry_attempts:
        try:
            customer_i, policy_i, address_i, object_i, payment_table = get_customer_policy(oid, counter)

            if not customer_i:
                return

            if customer_i[0][4] == 11:
                print(f"\t{customer_i[0][0]}: Company")
                org_id, org_name = pd.Search.organization(customer_i[0][0]) or (None, None)

                if org_id is None:
                    org_id = pd.Add.organization(customer_i[0], address_i[0])
                else:
                    pd.Update.organization(org_id, customer_i[0], address_i[0])

                entity_id, entype = org_id, 'org'

            else:
                print(f"\t{customer_i[0][0]}: Individual")
                person_id, person_name = pd.Search.person(customer_i[0][0]) or (None, None)

                if person_id is None:
                    person_id = pd.Add.person(customer_i[0])
                else:
                    pd.Update.person(person_id, customer_i[0])

                entity_id, entype = person_id, 'person'

            for i in range(len(policy_i)):
                deal_id, deal_title, _ = pd.Search.deal(policy_i[i][10]) or (None, None, None)

                if deal_id is None:
                    deal_id = pd.Add.deal(policy_i[i], entity_id, entype, customer_i[0][5])
                    print(f"Waiting for deal (id: {deal_id}) to be created...")
                    time.sleep(5)
                    process_table_policies(pd, policy_i[i][5], i, DATASET, deal_id)

                else:
                    pd.Update.deal(deal_id, policy_i[i], entity_id, entype)

                note_id = pd.Search.note(deal_id)

                if note_id is None:
                    pd.Add.note(object_i[i], deal_id, customer_i[0][5])
                else:
                    pd.Update.note(note_id, object_i[i], deal_id, customer_i[0][5])

                payment_table_note_id = pd.Search.payment_table_note(deal_id)

                if payment_table_note_id is None:
                    pd.Add.note(payment_table, deal_id, customer_i[0][5])
                else:
                    pd.Update.note(payment_table_note_id, payment_table, deal_id, customer_i[0][5])
            return

        except http.client.RemoteDisconnected as e:
            print(f"\nRemoteDisconnected error on customer {oid}: {e}")

        except ValueError as e:
            print(f"ValueError no customer {oid}: {e}")
            retry_attempts += 1
            print(f"\nAttempts left: {max_retry_attempts - retry_attempts}")

            if max_retry_attempts - retry_attempts == 0:
                print(f"Returning to #{counter + 1}\n")
                return

        except Exception as e:
            print(f"\nUnexpected error processing customer {oid}: {e}")
            print(traceback.format_exc())

        print(f"\nRetrying customer {oid} in {retry_delay} seconds...")
        time.sleep(retry_delay)
        retry_delay = min(retry_delay * 2, 60)


def main(pd):
    """
    Main function to retrieve customer data from Insly and process it in Pipedrive.

    .. rubric:: Behavior
    - Initializes a `Pipedrive` instance with the retrieved token from environment variables.
    - Calls `get_customer_list()` to fetch a list of customer OIDs from Insly.
    - If no customer OIDs are found, prints a message and exits.
    - Defines `start_from` to specify where to begin processing customers.
    - Extracts the remaining OIDs from `customer_oids` based on `start_from`.
    - Iterates through each remaining customer OID:
        Calls `process_customer(pd, oid, i)` to process the customer and their policies.\n
        Introduces a 1-second delay between processing each customer to avoid rate limits.

    Notes:
        - The script processes customers sequentially, starting from `start_from`.
        - If interrupted or restarted, `start_from` can be adjusted to resume from a specific OID.
        - The function ensures that all customers in the retrieved list are processed.
        - A delay is added to prevent excessive API requests that might trigger rate limiting.

    Returns:
        None: The function executes the pipeline but does not return a value.
    """
    print('Initializing environment...')
    global DATASET

    print('Fetching data from table...')
    data = read_data_from_worksheet(start_row=5, custom_column=4, sheet_number=1)
    seller_data = read_data_from_worksheet(start_row=2, sheet_number=2)
    policy_on_attb_data = read_data_from_worksheet(start_row=2, sheet_number=3)

    DATASET = (data, seller_data, policy_on_attb_data)

    print('Starting program...')
    customer_oids = get_customer_list()
    if not customer_oids:
        print("No customer OIDs found. Exiting.")
        return

    start_from = 18616

    remaining_oids = customer_oids[start_from - 1:]

    print(f"\n{len(remaining_oids)} OIDs ready!\n")

    for i, oid in enumerate(remaining_oids, start=start_from):
        process_customer(pd, oid, i)
        time.sleep(1)


def filtered_auto_close(pd):
    print('Fetching filtered deals...')
    filtered_deals = pd.Search.all_deals(filter_id=107)
    print(f"{len(filtered_deals)} deals found!\n")

    for i in range(len(filtered_deals)):
        policy_oid = filtered_deals[i].get('policy')
        deal_id = filtered_deals[i].get('id')

        print(f"#{i + 1} P_OID: {policy_oid}")

        if is_it_fully_paid(policy_oid):
            if is_it_expired(policy_oid):
                pd.Update.deal_status(deal_id, 'won')
            else:
                print(f"\tNot expired")
        else:
            print(f"\tNot fully paid")
        time.sleep(1)


def update_deals_with_no_seller(pd):
    cache = []
    print('Fetching filtered deals...')
    filtered_deals = pd.Search.all_deals(filter_id=74)
    print(f"{len(filtered_deals)} deals found!\n")

    for i in range(len(filtered_deals)):
        policy_oid = filtered_deals[i].get('policy')
        deal_id = filtered_deals[i].get('id')
        policy_number = filtered_deals[i].get('policy_number')

        if policy_number == 'Policy number is missing.':
            continue
            
        if policy_number in cache:
            print(f"Skipping {policy_number}. Have already been processed.")
            continue
        else:
            cache.append(policy_number)

        process_table_policies(pd, policy_number, i, DATASET, deal_id)

        time.sleep(1)
        
def run_daily():
    """
    Runs the `main()` function once a day, at midnight UTC, in an infinite loop.

    .. rubric:: Behavior
    - Continuously runs the `main()` function in a loop.
    - After each execution of `main()`, calculates the time until the next midnight UTC.
    - Pauses the execution using `time.sleep()` for the calculated duration, ensuring that the next `main()` run happens at midnight UTC.
    - Prints a message indicating the script is sleeping until the next scheduled run and then prints a message when it's running again.

    Notes:
        - The function uses `datetime` to calculate the exact number of seconds until midnight UTC.
        - The loop ensures that `main()` is executed once per day, and any errors or exceptions in `main()` will not stop the loop.

    Returns:
        None: The function does not return any value, it repeatedly calls `main()` at scheduled intervals.
    """
    load_dotenv()
    pd_token = os.getenv('PIPEDRIVE_TOKEN')
    pd = Pipedrive(pd_token)

    while True:
        try:
            now = datetime.datetime.now()
            if now.weekday() == 5:
                print("It's Saturday!")
                filtered_auto_close(pd)
            else:
                print("It's not Saturday.")
                main(pd)
                update_deals_with_no_seller(pd)

        except Exception as e:
            print(f"An error occurred during main(): {e}")
            print(traceback.format_exc())

        now = datetime.datetime.now()
        next_midnight = now.replace(hour=0, minute=0, second=0, microsecond=0) + datetime.timedelta(days=1)
        sleep_time = (next_midnight - now).total_seconds()
        print(f"Sleeping until 00:00...")
        time.sleep(sleep_time)
        print(f"\n\tRUNNING AGAIN!\n")


if __name__ == '__main__':
    run_daily()
