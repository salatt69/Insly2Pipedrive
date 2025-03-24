import requests
import time
import os
from datetime import datetime, timedelta
from helper import format_objects_to_html

INSLY_TOKEN = os.getenv('BEARER_TOKEN')
MAX_RETRIES = 10
RETRY_DELAY = 5
retry_buffer = []

# Pipedrive ID of user Darija (default value)
DEFAULT_OWNER = 22609901
BROKER_JSON = None


def get_customer_list():
    """
    Fetches the list of customer OIDs from the Insly API.

    Returns:
        list[int] | None: A list of customer OIDs if the request is successful, otherwise `None`.

    .. rubric:: Behavior
    - Sends a POST request to the Insly API endpoint to retrieve customer data.
    - Includes the authorization token in the request headers.
    - Parses the response JSON to extract customer OIDs.
    - If the request is successful (status code 200), returns a list of customer OIDs.
    - If the request fails, logs an error message and returns `None`.

    Note:
        - Uses the `INSLY_TOKEN` for authentication.
        - Expects the response JSON to contain a list of customers under the `customers` key.
    """
    url = 'https://vingo-api.insly.com/api/customer/getcustomerlist'
    headers = {
        'Authorization': f'Bearer {INSLY_TOKEN}'
    }

    print('Fetching OID\'s...')
    response = requests.post(url=url, json={}, headers=headers)

    if response.status_code == 200:
        data = response.json()

        customer_oids = [customer['customer_oid'] for customer in data['customers']]

        return customer_oids
    else:
        print(f"'get_customer_list': Request failed with status code {response.status_code}")
        return None


def get_customer_policy(oid, counter):
    """
    Retrieves and processes customer policy details.

    Args:
        oid (int): The customer ID.
        counter (int): A counter for tracking retries or processing steps.

    Returns:
        tuple[list, list, list, list]:
            - List of customer-related data tuples.
            - List of policy-related data tuples.
            - List of address-related data tuples.
            - List of formatted HTML representations of policy objects.

    .. rubric:: Behavior
    - Sends a request to fetch customer policy data.
    - If `BROKER_JSON` is not loaded, fetches it using `get_broker_json()`.
    - Iterates through customer policies and evaluates their expiration status.
    - If a policy is expired or ending within 21 days, it processes and formats data.
    - Calls `fetch_customer_data()` and `fetch_policy_data()` for extraction.
    - Determines policy installment status and assigns an appropriate category.
    - Returns structured lists of customer, policy, address, and policy object details.
    """
    url = 'https://vingo-api.insly.com/api/customer/getpolicy'
    body = {"customer_oid": oid, "get_inactive": 1}
    headers = {'Authorization': f'Bearer {INSLY_TOKEN}'}

    for attempt in range(MAX_RETRIES):
        response = requests.post(url=url, json=body, headers=headers)

        if response.status_code == 200:
            global BROKER_JSON

            data = response.json()
            customer_info = []
            policy_info = []
            address_info = []
            object_info = []
            customer_info_added = False
            latest_date = datetime(2024, 1, 1)
            current_date = datetime.today()
            future_date = current_date + timedelta(days=21)

            if BROKER_JSON is None:
                BROKER_JSON = get_broker_json()

            if 'policy' not in data:
                print(f"#{counter} Customer {oid}: No policies found.")
                return [], [], [], []

            for policy in data['policy']:
                p_date_end_raw = policy.get('policy_date_end', '')
                try:
                    exp_date = datetime.strptime(p_date_end_raw, "%d.%m.%Y")
                except ValueError:
                    print(f"#{counter} Skipping policy with invalid date '{p_date_end_raw}' for customer {oid}")
                    continue

                if latest_date <= exp_date < current_date or current_date <= exp_date < future_date:
                    if latest_date <= exp_date < current_date:
                        print(f"#{counter} Customer {oid}: Policy {policy['policy_no']} closed after {latest_date}.")
                    else:
                        print(f"#{counter} Customer {oid}: Policy {policy['policy_no']} ends within 21 days.")

                    if not customer_info_added:
                        fetched_a_info, fetched_c_info = fetch_customer_data(data)
                        address_info.append(fetched_a_info)
                        customer_info.append(fetched_c_info)
                        customer_info_added = True

                    fetched_p_info, fetched_o_info = fetch_policy_data(data, policy)

                    fetched_p_info = list(fetched_p_info)

                    if latest_date <= exp_date < current_date:
                        fetched_p_info[7] = 'lost'  # Default

                        if policy.get('payment'):
                            last_installment = max(policy['payment'], key=lambda x: x['policy_installment_num'])

                            if last_installment['policy_installment_num'] == policy['policy_installments']:
                                status = last_installment['policy_installment_status']

                                if status == 12:  # Fully paid
                                    fetched_p_info[7] = 'won'

                    elif current_date <= exp_date < future_date:
                        fetched_p_info[7] = 'open'  # Default

                        if policy.get('payment'):
                            last_installment = max(policy['payment'], key=lambda x: x['policy_installment_num'])

                            if last_installment['policy_installment_num'] == policy['policy_installments']:
                                status = last_installment['policy_installment_status']

                                if status == 12:  # Fully paid
                                    fetched_p_info[7] = 'won'
                                elif status == 99:  # Cancelled
                                    fetched_p_info[7] = 'lost'
                                else:               # Added, invoice created, partially paid
                                    fetched_p_info[7] = 'open'

                    fetched_p_info = tuple(fetched_p_info)
                    policy_info.append(fetched_p_info)
                    object_info.append(fetched_o_info)
                else:
                    print(f"#{counter} Customer {oid}: Policy {policy['policy_no']} out of range.")

            return customer_info, policy_info, address_info, object_info

        elif response.status_code == 429:
            print(f"Rate limit hit for get_customer_policy. Retrying in {RETRY_DELAY * (2 ** attempt)} seconds...")
            time.sleep(RETRY_DELAY * (2 ** attempt))
        else:
            print(f"'get_customer_policy': Request failed with status code {response.status_code}")
            return [], [], [], []

    print("Max retries exceeded for get_customer_policy.")
    return [], [], [], []


def get_classifier_value(value, classifier_field_name: str):
    """
    Retrieves the classifier value from the Insly API.

    Args:
        value (str): The key to look up within the classifier field.
        classifier_field_name (str): The name of the classifier field containing the mapping.

    Returns:
        str | None: The corresponding classifier value if found, otherwise returns the original `value` or `None` if the request fails.

    .. rubric:: Behavior
    - Sends a POST request to the Insly API to retrieve classifier mappings.
    - Extracts and returns the corresponding value from the classifier field if found.
    - If the request fails with a `429` status code, retries with exponential backoff.
    - If the request fails with another status code, logs an error message and returns `None`.
    - Stops retrying after exceeding `MAX_RETRIES`.

    Note:
        - Uses `INSLY_TOKEN` for authentication.
        - Implements exponential backoff with `RETRY_DELAY` to handle rate limits.
    """
    url = 'https://vingo-api.insly.com/api/policy/getclassifier'
    headers = {'Authorization': f'Bearer {INSLY_TOKEN}'}

    for attempt in range(MAX_RETRIES):
        response = requests.post(url=url, json={}, headers=headers)

        if response.status_code == 200:
            data = response.json()
            return data.get(classifier_field_name, {}).get(value, value)

        elif response.status_code == 429:
            print(f"Rate limit hit for get_classifier_value. Retrying in {RETRY_DELAY * (2 ** attempt)} seconds...")
            time.sleep(RETRY_DELAY * (2 ** attempt))
        else:
            print(f"'get_classifier_value': Request failed with status code {response.status_code}")
            return None

    print("Max retries exceeded for get_classifier_value.")
    return None


def get_policy_object(policy_oid):
    """
    Retrieves and formats policy objects as HTML.

    Args:
        policy_oid (str): The unique identifier of the policy.

    Returns:
        str: An HTML-formatted string containing policy object details or an error message.

    .. rubric:: Behavior
    - Sends a POST request to the Insly API to fetch policy details, including objects.
    - If objects are found, formats them into an HTML list using `format_objects_to_html`.
    - If no objects are found, returns a placeholder HTML message.
    - If the request fails with a `429` status code, retries with exponential backoff.
    - If the request fails with another status code, logs an error and returns an error message.
    - Stops retrying after exceeding `MAX_RETRIES`.

    Note:
        - Uses `INSLY_TOKEN` for authentication.
        - Implements exponential backoff with `RETRY_DELAY` to handle rate limits.
    """
    url = 'https://vingo-api.insly.com/api/policy/getpolicy'
    body = {"policy_oid": policy_oid, "return_objects": "1"}
    headers = {'Authorization': f'Bearer {INSLY_TOKEN}'}

    for attempt in range(MAX_RETRIES):
        response = requests.post(url=url, json=body, headers=headers)

        if response.status_code == 200:
            data = response.json()
            if "objects" in data and data["objects"]:
                return format_objects_to_html(data["objects"])
            else:
                return "<p>No objects found for this policy.</p>"

        elif response.status_code == 429:
            print(f"Rate limit hit for get_policy_object. Retrying in {RETRY_DELAY * (2 ** attempt)} seconds...")
            time.sleep(RETRY_DELAY * (2 ** attempt))
        else:
            print(f"'get_policy_object': '{policy_oid}' Request failed with status code {response.status_code}")
            return "<p>Error fetching policy objects.</p>"

    print("Max retries exceeded for get_policy_object.")
    return "<p>Error fetching policy objects.</p>"


def get_broker_person_fax(broker_oid):
    """
    Retrieves the fax number of a broker's person entry from `BROKER_JSON`.

    Args:
        broker_oid (str or int): The unique identifier of the broker.

    Returns:
        str or None: The broker's fax number if available; otherwise, None.

    .. rubric:: Behavior
    - Checks if `broker_oid` exists in `BROKER_JSON['person']`.
    - If `broker_person_fax` is missing, empty, or set to "Pipedrive", returns None.
    - Otherwise, returns the stored fax number.

    Note:
        - Assumes `BROKER_JSON` is a global dictionary containing broker data.
    """
    if str(broker_oid) not in BROKER_JSON['person']:
        return

    broker_data = BROKER_JSON['person'][str(broker_oid)]

    if not 'broker_person_fax' in broker_data:
        return

    if broker_data['broker_person_fax'] == '':
        return

    if broker_data['broker_person_fax'] == 'Pipedrive':
        return

    return broker_data['broker_person_fax']


def get_broker_person_name(broker_oid):
    """
    Retrieves the name of a broker's person entry from `BROKER_JSON`.

    Args:
        broker_oid (str or int): The unique identifier of the broker.

    Returns:
        str or None: The broker's name if available; otherwise, None.

    .. rubric:: Behavior
    - Checks if `broker_oid` exists in `BROKER_JSON['person']`.
    - If `broker_person_name` is missing, returns None.
    - Otherwise, returns the stored name.

    Note:
        - Assumes `BROKER_JSON` is a global dictionary containing broker data.
    """
    if str(broker_oid) not in BROKER_JSON['person']:
        return

    broker_data = BROKER_JSON['person'][str(broker_oid)]

    if not 'broker_person_name' in broker_data:
        return

    return broker_data['broker_person_name']


def get_broker_json():
    """
    Fetches broker-related data from the Insly API.

    Returns:
        dict or None: The JSON response containing broker data if the request is successful;
                      otherwise, None.

    .. rubric:: Behavior
    - Sends a POST request to the Insly API endpoint `system/getperson`.
    - Includes an authorization header with `INSLY_TOKEN`.
    - If the request succeeds (HTTP 200), returns the JSON response.
    - If the request fails, logs an error message and returns None.

    Note:
        - Assumes `INSLY_TOKEN` is a valid authentication token.
        - The function prints error details in case of failure.
    """
    url = 'https://vingo-api.insly.com/api/system/getperson'
    headers = {'Authorization': f'Bearer {INSLY_TOKEN}'}

    response = requests.post(url=url, json={}, headers=headers)

    if response.status_code == 200:
        return response.json()

    else:
        print(f"'get_broker_json': Request failed with status code {response.status_code}")
        print(response.json())


def fetch_policy_data(data, policy):
    """
    Extracts and formats policy-related data.

    Args:
        data (dict): Dictionary containing customer and broker information.
        policy (dict): Dictionary containing policy details.

    Returns:
        tuple[tuple, str]: - Key details about the policy.
                           - HTML-formatted policy objects.

    .. rubric:: Behavior
    - Extracts policy details such as currency, sum, description, end date, number, and OID.
    - Converts the policy end date from "DD.MM.YYYY" format to "YYYY-MM-DD".
    - Retrieves classified values for the insurer and policy type.
    - Fetches the broker's name using `get_broker_person_name()`.
    - Constructs a policy title using customer name, policy number, and policy type.
    - Retrieves policy objects in HTML format via `get_policy_object()`.
    """
    p_currency = policy.get('policy_premium_currency') or 'EUR'
    p_summ = policy.get('policy_payment_sum')
    p_description = policy.get('policy_description')
    p_date_end = datetime.strptime(policy.get('policy_date_end'), "%d.%m.%Y").strftime("%Y-%m-%d")
    p_number = policy.get('policy_no') or 'Policy number is missing.'
    p_installment_status = ''  # It will be determined in the 'get_customer_policy'
    p_insurer = get_classifier_value(value=policy.get('policy_insurer'), classifier_field_name='insurer')
    p_type = get_classifier_value(value=policy.get('policy_type'), classifier_field_name='product')
    p_broker_name = get_broker_person_name(data.get('broker_person_oid'))
    p_title = data.get('customer_name') + " - " + p_number + " - " + p_type
    p_oid = policy.get('policy_oid')

    policy_info = (p_title, p_currency, p_summ, p_description, p_date_end, p_number,
                   p_insurer, p_installment_status, p_type, p_broker_name, p_oid)
    object_info = get_policy_object(p_oid)

    return policy_info, object_info


def fetch_customer_data(data):
    """
    Extracts and formats customer-related data.

    Args:
        data (dict): Dictionary containing customer and broker details.

    Returns:
        tuple[tuple, tuple]: - Address details including value, country, and postal code
                             - Key customer details such as ID, name, contact information, and owner.

    .. rubric:: Behavior
    - Extracts customer address details if available; otherwise, assigns "N/A".
    - Retrieves broker owner information using `get_broker_person_fax()`.
    - If broker information is unavailable, defaults to `DEFAULT_OWNER`.
    - Parses customer-specific fields such as OID, name, email, phone numbers, type, and ID code.
    """
    if 'address' in data:
        address = data['address'][0]
        a_value = address['customer_address']
        a_country = address['customer_address_country']
        a_postal_code = address['customer_address_zip']
    else:
        a_value = 'N/A'
        a_country = 'N/A'
        a_postal_code = 'N/A'
    address_info = (a_value, a_country, a_postal_code)

    if data['broker_person_oid'] != 0:
        c_owner = get_broker_person_fax(data['broker_person_oid'])

        if c_owner is None:
            c_owner = DEFAULT_OWNER
        else:
            c_owner = int(c_owner)

    else:
        c_owner = DEFAULT_OWNER

    c_oid = int(data.get('customer_oid'))
    c_name = data.get('customer_name')
    c_email = data.get('customer_email')
    c_business_phone = data.get('customer_phone')
    c_personal_phone = data.get('customer_mobile')
    c_type = data.get('customer_type')
    c_idcode = data.get('customer_idcode')
    customer_info = (c_oid, c_name, c_email, c_business_phone,
                     c_type, c_owner, c_personal_phone, c_idcode)

    return address_info, customer_info