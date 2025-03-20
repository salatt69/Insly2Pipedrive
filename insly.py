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
            current_date = datetime.today()
            future_date = current_date + timedelta(days=21)

            if BROKER_JSON is None:
                BROKER_JSON = get_broker_json()

            if 'policy' not in data:
                print(f"#{counter} Customer {oid}: No policies found.")
                return [], [], [], []

            for policy in data['policy']:
                time.sleep(0.5)
                exp_date = datetime.strptime(policy['policy_date_end'], "%d.%m.%Y")

                if exp_date < current_date or current_date <= exp_date < future_date:
                    if exp_date < current_date:
                        print(f"#{counter} Customer {oid}: Policy {policy['policy_no']} closed.")
                    else:
                        print(f"#{counter} Customer {oid}: Policy {policy['policy_no']} ends within 21 days.")

                    if not customer_info_added:
                        fetched_a_info, fetched_c_info = fetch_customer_data(data)
                        address_info.append(fetched_a_info)
                        customer_info.append(fetched_c_info)
                        customer_info_added = True

                    fetched_p_info, fetched_o_info = fetch_policy_data(data, policy)

                    fetched_p_info = list(fetched_p_info)

                    if exp_date < current_date:
                        statuses = [installment['policy_installment_status'] for installment in policy['payment']]
                        if all(status == 12 for status in statuses):
                            fetched_p_info[7] = 'won'
                        elif all(status == 99 for status in statuses):
                            fetched_p_info[7] = 'lost'
                        else:
                            fetched_p_info[7] = 'open'
                    else:
                        fetched_p_info[7] = 'open'

                    fetched_p_info = tuple(fetched_p_info)
                    policy_info.append(fetched_p_info)
                    object_info.append(fetched_o_info)

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
    if str(broker_oid) not in BROKER_JSON['person']:
        return

    broker_data = BROKER_JSON['person'][str(broker_oid)]

    if not 'broker_person_name' in broker_data:
        return

    return broker_data['broker_person_name']


def get_broker_json():
    url = 'https://vingo-api.insly.com/api/system/getperson'
    headers = {'Authorization': f'Bearer {INSLY_TOKEN}'}

    response = requests.post(url=url, json={}, headers=headers)

    if response.status_code == 200:
        return response.json()

    else:
        print(f"'get_broker_json': Request failed with status code {response.status_code}")
        print(response.json())


def fetch_policy_data(data, policy):
    p_currency = policy.get('policy_premium_currency') or 'EUR'
    p_summ = policy.get('policy_payment_sum')
    p_description = policy.get('policy_description')
    p_date_end = datetime.strptime(policy.get('policy_date_end'), "%d.%m.%Y").strftime("%Y-%m-%d")
    p_number = policy.get('policy_no')
    p_installment_status = '' # It will be determined in the 'get_customer_policy'
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