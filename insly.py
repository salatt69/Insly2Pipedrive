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
    body = {"customer_oid": oid, "get_inactive": 0}
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
                exp_date = datetime.strptime(policy['policy_date_end'], "%d.%m.%Y")

                if not customer_info_added:
                    if 'address' in data:
                        address = data['address'][0]
                        a_value = address['customer_address']
                        a_country = address['customer_address_country']
                        a_postal_code = address['customer_address_zip']
                    else:
                        a_value = 'N/A'
                        a_country = 'N/A'
                        a_postal_code = 'N/A'
                    address_info.append((a_value, a_country, a_postal_code))

                    if data['broker_person_oid'] != 0:
                        c_owner = get_broker_person_fax(data['broker_person_oid'])

                        if c_owner is None:
                            c_owner = DEFAULT_OWNER
                        else:
                            c_owner = int(c_owner)

                    else:
                        c_owner = DEFAULT_OWNER

                    c_name = data.get('customer_name')
                    c_email = data.get('customer_email')
                    c_phone = data.get('customer_phone')
                    c_type = data.get('customer_type')
                    customer_info.append((oid, c_name, c_email, c_phone, c_type, c_owner))

                    customer_info_added = True

                p_title = data.get('customer_name') + " " + policy.get('policy_no')
                p_currency = policy.get('policy_premium_currency')
                p_summ = policy.get('policy_payment_sum')
                p_description = policy.get('policy_description')
                p_date_end = exp_date.strftime("%Y-%m-%d")
                p_number = policy.get('policy_no')
                p_insurer = get_classifier_value(value=policy.get('policy_insurer'),
                                                 classifier_field_name='insurer')
                p_type = get_classifier_value(value=policy.get('policy_type'),
                                              classifier_field_name='product')

                if current_date <= exp_date < future_date:
                    print(f"#{counter} Customer {oid}: Policy {policy['policy_no']} ends within 21 days.")
                    p_installment_status = 'open'

                else:
                    print(f"#{counter} Customer {oid}: Policy {policy['policy_no']} outside of the days range.")

                    statuses = [installment['policy_installment_status'] for installment in policy['payment']]

                    if all(status == 12 for status in statuses):
                        p_installment_status = 'won'
                    elif all(status == 99 for status in statuses):
                        p_installment_status = 'lost'
                    else:
                        p_installment_status = 'open'

                policy_info.append((p_title, p_currency, p_summ, p_description, p_date_end, p_number, p_insurer,
                                    p_installment_status, p_type))
                object_info.append(get_policy_object(policy.get('policy_oid')))

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
        # print(f'broker_oid: {broker_oid} => Unable to find broker person.')
        return

    broker_data = BROKER_JSON['person'][str(broker_oid)]

    if not 'broker_person_fax' in broker_data:
        # print(f"broker_oid: {broker_oid} => Doesn't have 'broker_person_fax' field.")
        return

    if broker_data['broker_person_fax'] == '':
        # print(f"broker_oid: {broker_oid} => 'broker_person_fax' field is empty.")
        return

    if broker_data['broker_person_fax'] == 'Pipedrive':
        # print(f"broker_oid: {broker_oid} => 'broker_person_fax' field is Pipedrive.")
        return

    return broker_data['broker_person_fax']


def get_broker_json():
    url = 'https://vingo-api.insly.com/api/system/getperson'
    headers = {'Authorization': f'Bearer {INSLY_TOKEN}'}

    response = requests.post(url=url, json={}, headers=headers)

    if response.status_code == 200:
        return response.json()

    else:
        print(f"'get_broker_json': Request failed with status code {response.status_code}")
        print(response.json())