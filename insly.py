import requests
from datetime import datetime, timedelta
import os
from helper import format_objects_to_html

retry_buffer = []
INSLY_TOKEN = os.getenv('BEARER_TOKEN')


def get_customer_policy(oid, counter):
    url = 'https://vingo-api.insly.com/api/customer/getpolicy'
    body = {"customer_oid": oid, "get_inactive": 0}
    headers = {'Authorization': f'Bearer {INSLY_TOKEN}'}

    response = requests.post(url=url, json=body, headers=headers)

    if response.status_code == 200:
        data = response.json()
        customer_info = []
        policy_info = []
        address_info = []
        object_info =[]
        customer_info_added = False
        current_date = datetime.today()
        future_date = current_date + timedelta(days=21)

        if 'policy' in data:
            for policy in data['policy']:
                exp_date = datetime.strptime(policy['policy_date_end'], "%d.%m.%Y")

                if current_date <= exp_date <= future_date:
                    print(f"#{counter} Customer {oid}: Policy {policy['policy_no']} ends within 21 days.")
                    if not customer_info_added:
                        if 'address' in data:
                            address = data['address'][0]
                            a_value = address['customer_address']
                            a_country = address['customer_address_country']
                            a_postal_code = address['customer_address_zip']
                            address_info.append((a_value, a_country, a_postal_code))

                        c_name = data.get('customer_name')
                        c_email = data.get('customer_email')
                        c_phone = data.get('customer_phone')
                        c_type = data.get('customer_type')
                        customer_info.append((oid, c_name, c_email, c_phone, c_type))
                        customer_info_added = True

                    statuses = [installment['policy_installment_status'] for installment in policy['payment']]

                    if all(status == 12 for status in statuses):
                        p_installment_status = 'won'
                    elif all(status == 99 for status in statuses):
                        p_installment_status = 'lost'
                    else:
                        p_installment_status = 'open'

                    p_title = data.get('customer_name') + " " + policy.get('policy_no')
                    p_currency = policy.get('policy_premium_currency')
                    p_summ = policy.get('policy_payment_sum')
                    p_description = policy.get('policy_description')
                    p_date_end = datetime.strptime(policy.get('policy_date_end'), "%d.%m.%Y").strftime("%Y-%m-%d")
                    p_number = policy.get('policy_no')
                    p_insurer = get_classifier_value(value=policy.get('policy_insurer'), classifier_field_name='insurer')
                    p_type = get_classifier_value(value=policy.get('policy_type'), classifier_field_name='product')
                    policy_info.append((p_title, p_currency, p_summ, p_description, p_date_end, p_number, p_insurer,
                                        p_installment_status, p_type))

                    object_info.append(get_policy_object(policy.get('policy_oid')))

                else:
                    print(f"#{counter} Customer {oid}: Policy {policy['policy_no']} doesn't suit our needs.")
        else:
            print(f"#{counter} Customer {oid}: No policies found.")

        return customer_info, policy_info, address_info, object_info

    elif response.status_code == 429:
        retry_buffer.append((oid, counter))

    print(f"'get_customer_policy': Request failed with status code {response.status_code}")
    return [], [], [], []



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


def get_classifier_value(value, classifier_field_name: str):
    url = 'https://vingo-api.insly.com/api/policy/getclassifier'
    headers = {'Authorization': f'Bearer {INSLY_TOKEN}'}

    response = requests.post(url=url, json={}, headers=headers)

    if response.status_code == 200:
        data = response.json()

        return data.get(classifier_field_name, {}).get(value, value)
    else:
        print(f"'get_policy_classifiers': Request failed with status code {response.status_code}")
        return None


def get_policy_object(policy_oid):
    url = 'https://vingo-api.insly.com/api/policy/getpolicy'
    body = {"policy_oid": policy_oid, "return_objects": "1"}
    headers = {'Authorization': f'Bearer {INSLY_TOKEN}'}

    response = requests.post(url=url, json=body, headers=headers)

    if response.status_code == 200:
        data = response.json()
        if "objects" in data and data["objects"]:
            return format_objects_to_html(data["objects"])
        else:
            return "<p>No objects found for this policy.</p>"
    else:
        print(f"'get_policy_object': '{policy_oid}' Request failed with status code {response.status_code}")
        print(response.json())