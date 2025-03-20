import json

import requests
from datetime import datetime
from helper import is_email_valid, truncate_utf8, extract_valid_phone

BASE_URL_V2 = 'https://api.pipedrive.com/api/v2'
BASE_URL_V1 = 'https://api.pipedrive.com/v1'
PIPEDRIVE_TOKEN = None

# Custom fields keys
INSLY_PERSON_OID = '86cae975675fb340afc1574e4743ae2f91604c62'
INSLY_ORGANIZATION_OID = '56fb82b7bf51f92fa7bb075d6225b240aca335c4'
RENEWED_OFFER_QUANTITY = '047f09d9770a057ee11f1285d1a9040753396a73'
RENEWAL_START_DATE = '277fae927484ea6779a715627105166d61593f96'
SELLER = 'dd13df304df939c49502d228441a544b2d7540a4'
POLICY_NO = '30bbd24791ef12c955ed795a6e93d64c4fd31fa1'
RENEWAL_POLICY_QUANTITY = '31106618eed52b54ca1e9a25eb8113fea358257f'
PRODUCT = '361d053d9234bc515f0884ecb4a12958c3b50574'
POLICY_ON_ATTB = '5b82893d6b640fa4e3ff36cfd44520ec569b73e8'
OBJECTS = '6981d2e1dc3d0212c5e581a4d627a18ac976f83f'
END_DATE = 'bee031bba9bdbeec53a9f85186f2a9f853fa8809'
INSURER = 'd897fe9647fdb08f70ff8abacf75a4e1c6078c5c'
REGISTRATION_CERTIFICATE_NO = 'eeefe1dfe026ffd8d13ce4a57af8650e8c8fb20a'
RENEWAL = 'f21e42e467108b3c0fce3f32834aa5ba48a5bad3'
RENEWED_POLICY_INSURER = 'fd5b6d71087035a3a5661481117eca35f36c8f15'
STATUS = '0cbadc7b01827c2ace7dfae87f7c710178dcdc42'
ORG_PHONE_NUMBER = '2f6ca1565cb01c3be96e09155b7d3ad3ed90a22f'
ORG_MOBILE_PHONE_NUMBER = '3a029778616fb8d7b46a80cb257b120fc39ebbb9'
ORG_EMAIL = '79df0042ead91e4d68adfae2c95f7a05cf6fb91f'
ORG_REGISTRATION_NUMBER = '572f5c7d9e53e4e3ba6aef777a2cb5bccd29d0d0'
POLICY_OID = 'a9bba6a79606e925f7682d1884c7ed8829cdf5e6'


class Pipedrive:
    def __init__(self, token: str):
        if not token:
            raise ValueError('API token is required!')
        global PIPEDRIVE_TOKEN
        PIPEDRIVE_TOKEN = token

    @staticmethod
    def find_custom_field(custom_field_key, option_label):
        if not option_label:
            return

        # Find field_id by its key from 'custom_field_ids.json'
        with open('custom_field_ids.json', 'r') as f:
            ids = json.load(f)
        filed_id = ids.get(custom_field_key)

        url = f'{BASE_URL_V1}/dealFields/{filed_id}'
        params = {'api_token': PIPEDRIVE_TOKEN}

        response = requests.get(url=url, params=params)

        if response.status_code == 200:
            data = response.json()

            options = data["data"]["options"]
            for option in options:
                if option_label.lower() in option['label'].lower():
                    return option['id']

        else:
            print(f'Failed while searching for custom field: {custom_field_key}. Code: {response.status_code}')
            print(response.json())

        pass

    # Bodies for 'add' and 'update' functions
    @staticmethod
    def get_deal_body(policy_info_arr, entity_id, entype, deal_owner):
        body = {
            "title": policy_info_arr[0],
            "owner_id": deal_owner,
            "currency": policy_info_arr[1],
            "value": policy_info_arr[2],
            "expected_close_date": policy_info_arr[4],
            "status": policy_info_arr[7],
            "visible_to": 3,
            "custom_fields": {
                # RENEWED_OFFER_QUANTITY:         None,
                # RENEWAL_START_DATE:             None,
                SELLER: policy_info_arr[9],
                POLICY_NO: policy_info_arr[5],
                # RENEWAL_POLICY_QUANTITY:        None,
                PRODUCT: Pipedrive.find_custom_field(PRODUCT, policy_info_arr[8]),
                # POLICY_ON_ATTB:                 None,
                OBJECTS: truncate_utf8(policy_info_arr[3]),
                END_DATE: policy_info_arr[4],
                INSURER: Pipedrive.find_custom_field(INSURER, policy_info_arr[6]),
                # REGISTRATION_CERTIFICATE_NO:    None,
                # RENEWAL:                        None,
                # RENEWED_POLICY_INSURER:         None,
                # STATUS:                         None
                POLICY_OID: str(policy_info_arr[10])
            }
        }

        if policy_info_arr[7] == 'won':
            body['won_time'] = (datetime.strptime(policy_info_arr[4], "%Y-%m-%d")
                                .replace(hour=9, minute=0, second=0).strftime('%Y-%m-%dT%H:%M:%SZ'))
            body['stage_id'] = 5

        if entype == 'org':
            body["org_id"] = entity_id
        else:
            body["person_id"] = entity_id
        return body

    @staticmethod
    def get_organization_body(org_info, address_info):
        body = {
            "name": org_info[1],
            "owner_id": org_info[5],
            "visible_to": 3,
            "custom_fields": {
                INSLY_ORGANIZATION_OID: str(org_info[0]),
                ORG_EMAIL: org_info[2] if org_info[2] and is_email_valid(org_info[2]) else None,
                ORG_PHONE_NUMBER: extract_valid_phone(org_info[3]) if org_info[3] else None,
                ORG_MOBILE_PHONE_NUMBER: extract_valid_phone(org_info[6]) if org_info[6] else None,
                ORG_REGISTRATION_NUMBER: org_info[7] if org_info[7] else None
            }
        }

        # Conditionally add address if there is one
        if address_info:
            body['address'] = {
                "value": address_info[0],
                "country": address_info[1],
                "postal_code": address_info[2]
            }
        return body

    @staticmethod
    def get_person_body(info):
        body = {
            "name": info[1],
            "owner_id": info[5],
            "visible_to": 3,
            "custom_fields": {
                INSLY_PERSON_OID: info[0]
            }
        }

        if info[2] and is_email_valid(info[2]):
            body["emails"] = [
                {
                    "value": info[2],
                    "primary": True,
                    "label": "email"
                }
            ]

        body["phones"] = []

        work_number = extract_valid_phone(info[3])
        mobile_number = extract_valid_phone(info[6])
        primary_number = True

        if work_number:
            body["phones"].append({
                "value": work_number,
                "primary": primary_number,
                "label": "work"
            })
            primary_number = not primary_number

        if mobile_number:
            body["phones"].append({
                "value": mobile_number,
                "primary": primary_number,
                "label": "mobile"
            })

        if not body["phones"]:
            del body["phones"]
        return body

    @staticmethod
    def get_note_body(content, deal_id, note_owner):
        body = {
            "content": content,
            "deal_id": deal_id,
            "user_id": note_owner
        }
        return body

    # Class, that keeps all the search API calls
    class Search:
        @staticmethod
        def organization(insly_customer_oid):
            url = f'{BASE_URL_V2}/organizations/search?term={insly_customer_oid}'
            params = {'api_token': PIPEDRIVE_TOKEN, 'exact_match': 1}

            response = requests.get(url=url, params=params)

            if response.status_code == 200:
                data = response.json()

                if data['data']['items']:
                    items = data['data']['items']

                    for item in items:
                        org_id = item['item']['id']
                        org_name = item['item']['name']
                        return org_id, org_name

                else:
                    return None, None

            else:
                print(f"'search_organization': Request failed with status code {response.status_code}")
                print(response.json())

        @staticmethod
        def person(insly_customer_oid):
            url = f'{BASE_URL_V2}/persons/search?term={insly_customer_oid}'
            params = {'api_token': PIPEDRIVE_TOKEN, 'exact_match': 1}

            response = requests.get(url=url, params=params)

            if response.status_code == 200:
                data = response.json()
                if data['data']['items']:
                    items = data['data']['items']

                    for item in items:
                        person_id = item['item']['id']
                        person_name = item['item']['name']
                        return person_id, person_name

                else:
                    return None, None
            else:
                print(f"'search_person': Request failed with status code {response.status_code}")
                print(response.json())

        @staticmethod
        def deal(insly_policy_oid):
            url = f'{BASE_URL_V2}/deals/search?term={insly_policy_oid}'
            params = {'api_token': PIPEDRIVE_TOKEN, 'exact_match': 1}

            response = requests.get(url=url, params=params)

            if response.status_code == 200:
                data = response.json()
                if data['data']['items']:
                    items = data['data']['items']

                    for item in items:
                        deal_id = item['item']['id']
                        deal_title = item['item']['title']
                        return deal_id, deal_title

                else:
                    return None, None

            else:
                print(f"'search_deal': Request failed with status code {response.status_code}")
                print(response.json())

        @staticmethod
        def note(deal_id):
            url = f'{BASE_URL_V1}/notes?deal_id={deal_id}'
            params = {'api_token': PIPEDRIVE_TOKEN}

            response = requests.get(url=url, params=params)

            if response.status_code == 200:
                data = response.json()
                if data['data'] is not None:
                    return data["data"][0]["id"]

                return None
            else:
                print(f"'search_note': Request failed with status code {response.status_code}")
                print(response.json())

    # Class, that keeps all the add API calls
    class Add:
        @staticmethod
        def organization(org_info, address_info):
            url = f'{BASE_URL_V2}/organizations'
            params = {'api_token': PIPEDRIVE_TOKEN}
            body = Pipedrive.get_organization_body(org_info, address_info)

            response = requests.post(url=url, params=params, json=body)

            if response.status_code == 200:
                print(f'\t{response.json()['data']['id']}: Organization Added!')
                return response.json()['data']['id']
            else:
                print(f"'add_organization': '{org_info[0]}' Request failed with status code {response.status_code}")
                print(response.json())

        @staticmethod
        def person(info):
            url = f'{BASE_URL_V2}/persons'
            params = {'api_token': PIPEDRIVE_TOKEN}
            body = Pipedrive.get_person_body(info)

            response = requests.post(url=url, params=params, json=body)

            if response.status_code == 200:
                print(f'\t{response.json()['data']['id']}: Person added!')
                return response.json()['data']['id']

            else:
                print(f"'add_person': '{info[0]}' Request failed with status code {response.status_code}")
                print(response.json())

        @staticmethod
        def deal(policy_info_arr, entity_id, entype, deal_owner):
            url = f'{BASE_URL_V2}/deals'
            params = {'api_token': PIPEDRIVE_TOKEN}
            body = Pipedrive.get_deal_body(policy_info_arr, entity_id, entype, deal_owner)

            response = requests.post(url=url, params=params, json=body)

            if response.status_code == 200:
                print(f'\t{response.json()['data']['id']}: Deal added!')
                return response.json()['data']['id']
            else:
                print(f"'add_deal': Request failed with status code {response.status_code}")
                print(response.json())

        @staticmethod
        def note(content, deal_id, note_owner):
            url = f'{BASE_URL_V1}/notes'
            params = {'api_token': PIPEDRIVE_TOKEN}
            body = Pipedrive.get_note_body(content, deal_id, note_owner)

            response = requests.post(url=url, params=params, json=body)

            if response.status_code == 200 or response.status_code == 201:
                print(f'\t{response.json()['data']['id']}: Note added!')
            else:
                print(f"'add_note': Request failed with status code {response.status_code}")
                print(response.json())

    # Class, that keeps all the update API calls
    class Update:
        @staticmethod
        def organization(org_id, org_info, address_info):
            url = f'{BASE_URL_V2}/organizations/{org_id}'
            params = {'api_token': PIPEDRIVE_TOKEN}
            body = Pipedrive.get_organization_body(org_info, address_info)

            response = requests.patch(url=url, params=params, json=body)

            if response.status_code == 200:
                print(f'\t{response.json()['data']['id']}: Organization Updated!')
                pass
            else:
                print(f"'update_organization': '{org_info[0]}' Request failed with status code {response.status_code}")
                print(response.json())

        @staticmethod
        def person(person_id, info):
            url = f'{BASE_URL_V2}/persons/{person_id}'
            params = {'api_token': PIPEDRIVE_TOKEN}
            body = Pipedrive.get_person_body(info)

            response = requests.patch(url=url, params=params, json=body)

            if response.status_code == 200:
                print(f'\t{person_id}: Person updated!')
            else:
                print(f"'update_person': '{person_id}' Request failed with status code {response.status_code}")
                print(response.json())

        @staticmethod
        def deal(deal_id, policy_info_arr, entity_id, entype, deal_owner):
            url = f'{BASE_URL_V2}/deals/{deal_id}'
            params = {'api_token': PIPEDRIVE_TOKEN}
            body = Pipedrive.get_deal_body(policy_info_arr, entity_id, entype, deal_owner)

            response = requests.patch(url=url, params=params, json=body)

            if response.status_code == 200:
                print(f'\t{deal_id}: Deal updated!')
            else:
                print(f"'update_deal': '{deal_id}' Request failed with status code {response.status_code}")
                print(response.json())

        @staticmethod
        def note(note_id, content, deal_id, note_owner):
            url = f'{BASE_URL_V1}/notes/{note_id}'
            params = {'api_token': PIPEDRIVE_TOKEN}
            body = Pipedrive.get_note_body(content, deal_id, note_owner)

            response = requests.put(url=url, params=params, json=body)

            if response.status_code == 200:
                print(f'\t{response.json()['data']['id']}: Note updated!')
            else:
                print(f"'update_note': Request failed with status code {response.status_code}")
                print(response.json())