import json

import requests
from datetime import datetime
from helper import is_email_valid, is_phone_valid

BASE_URL_V2 = 'https://api.pipedrive.com/api/v2'
BASE_URL_V1 = 'https://api.pipedrive.com/v1'
PIPEDRIVE_TOKEN = None
CREATOR_USER_ID = 22609901  # Darija

# Custom fields keys
INSLY_OID = '86cae975675fb340afc1574e4743ae2f91604c62'
RENEWED_OFFER_QUANTITY = '047f09d9770a057ee11f1285d1a9040753396a73'
RENEWAL_START_DATE = '277fae927484ea6779a715627105166d61593f96'
SELLER = '2c136c23787bac259a1e16750b653b95f42b4a9b'
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

    class Search:
        @staticmethod
        def organization(org_name):
            url = f'{BASE_URL_V2}/organizations/search?term={org_name}'
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
        def person(person_name):
            url = f'{BASE_URL_V2}/persons/search?term={person_name}'
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
        def deal(deal_title):
            url = f'{BASE_URL_V2}/deals/search?term={deal_title}'
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

    class Add:
        @staticmethod
        def organization(org_name, oid, address_info_array):
            add_time = datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')

            url = f'{BASE_URL_V2}/organizations'
            params = {'api_token': PIPEDRIVE_TOKEN}
            body = {
                "name": org_name,
                "owner_id": CREATOR_USER_ID,
                "add_time": add_time,
                "visible_to": 3
            }

            # Conditionally add address if there is one
            if address_info_array:
                body['address'] = {
                    "value": address_info_array[0][0],
                    "country": address_info_array[0][1],
                    # "locality": "Sunnyvale",
                    # "sublocality": "Downtown",
                    "postal_code": address_info_array[0][2]
                }

            response = requests.post(url=url, params=params, json=body)

            if response.status_code == 200:
                print(f'{response.json()['data']['id']}: Organization Added!')
                return response.json()['data']['id']
            else:
                print(f"'add_organization': '{oid}' Request failed with status code {response.status_code}")
                print(response.json())

        @staticmethod
        def person(info):
            url = f'{BASE_URL_V2}/persons'
            body = {
                "name": info[1],
                "owner_id": CREATOR_USER_ID,
                "visible_to": 3,
                "custom_fields": {
                    INSLY_OID: info[0]
                }
            }
            params = {'api_token': PIPEDRIVE_TOKEN}

            # Conditionally add emails if valid
            if info[2] and is_email_valid(info[2]):
                body["emails"] = [
                    {
                        "value": info[2],
                        "primary": True,
                        "label": "email"
                    }
                ]

            # Conditionally add phones if valid
            if info[3] and is_phone_valid(info[3]):
                body["phones"] = [
                    {
                        "value": info[3],
                        "primary": True,
                        "label": "phone_number"
                    }
                ]

            response = requests.post(url=url, params=params, json=body)

            if response.status_code == 200:
                print(f'{response.json()['data']['id']}: Person added!')
                return response.json()['data']['id']

            else:
                print(f"'add_person': '{info[0]}' Request failed with status code {response.status_code}")
                print(response.json())

        @staticmethod
        def deal(policy_info_arr, entity_id, entype):
            url = f'{BASE_URL_V2}/deals'
            params = {'api_token': PIPEDRIVE_TOKEN}
            body = {
                "title": policy_info_arr[0],
                "owner_id": CREATOR_USER_ID,
                "currency": policy_info_arr[1],
                "value": policy_info_arr[2],
                "expected_close_date": policy_info_arr[4],
                "status": policy_info_arr[7],
                "visible_to": 3,
                "custom_fields": {
                    # RENEWED_OFFER_QUANTITY:         None,
                    # RENEWAL_START_DATE:             None,
                    # SELLER:                         None,
                    POLICY_NO: policy_info_arr[5],
                    # RENEWAL_POLICY_QUANTITY:        None,
                    PRODUCT: Pipedrive.find_custom_field(PRODUCT, policy_info_arr[8]),
                    # POLICY_ON_ATTB:                 None,
                    OBJECTS: policy_info_arr[3],
                    END_DATE: policy_info_arr[4],
                    INSURER: Pipedrive.find_custom_field(INSURER, policy_info_arr[6]),
                    # REGISTRATION_CERTIFICATE_NO:    None,
                    # RENEWAL:                        None,
                    # RENEWED_POLICY_INSURER:         None,
                    # STATUS:                         None
                }
            }

            if policy_info_arr[7] == 'won':
                body['won_time'] = (datetime.strptime(policy_info_arr[4], "%Y-%m-%d")
                                    .replace(hour=9, minute=0,second=0).strftime('%Y-%m-%dT%H:%M:%SZ'))
                body['stage_id'] = 5

            # elif policy_info_arr[7] == 'lost':
            #     body['lost_time'] = (datetime.strptime(policy_info_arr[4], "%Y-%m-%d")
            #                          .replace(hour=9, minute=0, second=0).strftime('%Y-%m-%dT%H:%M:%SZ'))
            #     body['stage_id'] = 1

            else:
                body['won_time'] = ''
                body['lost_time'] = ''

            if entype == 'org':
                body["org_id"] = entity_id
            else:
                body["person_id"] = entity_id

            response = requests.post(url=url, params=params, json=body)

            if response.status_code == 200:
                print(f'{response.json()['data']['id']}: Deal added!')
                return response.json()['data']['id']
            else:
                print(f"'add_deal': Request failed with status code {response.status_code}")
                print(response.json())

        @staticmethod
        def note(content, deal_id):
            url = f'{BASE_URL_V1}/notes'
            params = {'api_token': PIPEDRIVE_TOKEN}
            body = {
                "content": content,
                "deal_id": deal_id,
                "user_id": CREATOR_USER_ID
            }

            response = requests.post(url=url, params=params, json=body)

            if response.status_code == 200 or response.status_code == 201:
                print(f'{response.json()['data']['id']}: Note added!')
                # return response.json()['data']['id']
            else:
                print(f"'add_note': Request failed with status code {response.status_code}")
                print(response.json())

    class Update:
        @staticmethod
        def organization(org_id, org_name, oid):
            url = f'{BASE_URL_V2}/organizations/{org_id}'
            params = {'api_token': PIPEDRIVE_TOKEN}
            body = {
                "name": org_name,
                "owner_id": CREATOR_USER_ID,
                "visible_to": 3
            }

            response = requests.patch(url=url, params=params, json=body)

            if response.status_code == 200:
                print(f'{oid}: Updated!')
                pass
            else:
                print(f"'update_organization': '{oid}' Request failed with status code {response.status_code}")
                print(response.json())

        @staticmethod
        def person(person_id, info):
            url = f'{BASE_URL_V2}/persons/{person_id}'
            params = {'api_token': PIPEDRIVE_TOKEN}
            body = {
                "name": info[1],
                "owner_id": CREATOR_USER_ID,
                "visible_to": 3,
                "custom_fields": {
                    INSLY_OID: info[0]
                }
            }

            # Conditionally add emails if valid
            if info[2] and is_email_valid(info[2]):
                body["emails"] = [
                    {
                        "value": info[2],
                        "primary": True,
                        "label": "email"
                    }
                ]

            # Conditionally add phones if valid
            if info[3] and is_phone_valid(info[3]):
                body["phones"] = [
                    {
                        "value": info[3],
                        "primary": True,
                        "label": "phone_number"
                    }
                ]

            response = requests.patch(url=url, params=params, json=body)

            if response.status_code == 200:
                print(f'{person_id}: Person updated!')
            else:
                print(f"'update_person': '{person_id}' Request failed with status code {response.status_code}")
                print(response.json())

        @staticmethod
        def deal(deal_id, policy_info_arr, entity_id, entype):
            url = f'{BASE_URL_V2}/deals/{deal_id}'
            params = {'api_token': PIPEDRIVE_TOKEN}
            body = {
                "title": policy_info_arr[0],
                "owner_id": CREATOR_USER_ID,
                "currency": policy_info_arr[1],
                "value": policy_info_arr[2],
                "expected_close_date": policy_info_arr[4],
                "status": policy_info_arr[7],
                "visible_to": 3,
                "custom_fields": {
                    # RENEWED_OFFER_QUANTITY:         None,
                    # RENEWAL_START_DATE:             None,
                    # SELLER:                         None,
                    POLICY_NO:                      policy_info_arr[5],
                    # RENEWAL_POLICY_QUANTITY:        None,
                    PRODUCT:                        Pipedrive.find_custom_field(PRODUCT, policy_info_arr[8]),
                    # POLICY_ON_ATTB:                 None,
                    OBJECTS:                        policy_info_arr[3],
                    END_DATE:                       policy_info_arr[4],
                    INSURER:                        Pipedrive.find_custom_field(INSURER, policy_info_arr[6]),
                    # REGISTRATION_CERTIFICATE_NO:    None,
                    # RENEWAL:                        None,
                    # RENEWED_POLICY_INSURER:         None,
                    # STATUS:                         None
                }
            }

            if policy_info_arr[7] == 'won':
                body['won_time'] = (datetime.strptime(policy_info_arr[4], "%Y-%m-%d")
                                    .replace(hour=9, minute=0, second=0).strftime('%Y-%m-%dT%H:%M:%SZ'))
                body['stage_id'] = 5

            # elif policy_info_arr[7] == 'lost':
            #     body['lost_time'] = (datetime.strptime(policy_info_arr[4], "%Y-%m-%d")
            #                          .replace(hour=9, minute=0, second=0).strftime('%Y-%m-%dT%H:%M:%SZ'))
            #     body['stage_id'] = 1

            if entype == 'org':
                body["org_id"] = entity_id
            else:
                body["person_id"] = entity_id

            response = requests.patch(url=url, params=params, json=body)

            if response.status_code == 200:
                print(f'{deal_id}: Deal updated!')
            else:
                print(f"'update_deal': '{deal_id}' Request failed with status code {response.status_code}")
                print(response.json())

        @staticmethod
        def note(note_id, content, deal_id):
            url = f'{BASE_URL_V1}/notes/{note_id}'
            params = {'api_token': PIPEDRIVE_TOKEN}
            body = {
                "content": content,
                "deal_id": deal_id,
                "user_id": CREATOR_USER_ID
            }

            response = requests.put(url=url, params=params, json=body)

            if response.status_code == 200:
                print(f'{response.json()['data']['id']}: Note updated!')
            else:
                print(f"'update_note': Request failed with status code {response.status_code}")
                print(response.json())