import requests
from datetime import datetime
from helper import is_email_valid, is_phone_valid

BASE_URL = 'https://api.pipedrive.com/api/v2'
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

    class Search:
        @staticmethod
        def organization(org_name):
            url = f'{BASE_URL}/organizations/search?term={org_name}'
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
            url = f'{BASE_URL}/persons/search?term={person_name}'
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
            url = f'{BASE_URL}/deals/search?term={deal_title}'
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

    class Add:
        @staticmethod
        def organization(org_name, oid, address_info_array):
            add_time = datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')

            url = f'{BASE_URL}/organizations'
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
        def person(org_id, info):
            url = f'{BASE_URL}/persons'
            body = {
                "name": info[0][1],
                "owner_id": CREATOR_USER_ID,
                "org_id": org_id,
                "visible_to": 3,
                "custom_fields": {
                    INSLY_OID: info[0][0]
                }
            }
            params = {'api_token': PIPEDRIVE_TOKEN}

            # Conditionally add emails if valid
            if info[0][2] and is_email_valid(info[0][2]):
                body["emails"] = [
                    {
                        "value": info[0][2],
                        "primary": True,
                        "label": "email"
                    }
                ]

            # Conditionally add phones if valid
            if info[0][3] and is_phone_valid(info[0][3]):
                body["phones"] = [
                    {
                        "value": info[0][3],
                        "primary": True,
                        "label": "phone_number"
                    }
                ]

            response = requests.post(url=url, params=params, json=body)

            if response.status_code == 200:
                print(f'{response.json()['data']['id']}: Person added!')
                return response.json()['data']['id']

            else:
                print(f"'add_person': '{info[0][0]}' Request failed with status code {response.status_code}")
                print(response.json())

        @staticmethod
        def deal(policy_info_arr, p_id, org_id):
            url = f'{BASE_URL}/deals'
            params = {'api_token': PIPEDRIVE_TOKEN}

            body = {
                "title": policy_info_arr[0][0],
                "owner_id": CREATOR_USER_ID,
                "person_id": p_id,
                "org_id": org_id,
                "pipeline_id": 1,
                "stage_id": 1,
                "currency": policy_info_arr[0][1],
                "value": policy_info_arr[0][2],
                "expected_close_date": policy_info_arr[0][4],
                "status": "open",
                "visible_to": 3,
                "custom_fields": {
                    # RENEWED_OFFER_QUANTITY:         None,
                    # RENEWAL_START_DATE:             None,
                    # SELLER:                         None,
                    # POLICY_NO:                      policy_info_arr[0][5],
                    # RENEWAL_POLICY_QUANTITY:        None,
                    # PRODUCT:                        None,
                    # POLICY_ON_ATTB:                 None,
                    # OBJECTS:                        policy_info_arr[0][3],
                    # END_DATE:                       policy_info_arr[0][4],
                    # INSURER:                        policy_info_arr[0][6],
                    # REGISTRATION_CERTIFICATE_NO:    None,
                    # RENEWAL:                        None,
                    # RENEWED_POLICY_INSURER:         None,
                    # STATUS:                         None
                }
            }

            response = requests.post(url=url, params=params, json=body)

            if response.status_code == 200:
                print(f'{response.json()['data']['id']}: Deal added!')
                return response.json()['data']['id']
            else:
                print(f"'add_deal': Request failed with status code {response.status_code}")
                print(response.json())

    class Update:
        @staticmethod
        def organization(org_id, org_name, oid):
            url = f'{BASE_URL}/organizations/{org_id}'
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
        def person(person_id, org_id, info):
            url = f'{BASE_URL}/persons/{person_id}'
            params = {'api_token': PIPEDRIVE_TOKEN}
            body = {
                "name": info[0][1],
                "owner_id": CREATOR_USER_ID,
                "org_id": org_id,
                "visible_to": 3,
                "custom_fields": {
                    INSLY_OID: info[0][0]
                }
            }

            # Conditionally add emails if valid
            if info[0][2] and is_email_valid(info[0][2]):
                body["emails"] = [
                    {
                        "value": info[0][2],
                        "primary": True,
                        "label": "email"
                    }
                ]

            # Conditionally add phones if valid
            if info[0][3] and is_phone_valid(info[0][3]):
                body["phones"] = [
                    {
                        "value": info[0][3],
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
        def deal(deal_id, policy_info_arr, p_id, org_id):
            url = f'{BASE_URL}/deals/{deal_id}'
            params = {'api_token': PIPEDRIVE_TOKEN}
            body = {
                "title": policy_info_arr[0][0],
                "owner_id": CREATOR_USER_ID,
                "person_id": p_id,
                "org_id": org_id,
                "pipeline_id": 1,
                "stage_id": 1,
                "currency": policy_info_arr[0][1],
                "value": policy_info_arr[0][2],
                "expected_close_date": policy_info_arr[0][4],
                "visible_to": 3,
                "custom_fields": {
                    # RENEWED_OFFER_QUANTITY:         None,
                    # RENEWAL_START_DATE:             None,
                    # SELLER:                         None,
                    # POLICY_NO:                      policy_info_arr[0][5],
                    # RENEWAL_POLICY_QUANTITY:        None,
                    # PRODUCT:                        None,
                    # POLICY_ON_ATTB:                 None,
                    # OBJECTS:                        policy_info_arr[0][3],
                    # END_DATE:                       policy_info_arr[0][4],
                    # INSURER:                        policy_info_arr[0][6],
                    # REGISTRATION_CERTIFICATE_NO:    None,
                    # RENEWAL:                        None,
                    # RENEWED_POLICY_INSURER:         None,
                    # STATUS:                         None
                }
            }

            response = requests.patch(url=url, params=params, json=body)

            if response.status_code == 200:
                print(f'{deal_id}: Deal updated!')
            else:
                print(f"'update_deal': '{deal_id}' Request failed with status code {response.status_code}")
                print(response.json())
