import json
import time

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
# SELLER = 'dd13df304df939c49502d228441a544b2d7540a4'
POLICY_NO = '30bbd24791ef12c955ed795a6e93d64c4fd31fa1'
RENEWAL_POLICY_QUANTITY = '31106618eed52b54ca1e9a25eb8113fea358257f'
PRODUCT = '361d053d9234bc515f0884ecb4a12958c3b50574'
POLICY_ON_ATTB = 'bf0217a840456447e891aebbf04ad4e433440a8e'
OBJECTS = '6981d2e1dc3d0212c5e581a4d627a18ac976f83f'
END_DATE = 'bee031bba9bdbeec53a9f85186f2a9f853fa8809'
INSURER = 'd897fe9647fdb08f70ff8abacf75a4e1c6078c5c'
REGISTRATION_CERTIFICATE_NO = 'eeefe1dfe026ffd8d13ce4a57af8650e8c8fb20a'
RENEWAL = 'f21e42e467108b3c0fce3f32834aa5ba48a5bad3'
RENEWED_POLICY_INSURER = '471feb53601b3f016fb41ea421c9fd9c3a368602'
STATUS = '0cbadc7b01827c2ace7dfae87f7c710178dcdc42'
ORG_PHONE_NUMBER = '2f6ca1565cb01c3be96e09155b7d3ad3ed90a22f'
ORG_MOBILE_PHONE_NUMBER = '3a029778616fb8d7b46a80cb257b120fc39ebbb9'
ORG_EMAIL = '79df0042ead91e4d68adfae2c95f7a05cf6fb91f'
ORG_REGISTRATION_NUMBER = '572f5c7d9e53e4e3ba6aef777a2cb5bccd29d0d0'
POLICY_OID = 'a9bba6a79606e925f7682d1884c7ed8829cdf5e6'
SELLER_LIST = '4ab090f2198c1c3f4b8dfff65bd7bfe7b0046d5b'
POLICY_ON_ATTB_LIST = 'bf0217a840456447e891aebbf04ad4e433440a8e'


class Pipedrive:
    def __init__(self, token: str):
        if not token:
            raise ValueError('API token is required!')
        global PIPEDRIVE_TOKEN
        PIPEDRIVE_TOKEN = token

    @staticmethod
    def find_custom_field(custom_field_key, option_label):
        """
        Retrieves the ID of a specific option within a custom field in Pipedrive.

        This method searches for a custom field by its key and then attempts to find
        an option within that field that matches the given label.

        Args:
            custom_field_key (str): The unique key of the custom field, which is mapped
                                    to its field ID in 'custom_field_ids.json'.
            option_label (str): The label of the option to search for within the custom field.

        Returns:
            int | None: The ID of the matching option if found, otherwise None.

        .. rubric:: Behavior
        - Reads 'custom_field_ids.json' to get the field ID corresponding to `custom_field_key`.
        - Sends a GET request to the Pipedrive API to retrieve details of the specified custom field.
        - Iterates through the available options in the field to find one that matches `option_label`.
        - If a match is found (case-insensitive comparison), returns the option's ID.
        - Logs an error message and prints the response JSON if the API request fails.

        Note:
            - If `option_label` is empty or None, the function exits early without making an API request.
            - The function relies on `BASE_URL_V1` and `PIPEDRIVE_TOKEN` for API communication.
        """
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

    @staticmethod
    def get_deal_body(policy_info_arr, entity_id, entype, deal_owner):
        """
        Constructs the request body for creating or updating a deal in Pipedrive.

        This method assembles a structured dictionary containing deal details and
        associated custom field values based on the provided policy information.

        Args:
            policy_info_arr (list): A list containing policy details in the predefined order.
            entity_id (int): The ID of the associated entity (e.g., company or person).
            entype (str): The type of entity associated with the deal.
            deal_owner (int | None): The Pipedrive user ID of the deal owner.

        Returns:
            dict: A dictionary representing the deal's request body, ready for use in API calls to Pipedrive.

        .. rubric:: Behavior
        - Populates standard deal fields such as `title`, `owner_id`, `currency`,
          `value`, `expected_close_date`, `status`, and `visible_to`.
        - Maps predefined keys in `custom_fields` to their corresponding values
          from `policy_info_arr`.
        - Uses :meth:`Pipedrive.find_custom_field` to dynamically retrieve the ID of
          the insurer and product fields based on their names.
        - Truncates object details using `truncate_utf8()` to ensure compliance
          with character limits.

        Note:
            - The method assumes `PRODUCT` and `INSURER` are predefined constants representing
              custom field keys.
            - The `truncate_utf8()` function ensures that object details fit within field constraints.
            - Missing or non-required fields are commented out but can be enabled if needed.
        """
        body = {
            "title": policy_info_arr[0],
            "currency": policy_info_arr[1],
            "value": policy_info_arr[2],
            "expected_close_date": policy_info_arr[4],
            "status": policy_info_arr[7],
            "visible_to": 3,
            "custom_fields": {
                # SELLER: policy_info_arr[9],
                POLICY_NO: policy_info_arr[5],
                PRODUCT: Pipedrive.find_custom_field(PRODUCT, policy_info_arr[8]),
                OBJECTS: truncate_utf8(policy_info_arr[3]),
                END_DATE: policy_info_arr[4],
                INSURER: Pipedrive.find_custom_field(INSURER, policy_info_arr[6]),
                POLICY_OID: str(policy_info_arr[10])
            }
        }

        if policy_info_arr[7] == 'won':
            body['won_time'] = (datetime.strptime(policy_info_arr[4], "%Y-%m-%d")
                                .replace(hour=9, minute=0, second=0).strftime('%Y-%m-%dT%H:%M:%SZ'))
            body['stage_id'] = 5

        if deal_owner is not None:
            body['owner_id'] = deal_owner

        if entype == 'org':
            body["org_id"] = entity_id
        else:
            body["person_id"] = entity_id
        return body

    @staticmethod
    def get_organization_body(org_info, address_info):
        """
        Constructs the request body for creating or updating an organization in Pipedrive.

        Args:
            org_info (list): A list containing organization details in the predefined order.
            address_info (list): A list containing address details or None if no address exists.

        Returns:
            dict: A dictionary representing the request body for the organization.

        .. rubric:: Behavior
        - Extracts and formats organization details, including name, owner, visibility, and custom fields.
        - Validates email and phone numbers before including them in the request body.
        - Conditionally includes an address if `address_info` is provided.

        Notes:
            - Calls `is_email_valid()` to validate the organization email.
            - Calls `extract_valid_phone()` to validate and format phone numbers.
            - Includes address details only if `address_info` is provided.
            - The returned dictionary is structured for API compatibility with Pipedrive.
        """
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
        """
        Constructs the request body for creating or updating a person in Pipedrive.

        Args:
            info (list): A list containing person details in the predefined order.

        Returns:
            dict: A dictionary representing the request body for the person.

        .. rubric:: Behavior
        - Extracts and formats person details, including name, owner, visibility, and custom fields.
        - Validates email and phone numbers before including them in the request body.
        - Assigns a primary phone number if multiple numbers are available.
        - Removes the phone field if no valid numbers exist.

        Notes:
            - Calls `is_email_valid()` to validate the email before inclusion.
            - Calls `extract_valid_phone()` to validate and format phone numbers.
            - Assigns the first valid phone number as "primary"; if both work and mobile exist, work is prioritized.
            - The returned dictionary is structured for API compatibility with Pipedrive.
        """
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
        """
        Constructs the request body for creating a note in Pipedrive.

        Args:
            content (str): The string content of the note.
            deal_id (int): The ID of the deal to which the note is linked.
            note_owner (int): The ID of the user who owns the note.

        Returns:
            dict: A dictionary representing the request body for the note.

        .. rubric:: Behavior
        - Creates a note associated with a specific deal and assigns an owner.
        - Ensures the note content is properly structured for API submission.

        Notes:
            - The `content` field should be HTML format.
            - The `deal_id` must correspond to an existing deal in Pipedrive.
            - The `user_id` field assigns ownership of the note to a specific user.
        """
        body = {
            "content": content,
            "deal_id": deal_id,
            "user_id": note_owner
        }
        return body

    class Search:
        """
        Provides static methods to search for organizations,
        persons, deals, and notes in Pipedrive.
        """
        @staticmethod
        def organization(insly_customer_oid):
            """
            Searches for an organization in Pipedrive using an Insly customer OID.

            Args:
                insly_customer_oid (str): The unique identifier of the customer from Insly.

            Returns:
                tuple[int, str] | tuple[None, None]:
                    - `(org_id, org_name)`: If an organization is found.
                    - `(None, None)`: If no matching organization is found.

            .. rubric:: Behavior
            - Sends a request to the Pipedrive API to search for an organization by `insly_customer_oid`.
            - Uses an exact match to ensure precise results.
            - If the organization exists, returns its ID and name.
            - If no organization is found, returns `(None, None)`.
            - If the request fails, logs an error message.

            Notes:
                - Uses `BASE_URL_V2` for the API endpoint.
                - The API request uses an exact match to prevent partial matches.
                - The returned `org_id` can be used for further operations in Pipedrive.
            """
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
            """
            Searches for a person in Pipedrive using the provided `insly_customer_oid`.

            Args:
                insly_customer_oid (str): The unique identifier of the customer in Insly.

            Returns:
                tuple[int, str] | tuple[None, None]: A tuple containing the person's ID and name if found,
                otherwise `(None, None)`.

            .. rubric:: Behavior
            - Sends a request to the Pipedrive API to search for a person by `insly_customer_oid`.
            - Uses an exact match to ensure precise results.
            - If the person exists, returns their ID and name.
            - If no person is found, returns `(None, None)`.
            - If the request fails, logs an error message.

            Note:
                - Uses `BASE_URL_V2` for the API endpoint.
                - The API request uses an exact match to prevent partial matches.
                - The returned `org_id` can be used for further operations in Pipedrive.
            """
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
            """
            Searches for a deal in Pipedrive using the provided `insly_policy_oid`.

            Args:
                insly_policy_oid (str): The unique identifier of the policy in Insly.

            Returns:
                tuple[int, str] | tuple[None, None]: A tuple containing the deal's ID and title if found,
                otherwise `(None, None)`.

            .. rubric:: Behavior
            - Sends a request to the Pipedrive API to search for a deal by `insly_policy_oid`.
            - Uses an exact match to ensure precise results.
            - If the deal exists, returns its ID and title.
            - If no deal is found, returns `(None, None)`.
            - If the request fails, logs an error message.

            Note:
                - Uses `BASE_URL_V2` for the API endpoint.
                - The API request uses an exact match to prevent partial matches.
                - The returned `deal_id` can be used for further operations in Pipedrive.
            """
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
        def all_deals(start_pos=0, limit=50, results=None):
            """
            Retrieves all deals from Pipedrive, collecting `id` and values associated with `POLICY_OID`.

            Args:
                start_pos (int): The starting position for pagination. Defaults to 0.
                limit (int): The number of deals to fetch per request. Defaults to 50.
                results (list): A list to accumulate extracted dictionaries. Defaults to None.

            Returns:
                list[dict]: A list of dictionaries containing `id` and `POLICY_OID` values.

            .. rubric:: Behavior
            - Sends a request to the Pipedrive API to retrieve deals with pagination.
            - Extracts and stores the `id` and the corresponding `POLICY_OID` value from each deal.
            - If there are more items available, recursively calls itself with `next_start`.
            - Accumulates all extracted data across multiple paginated requests.
            - Returns the full list of dictionaries.

            Note:
                - Uses `BASE_URL_V1` for the API endpoint.
                - The function is recursive and continues fetching until all deals are retrieved.
                - Includes a small delay (`time.sleep(0.5)`) between requests to avoid hitting rate limits.
                - Ensures that both `id` and `POLICY_OID` values are captured for each deal.
            """
            if results is None:
                results = []

            url = f'{BASE_URL_V1}/deals'
            params = {
                'api_token': PIPEDRIVE_TOKEN,
                'filter_id': 68,
                'start': start_pos,
                'limit': limit
            }

            response = requests.get(url=url, params=params)

            if response.status_code == 200:
                data = response.json()

                if 'data' in data and isinstance(data['data'], list):
                    for item in data['data']:
                        policy_value = item.get(POLICY_OID)
                        deal_id = item.get("id")
                        if policy_value is not None and deal_id is not None:
                            results.append({"id": deal_id, "policy": policy_value})

                pagination = data.get('additional_data', {}).get('pagination', {})
                if pagination.get('more_items_in_collection'):
                    next_start = pagination.get('next_start')
                    time.sleep(0.5)
                    return Pipedrive.Search.all_deals(start_pos=next_start, limit=limit, results=results)

            else:
                print(f"Request failed with status code {response.status_code}")
                print(response.json())

            return results

        @staticmethod
        def note(deal_id):
            """
            Searches for a note associated with a given deal in Pipedrive.

            Args:
                deal_id (int): The unique identifier of the deal in Pipedrive.

            Returns:
                int | None: The ID of the first found note if available, otherwise `None`.

            .. rubric:: Behavior
            - Sends a request to the Pipedrive API to search for notes associated with `deal_id`.
            - If notes exist, returns the ID of the first note found.
            - If no notes are found, returns `None`.
            - If the request fails, logs an error message.

            Note:
                - Uses `BASE_URL_V1` for the API endpoint.
                - The returned `deal_id` can be used for further operations in Pipedrive.
            """
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
        """
        Provides methods for adding new entities to Pipedrive,
        including organizations, persons, deals, and notes.
        """
        @staticmethod
        def organization(org_info, address_info):
            """
            Adds a new organization to Pipedrive.

            Args:
                org_info (list): A list containing organization details such as name, email, phone, and owner ID.
                address_info (list | None): A list containing address details (street, country, postal code) or `None` if no address is provided.

            Returns:
                int | None: The ID of the newly created organization if successful, otherwise `None`.

            .. rubric:: Behavior
            - Constructs an organization body using `Pipedrive.get_organization_body()`.
            - Sends a POST request to the Pipedrive API to create the organization.
            - If the request succeeds, returns the newly created organization's ID and prints a success message.
            - If the request fails, logs an error message and does not return an ID.

            Note:
                - Uses `BASE_URL_V2` for the API endpoint.
            """
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
            """
            Adds a new person to Pipedrive.

            Args:
                info (list): A list containing person details such as name, email, phone, and owner ID.

            Returns:
                int | None: The ID of the newly created person if successful, otherwise `None`.

            .. rubric:: Behavior
            - Constructs a person body using `Pipedrive.get_person_body()`.
            - Sends a POST request to the Pipedrive API to create the person.
            - If the request succeeds, returns the newly created person's ID and prints a success message.
            - If the request fails, logs an error message and does not return an ID.

            Note:
                - Uses `BASE_URL_V2` for the API endpoint.
            """
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
        def deal(policy_info_arr, entity_id, entype, deal_owner=None):
            """
            Adds a new deal to Pipedrive.

            Args:
                policy_info_arr (list): A list containing policy details such as title, currency, value, and status.
                entity_id (int): The ID of the associated entity (e.g., organization or person).
                entype (str): The type of entity associated with the deal.
                deal_owner (int): The ID of the deal owner.

            Returns:
                int | None: The ID of the newly created deal if successful, otherwise `None`.

            .. rubric:: Behavior
            - Constructs a deal body using `Pipedrive.get_deal_body()`.
            - Sends a POST request to the Pipedrive API to create the deal.
            - If the request succeeds, returns the newly created deal's ID and prints a success message.
            - If the request fails, logs an error message and does not return an ID.

            Note:
                - Uses `BASE_URL_V2` for the API endpoint.
            """
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
            """
            Adds a new note to a deal in Pipedrive.

            Args:
                content (str): The content of the note.
                deal_id (int): The ID of the deal to associate the note with.
                note_owner (int): The ID of the user creating the note.

            Returns:
                None

            .. rubric:: Behavior
            - Constructs a note body using `Pipedrive.get_note_body()`.
            - Sends a POST request to the Pipedrive API to create the note.
            - If the request succeeds (status code 200 or 201), prints a success message.
            - If the request fails, logs an error message.

            Note:
                - Uses `BASE_URL_V1` for the API endpoint.
                - Requires a valid `deal_id` to associate the note.
            """

            url = f'{BASE_URL_V1}/notes'
            params = {'api_token': PIPEDRIVE_TOKEN}
            body = Pipedrive.get_note_body(content, deal_id, note_owner)

            response = requests.post(url=url, params=params, json=body)

            if response.status_code == 200 or response.status_code == 201:
                print(f'\t{response.json()['data']['id']}: Note added!')
            else:
                print(f"'add_note': Request failed with status code {response.status_code}")
                print(response.json())

    class Update:
        """
        Provides methods for updating existing entities in Pipedrive,
        including organizations, persons, deals, and notes.
        """
        @staticmethod
        def organization(org_id, org_info, address_info):
            """
            Updates an existing organization in Pipedrive.

            Args:
                org_id (int): The ID of the organization to update.
                org_info (list): A list containing updated organization details such as name, email, phone, and owner ID.
                address_info (list | None): A list containing updated address details (street, country, postal code) or `None` if no changes.

            Returns:
                None

            .. rubric:: Behavior
            - Constructs an updated organization body using `Pipedrive.get_organization_body()`.
            - Sends a PATCH request to the Pipedrive API to update the organization.
            - If the request succeeds, logs a success message.
            - If the request fails, logs an error message.

            Note:
                - Uses `BASE_URL_V2` for the API endpoint.
            """
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
            """
            Updates an existing person in Pipedrive.

            Args:
                person_id (int): The ID of the person to update.
                info (list): A list containing updated person details such as name, email, phone, and owner ID.

            Returns:
                None

            .. rubric:: Behavior
            - Constructs an updated person body using `Pipedrive.get_person_body()`.
            - Sends a PATCH request to the Pipedrive API to update the person.
            - If the request succeeds, logs a success message.
            - If the request fails, logs an error message.

            Note:
                - Uses `BASE_URL_V2` for the API endpoint.
            """
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
        def deal_custom_fields(deal_id, info):
            """
            Updates custom fields for an existing deal in Pipedrive.

            Args:
                deal_id (int): The ID of the deal to update.
                info (list): A list containing updated custom field values in the predefined order.

            Returns:
                None

            .. rubric:: Behavior
            - Constructs an updated deal body with custom fields.
            - Sends a PATCH request to the Pipedrive API to update the deal.
            - If the request succeeds, logs a success message.
            - If the request fails, logs an error message.

            Note:
                - Uses `BASE_URL_V2` for the API endpoint.
            """
            url = f'{BASE_URL_V2}/deals/{deal_id}'
            params = {'api_token': PIPEDRIVE_TOKEN}
            body = {
                "custom_fields": {
                    POLICY_ON_ATTB: info[0],
                    RENEWED_OFFER_QUANTITY: info[1],
                    RENEWAL_POLICY_QUANTITY: info[2],
                    RENEWED_POLICY_INSURER: info[3],
                    STATUS: info[4],
                    RENEWAL: info[5],
                    RENEWAL_START_DATE: info[6],
                    REGISTRATION_CERTIFICATE_NO: info[7],
                    SELLER_LIST: info[8],
                    POLICY_ON_ATTB_LIST: info[9]
                }
            }
            response = requests.patch(url=url, params=params, json=body)

            if response.status_code == 200:
                print(f'\t{deal_id}: Deal updated!')
            else:
                print(f"'update_deal_custom_fields': '{deal_id}' Request failed with status code {response.status_code}")
                print(response.json())

        @staticmethod
        def deal(deal_id, policy_info_arr, entity_id, entype):
            """
            Updates an existing deal in Pipedrive.

            Args:
                deal_id (int): The ID of the deal to update.
                policy_info_arr (list): A list containing updated policy details such as title, currency, value, and status.
                entity_id (int): The ID of the associated entity (e.g., organization or person).
                entype (str): The type of entity associated with the deal.
                deal_owner (int): The ID of the deal owner.

            Returns:
                None

            .. rubric:: Behavior
            - Constructs an updated deal body using `Pipedrive.get_deal_body()`.
            - Sends a PATCH request to the Pipedrive API to update the deal.
            - If the request succeeds, logs a success message.
            - If the request fails, logs an error message.

            Note:
                - Uses `BASE_URL_V2` for the API endpoint.
                - `deal_owner` is hardcoded None, in order to assign owner only when deal is being created.
            """
            url = f'{BASE_URL_V2}/deals/{deal_id}'
            params = {'api_token': PIPEDRIVE_TOKEN}
            body = Pipedrive.get_deal_body(policy_info_arr, entity_id, entype, None)

            response = requests.patch(url=url, params=params, json=body)

            if response.status_code == 200:
                print(f'\t{deal_id}: Deal updated!')
            else:
                print(f"'update_deal': '{deal_id}' Request failed with status code {response.status_code}")
                print(response.json())

        @staticmethod
        def deal_status(deal_id, status):
            """
            Updates the status of a specific deal in Pipedrive.

            Args:
                deal_id (int): The ID of the deal to be updated.
                status (str): The new status to set for the deal.

            Returns:
                None: The function updates the deal status and prints a success message or an error message.

            .. rubric:: Behavior
            - Sends a `PATCH` request to the Pipedrive API to update the status of a deal.
            - If the request is successful (status code 200), prints a confirmation message with the deal ID.
            - If the request fails, prints an error message with the status code and response details.

            Note:
                - Uses `BASE_URL_V2` for the API endpoint.
                - The request is made with the `api_token` parameter for authentication.
                - The body of the request contains the new `status` to be updated.
            """
            url = f'{BASE_URL_V2}/deals/{deal_id}'
            params = {'api_token': PIPEDRIVE_TOKEN}
            body = {
                "status": status
            }
            response = requests.patch(url=url, params=params, json=body)

            if response.status_code == 200:
                print(f'\t{deal_id}: Deal status updated!')
            else:
                print(f"'update_deal_status': '{deal_id}' Request failed with status code {response.status_code}")
                print(response.json())

        @staticmethod
        def note(note_id, content, deal_id, note_owner):
            """
            Updates an existing note in Pipedrive.

            Args:
                note_id (int): The ID of the note to update.
                content (str): The updated content of the note.
                deal_id (int): The ID of the deal associated with the note.
                note_owner (int): The ID of the user updating the note.

            Returns:
                None

            .. rubric:: Behavior
            - Constructs an updated note body using `Pipedrive.get_note_body()`.
            - Sends a PUT request to the Pipedrive API to update the note.
            - If the request succeeds, logs a success message.
            - If the request fails, logs an error message.

            Note:
                - Uses `BASE_URL_V1` for the API endpoint.
            """
            url = f'{BASE_URL_V1}/notes/{note_id}'
            params = {'api_token': PIPEDRIVE_TOKEN}
            body = Pipedrive.get_note_body(content, deal_id, note_owner)

            response = requests.put(url=url, params=params, json=body)

            if response.status_code == 200:
                print(f'\t{response.json()['data']['id']}: Note updated!')
            else:
                print(f"'update_note': Request failed with status code {response.status_code}")
                print(response.json())