import time
import json
import os

from pipedrive import Pipedrive
from insly import get_customer_policy
from helper import retry_requests


def load_customer_oids(file_path='customer_oids.json'):
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            return json.load(file)
    except Exception as e:
        print(f"Error loading customer OIDs: {e}")
        return []


def save_active_policies(active_policies, file_path='active_policies.json'):
    try:
        with open(file_path, 'w', encoding='utf-8') as file:
            json.dump(active_policies, file, ensure_ascii=False, indent=4)
        print(f"Active policies saved to '{file_path}'.")
    except Exception as e:
        print(f"Failed to save active policies: {e}")


def process_customer(pd, oid, counter, active_policies):
    retry_requests()
    customer_i, policy_i, address_i, object_i = get_customer_policy(oid, counter)

    if not customer_i:
        return

    active_policies.append(customer_i[0][1])

    if customer_i[0][4] == 11:  # Company
        print(f"{customer_i[0][0]}: '11' Company")

        org_id, org_name = pd.Search.organization(customer_i[0][1]) or (None, None)

        if org_id is None:
            org_id = pd.Add.organization(customer_i[0][1], oid, address_i)
        else:
            pd.Update.organization(org_id, org_name, oid)

        entity_id = org_id
        entype = 'org'

    else:
        print(f"{customer_i[0][0]}: 'Individual'")

        person_id, person_name = pd.Search.person(customer_i[0][1]) or (None, None)

        if person_id is None:
            person_id = pd.Add.person(customer_i[0])
        else:
            pd.Update.person(person_id, customer_i[0])

        entity_id = person_id
        entype = 'person'

    for i in range(len(policy_i)):
        deal_id, deal_title = pd.Search.deal(policy_i[i][0]) or (None, None)

        if deal_id is None:
            deal_id = pd.Add.deal(policy_i[i], entity_id, entype)
        else:
            pd.Update.deal(deal_id, policy_i[i], entity_id, entype)

        note_id = pd.Search.note(deal_id)

        if note_id is None:
            pd.Add.note(object_i[i], deal_id)
        else:
            pd.Update.note(note_id, object_i[i], deal_id)


def main():
    pd_token = os.getenv('PIPEDRIVE_TOKEN')
    pd = Pipedrive(pd_token)

    customer_oids = load_customer_oids()
    if not customer_oids:
        print("No customer OIDs found. Exiting.")
        return

    counter = 18254
    # 18254
    remaining_oids = customer_oids[counter - 1:]
    print(f"\n{len(remaining_oids)} OIDs ready!\n")

    active_policies = []

    for i, oid in enumerate(remaining_oids, start=counter):
        process_customer(pd, oid, i, active_policies)
        time.sleep(1)

    print(f"\n{len(active_policies)} policies expiring this month!\n")
    save_active_policies(active_policies)


if __name__ == '__main__':
    main()
