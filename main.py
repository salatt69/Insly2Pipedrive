import time
import os

from pipedrive import Pipedrive
from insly import get_customer_policy, get_customer_list
from helper import retry_requests


def process_customer(pd, oid, counter):
    retry_requests()
    customer_i, policy_i, address_i, object_i = get_customer_policy(oid, counter)

    if not customer_i:
        return

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

    customer_oids = get_customer_list()
    if not customer_oids:
        print("No customer OIDs found. Exiting.")
        return

    # 18254
    counter = 1

    remaining_oids = customer_oids[counter - 1:]
    print(f"\n{len(remaining_oids)} OIDs ready!\n")

    for i, oid in enumerate(remaining_oids, start=counter):
        process_customer(pd, oid, i)
        time.sleep(1)


if __name__ == '__main__':
    main()
