import time
import os
import http.client
import traceback

from pipedrive import Pipedrive
from insly import get_customer_policy, get_customer_list
from helper import retry_requests


def process_customer(pd, oid, counter):
    retry_requests()
    retry_delay = 5

    while True:
        try:
            customer_i, policy_i, address_i, object_i = get_customer_policy(oid, counter)

            if not customer_i:
                return

            if customer_i[0][4] == 11:
                print(f"\t{customer_i[0][0]}: Company")
                org_id, org_name = pd.Search.organization(customer_i[0][0]) or (None, None)

                if org_id is None:
                    org_id = pd.Add.organization(customer_i[0], address_i[0])
                else:
                    pd.Update.organization(org_id, customer_i[0], address_i[0])

                entity_id, entype = org_id, 'org'

            else:
                print(f"\t{customer_i[0][0]}: Individual")
                person_id, person_name = pd.Search.person(customer_i[0][0]) or (None, None)

                if person_id is None:
                    person_id = pd.Add.person(customer_i[0])
                else:
                    pd.Update.person(person_id, customer_i[0])

                entity_id, entype = person_id, 'person'

            for i in range(len(policy_i)):
                deal_id, deal_title = pd.Search.deal(policy_i[i][10]) or (None, None)

                if deal_id is None:
                    deal_id = pd.Add.deal(policy_i[i], entity_id, entype, customer_i[0][5])
                else:
                    pd.Update.deal(deal_id, policy_i[i], entity_id, entype, customer_i[0][5])

                note_id = pd.Search.note(deal_id)

                if note_id is None:
                    pd.Add.note(object_i[i], deal_id, customer_i[0][5])
                else:
                    pd.Update.note(note_id, object_i[i], deal_id, customer_i[0][5])
            return

        except http.client.RemoteDisconnected as e:
            print(f"\nRemoteDisconnected error on customer {oid}: {e}")
        except Exception as e:
            print(f"\nUnexpected error processing customer {oid}: {e}")
            print(traceback.format_exc())

        print(f"\nRetrying customer {oid} in {retry_delay} seconds...")
        time.sleep(retry_delay)
        retry_delay = min(retry_delay * 2, 60)


def main():
    pd_token = os.getenv('PIPEDRIVE_TOKEN')
    pd = Pipedrive(pd_token)

    customer_oids = get_customer_list()
    if not customer_oids:
        print("No customer OIDs found. Exiting.")
        return

    start_from = 1

    customer_oids = customer_oids[start_from:]

    print(f"\n{len(customer_oids)} OIDs ready!\n")

    for i, oid in enumerate(customer_oids, start=1):
        process_customer(pd, oid, i)
        time.sleep(1)

if __name__ == '__main__':
    main()
