import time
import json
import os

from pipedrive import Pipedrive
from insly import get_customer_policy
from helper import retry_requests


def main():
    pd_token = os.getenv('PIPEDRIVE_TOKEN')

    pd = Pipedrive(pd_token)

    with open('customer_oids.json', 'r', encoding='utf-8') as oids:
        customer_oids = json.load(oids)

    active_policies = []
    counter = 815

    print(f'\n{len(customer_oids) - (counter - 1)} OID\'s ready!\n')

    for oid in customer_oids[counter - 1:]:
        retry_requests()
        customer_i, policy_i, address_i = get_customer_policy(oid, counter)

        if customer_i:
            active_policies.append(customer_i[0][1])
            org_id, org_name = pd.Search.organization(customer_i[0][1])

            if org_id and org_name:
                pd.Update.organization(org_id, org_name, oid)
            else:
                org_id = pd.Add.organization(customer_i[0][1], oid, address_i)

            if customer_i[0][4] == 11:  # Company
                print(f'{customer_i[0][0]}: \'{customer_i[0][4]}\' Company')

                person_id, person_name = pd.Search.person(customer_i[0][1])

                if person_id and person_name:
                    pd.Update.person(person_id, org_id, customer_i)
                else:
                    person_id = pd.Add.person(org_id, customer_i)

                for i in range(len(policy_i)):
                    deal_id, deal_name = pd.Search.deal(policy_i[i][0])

                    if deal_id and deal_name:
                        pd.Update.deal(deal_id, policy_i, person_id, org_id)
                    else:
                        deal_id = pd.Add.deal(policy_i, person_id, org_id)

            else:  # Individual
                print(f'{customer_i[0][0]}: \'{customer_i[0][4]}\' Individual')

        counter += 1
        time.sleep(0.7)

    print(f'\n{len(active_policies)} policies expiring this month!\n')

    try:
        with open('active_policies.json', 'w', encoding='utf-8') as json_file:
            json.dump(active_policies, json_file, ensure_ascii=False, indent=4)
        print("Active policies saved to 'active_policies.json'.")

    except Exception as e:
        print(f"Failed to save active policies to file: {e}")

    return


if __name__ == '__main__':
    main()
