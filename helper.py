def retry_requests():
    import time
    from insly import get_customer_policy, retry_buffer

    while retry_buffer:
        wait_time = 40 # In seconds
        oid, counter = retry_buffer.pop(0)
        print(f"#{counter} Rate limit exceeded! Retrying {oid} after {wait_time} seconds...\n")
        time.sleep(wait_time)
        get_customer_policy(oid, counter)
    retry_buffer.clear()


def is_email_valid(email):
    import re
    email_regex = r"[^@ \t\r\n]+@[^@ \t\r\n]+\.[^@ \t\r\n]+"
    return bool(re.match(email_regex, email))


def is_phone_valid(phone):
    import re
    phone_regex = r"^[\+]?[(]?[0-9]{3}[)]?[-\s\.]?[0-9]{3}[-\s\.]?[0-9]{4,6}$"
    return bool(re.match(phone_regex, phone))

