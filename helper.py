def retry_requests(wait_time=40):
    """
    Retries failed requests due to rate limits by processing entries in `retry_buffer`.

    Args:
        wait_time (int): The number of seconds to wait before retrying each request. Defaults to 40.

    .. rubric:: Behavior
    - Iterates over the `retry_buffer`, which stores tuples of (object ID, retry counter).
    - Waits for `wait_time` seconds before retrying each request.
    - Calls `get_customer_policy(oid, counter)` to retry fetching customer policy.
    - Continues until all retry attempts are exhausted.
    - Clears `retry_buffer` at the end.

    Note:
        - Assumes `retry_buffer` is a global list storing failed request details.
        - The function imports `get_customer_policy` and `retry_buffer` from `insly`.
    """
    import time
    from insly import get_customer_policy, retry_buffer

    while retry_buffer:
        oid, counter = retry_buffer.pop(0)
        print(f"#{counter} Rate limit exceeded! Retrying after {wait_time} seconds...\n")
        time.sleep(wait_time)
        get_customer_policy(oid, counter)
    retry_buffer.clear()


def is_email_valid(email):
    """
    Validates an email address format.

    Args:
        email (str): The email address to validate.

    Returns:
        bool: `True` if the email format is valid, otherwise `False`.

    .. rubric:: Behavior
    - Uses a regular expression to check if the email follows a standard format.
    - Ensures the email contains a single '@' symbol and a domain with at least one dot.
    - Returns `True` for valid email formats and `False` otherwise.

    Note:
        - Does not verify if the email actually exists, only checks its format.
    """
    import re
    email_regex = r"[^@ \t\r\n]+@[^@ \t\r\n]+\.[^@ \t\r\n]+"
    return bool(re.match(email_regex, email))


def extract_valid_phone(phone):
    """
    Extracts a valid phone number from a given string.

    Args:
        phone (str): The input string containing a phone number.

    Returns:
        str | None: The extracted phone number if found, otherwise `None`.

    .. rubric:: Behavior
    - Uses a regular expression to find a sequence of 7 to 15 digits.
    - Returns the first matching number found in the input string.
    - If no valid phone number is found, returns `None`.

    Note:
        - Only extracts numeric phone numbers without formatting.
        - Does not validate international dialing codes or separators.
    """
    import re
    phone_regex = r"\b\d{7,15}\b"  # Match numbers between 7 and 15 digits
    match = re.search(phone_regex, phone)  # Find first number in string
    return match.group(0) if match else None  # Return only the number


def format_objects_to_html(objects):
    """
    Formats a list of vehicle objects into an HTML string.

    Args:
        objects (list of dict): A list of dictionaries, where each dictionary contains vehicle attributes.

    Returns:
        str: A formatted HTML string representing the vehicle details.

    .. rubric:: Behavior
    - Iterates through the list of vehicle objects.
    - Escapes HTML special characters to prevent injection.
    - Constructs an HTML list (`<ul>`) with each vehicle's attributes inside list items (`<li>`).
    - Includes vehicle details such as type, license plate, make, model, VIN, year, power, gross weight, and owner.
    - Returns an HTML-formatted string with structured vehicle information.

    Note:
        - Uses the `escape()` function from `html` to prevent XSS vulnerabilities.
        - Defaults missing values to `'N/A'` if a key is not found or has a `None` value.
    """
    from html import escape
    html_content = "<h3>Policy Objects</h3><ul>"

    for obj in objects:
        html_content += "<li>"
        html_content += f"<strong>Vehicle Type:</strong> {escape(obj.get('vehicle_type') or 'N/A')}<br>"
        html_content += f"<strong>License Plate:</strong> {escape(obj.get('vehicle_licenseplate') or 'N/A')}<br>"
        html_content += f"<strong>Make:</strong> {escape(obj.get('vehicle_make') or 'N/A')}<br>"
        html_content += f"<strong>Model:</strong> {escape(obj.get('vehicle_model') or 'N/A')}<br>"
        html_content += f"<strong>VIN:</strong> {escape(obj.get('vehicle_vincode') or 'N/A')}<br>"
        html_content += f"<strong>Year:</strong> {escape(obj.get('vehicle_year') or 'N/A')}<br>"
        html_content += f"<strong>Power:</strong> {escape(obj.get('vehicle_power') or 'N/A')} HP<br>"
        html_content += f"<strong>Gross Weight:</strong> {escape(obj.get('vehicle_grossweight') or 'N/A')} kg<br>"
        html_content += f"<strong>Owner:</strong> {escape(obj.get('vehicle_owner_name') or 'N/A')}<br>"
        html_content += "</li><br>"

    html_content += "</ul>"
    return html_content


def truncate_utf8(value, byte_limit=255):
    """
    Truncates a string to fit within a specified UTF-8 byte limit.

    Args:
        value (str): The input string to be truncated.
        byte_limit (int): The maximum number of bytes allowed. Defaults to 255.

    Returns:
        str: The truncated string, ensuring it does not exceed the byte limit.

    .. rubric:: Behavior
    - Encodes the input string as UTF-8 bytes.
    - Truncates the byte sequence to the specified limit.
    - Decodes the truncated byte sequence back into a UTF-8 string, ignoring incomplete characters.
    - Returns the safely truncated string.

    Note:
        - Prevents errors by ignoring incomplete UTF-8 sequences during decoding.
        - Ensures the resulting string remains valid UTF-8.
    """
    if not value:
        return ""
    encoded_value = value.encode("utf-8")[:byte_limit]  # Truncate by bytes
    return encoded_value.decode("utf-8", errors="ignore")  # Decode safely
