def retry_requests():
    import time
    from insly import get_customer_policy, retry_buffer

    while retry_buffer:
        wait_time = 40 # In seconds
        oid, counter = retry_buffer.pop(0)
        print(f"#{counter} Rate limit exceeded! Retrying after {wait_time} seconds...\n")
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


def format_objects_to_html(objects):
    from html import escape
    html_content = "<h3>Policy Objects</h3><ul>"

    for obj in objects:
        html_content += "<li>"
        html_content += f"<strong>Vehicle Type:</strong> {escape(obj.get('vehicle_type', 'N/A'))}<br>"
        html_content += f"<strong>License Plate:</strong> {escape(obj.get('vehicle_licenseplate', 'N/A'))}<br>"
        html_content += f"<strong>Make:</strong> {escape(obj.get('vehicle_make', 'N/A'))}<br>"
        html_content += f"<strong>Model:</strong> {escape(obj.get('vehicle_model', 'N/A'))}<br>"
        html_content += f"<strong>VIN:</strong> {escape(obj.get('vehicle_vincode', 'N/A'))}<br>"
        html_content += f"<strong>Year:</strong> {escape(obj.get('vehicle_year', 'N/A'))}<br>"
        html_content += f"<strong>Power:</strong> {escape(obj.get('vehicle_power', 'N/A'))} HP<br>"
        html_content += f"<strong>Gross Weight:</strong> {escape(obj.get('vehicle_grossweight', 'N/A'))} kg<br>"
        html_content += f"<strong>Owner:</strong> {escape(obj.get('vehicle_owner_name', 'N/A'))}<br>"
        html_content += "</li><br>"

    html_content += "</ul>"
    return html_content