import requests
import csv

# API Configuration
store_hash = 'gud7r2x2lu'  # Replace with your store hash
base_url = f'https://api.bigcommerce.com/stores/{store_hash}/v3'
filename = 'Attributes_Export_All.csv'
access_token = 'tcb6atv3hy4bzppemko5g5z2fk1vr9i'

# Attribute IDs to fetch
attribute_ids = [5, 6]

# Dictionary to store customer data
customer_data = {}

# Headers for API requests
headers = {
    'Accept': 'application/json',
    'Content-Type': 'application/json',
    'X-Auth-Token': access_token
}

# Step 1: Fetch Attribute Data
for attr_id in attribute_ids:
    page = 1  # Start from page 1

    while True:  # Loop until no more data is returned
        url = f"{base_url}/customers/attribute-values?attribute_id:in={attr_id}&limit=250&page={page}"

        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an exception for unsuccessful requests

        json_response = response.json()

        # Ensure the response has the expected structure
        if isinstance(json_response, dict) and 'data' in json_response:
            data = json_response['data']

            if not data:  # Stop when there are no more results
                print(f"No more data for attribute_id {attr_id}, stopping pagination.")
                break

            for item in data:
                customer_id = item.get('customer_id')
                attribute_value = item.get('attribute_value')  # Correct field for value

                if customer_id not in customer_data:
                    customer_data[customer_id] = {'customer_id': customer_id}

                # Store attribute_value for respective attribute_id
                customer_data[customer_id][f'attribute_{attr_id}'] = attribute_value

            page += 1  # Move to the next page
        else:
            print(f"Invalid JSON response on page {page} for attribute_id {attr_id}. Stopping pagination.")
            break

# Step 2: Fetch Customer Emails
customer_ids = list(customer_data.keys())  # Extract unique customer IDs
batch_size = 50  # BigCommerce API allows batch fetching, limit to 50 per request

for i in range(0, len(customer_ids), batch_size):
    batch = customer_ids[i:i + batch_size]
    customer_id_list = ",".join(map(str, batch))  # Convert list to comma-separated string

    url = f"{base_url}/customers?id:in={customer_id_list}&limit=50"

    response = requests.get(url, headers=headers)
    response.raise_for_status()

    json_response = response.json()

    # Ensure the response has the expected structure
    if isinstance(json_response, dict) and 'data' in json_response:
        data = json_response['data']

        for customer in data:
            customer_id = customer.get('id')
            email = customer.get('email')

            if customer_id in customer_data:
                customer_data[customer_id]['email'] = email

# Step 3: Write Data to CSV
if customer_data:
    # Determine fieldnames dynamically
    fieldnames = ['customer_id', 'email'] + [f'attribute_{attr_id}' for attr_id in attribute_ids]

    with open(filename, 'w', newline='') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(customer_data.values())

    print(f"Export successful! CSV file '{filename}' created.")
else:
    print("No data to export.")