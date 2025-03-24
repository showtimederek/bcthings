import requests
import csv
from tqdm import tqdm

# BigCommerce API credentials
STORE_HASH = "3jnc7mz2z7"
ACCESS_TOKEN = "cgjlvalglgzwdqm01l5zh2s0uedjllj"

# API endpoints
INSTRUMENTS_API_URL = f"https://api.bigcommerce.com/stores/{STORE_HASH}/v3/payments/stored-instruments"
CUSTOMERS_API_URL = f"https://api.bigcommerce.com/stores/{STORE_HASH}/v3/customers"

# Headers
HEADERS = {
    "X-Auth-Token": ACCESS_TOKEN,
    "Accept": "application/json",
    "Content-Type": "application/json"
}

def fetch_stored_instruments():
    all_data = []
    page = 1
    limit = 250  # Adjust if needed
    
    print("Fetching stored instruments...")
    with tqdm(desc="Fetching stored instruments", unit="page") as pbar:
        while True:
            response = requests.get(f"{INSTRUMENTS_API_URL}?limit={limit}&page={page}", headers=HEADERS)
            if response.status_code == 200:
                data = response.json().get("data", [])
                if not data:
                    break
                filtered_data = [item for item in data if item.get("is_default")]
                all_data.extend(filtered_data)
                page += 1
                pbar.update(1)
            else:
                print(f"Error: Unable to fetch data. Status Code: {response.status_code}")
                print(response.text)
                break
    
    return all_data

def fetch_customer_emails(customer_ids):
    emails = {}
    batch_size = 20
    total_batches = (len(customer_ids) + batch_size - 1) // batch_size
    
    print("Fetching customer emails...")
    for i in tqdm(range(0, len(customer_ids), batch_size), desc="Fetching customer emails", unit="batch"):
        batch = customer_ids[i:i + batch_size]
        ids_param = ",".join(map(str, batch))
        response = requests.get(f"{CUSTOMERS_API_URL}?id:in={ids_param}", headers=HEADERS)
        if response.status_code == 200:
            customers = response.json().get("data", [])
            for customer in customers:
                emails[customer["id"]] = customer.get("email", "")
        else:
            print(f"Error fetching customer emails. Status Code: {response.status_code}")
            print(response.text)
    
    return emails

def export_to_csv(data, filename="stored_instruments.csv"):
    if not data:
        print("No data to export.")
        return
    
    customer_ids = list(set(item["customer_id"] for item in data if "customer_id" in item))
    customer_emails = fetch_customer_emails(customer_ids)
    
    for item in tqdm(data, desc="Processing data", unit="record"):
        item["customer_email"] = customer_emails.get(item["customer_id"], "")
        item["paypal_email"] = item.get("email", "")
        billing_address = item.get("billing_address", {})
        item["billing_first_name"] = billing_address.get("first_name", "")
        item["billing_last_name"] = billing_address.get("last_name", "")
        item["billing_email"] = billing_address.get("email", "")
        item["billing_address1"] = billing_address.get("address1", "")
        item["billing_address2"] = billing_address.get("address2", "")
        item["billing_city"] = billing_address.get("city", "")
        item["billing_state_or_province"] = billing_address.get("state_or_province", "")
        item["billing_state_or_province_code"] = billing_address.get("state_or_province_code", "")
        item["billing_postal_code"] = billing_address.get("postal_code", "")
        item["billing_country_code"] = billing_address.get("country_code", "")
        item["billing_phone"] = billing_address.get("phone", "")
    
    keys = [
        "customer_id", "customer_email", "type", "paypal_email", "is_default", "brand", "expiry_month", "expiry_year", "last_4", 
        "billing_first_name", "billing_last_name", "billing_email", "billing_address1", "billing_address2", "billing_city", "billing_state_or_province",
        "billing_state_or_province_code", "billing_postal_code", "billing_country_code", "billing_phone"
    ]
    
    with open(filename, mode='w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=keys)
        writer.writeheader()
        writer.writerows([{key: item.get(key, "") for key in keys} for item in data])
    
    print(f"Data successfully exported to {filename}")

def main():
    data = fetch_stored_instruments()
    export_to_csv(data)

if __name__ == "__main__":
    main()