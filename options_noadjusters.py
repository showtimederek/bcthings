import requests
import csv

# BigCommerce API Credentials
BASE_URL = "https://api.bigcommerce.com/stores/gud7r2x2lu/v3/catalog"
HEADERS = {
    "X-Auth-Token": "5th2nkh3dwlgnzobal3dqwj65lxw9y8",
    "Content-Type": "application/json",
    "Accept": "application/json"
}

def get_all_products():
    products = []
    page = 1
    limit = 250  # BigCommerce default page size
    
    while True:
        url = f"{BASE_URL}/products?page={page}&limit={limit}"
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        data = response.json().get("data", [])
        
        if not data:
            break  # Stop if no more products are returned
        
        products.extend(data)
        page += 1  # Move to the next page
    
    return products

def get_product_modifiers(product_id):
    url = f"{BASE_URL}/products/{product_id}/modifiers"
    response = requests.get(url, headers=HEADERS)
    response.raise_for_status()
    return response.json().get("data", [])

def filter_products(products):
    results = []
    for product in products:
        product_id = product.get("id")
        modifiers = get_product_modifiers(product_id)
        
        for modifier in modifiers:
            if modifier.get("name") == "Ships Every":
                # Check option_values array for adjusters
                option_values = modifier.get("option_values", [])
                for option in option_values:
                    if not option.get("adjusters"):
                        results.append({
                            "Product ID": product_id,
                            "Product Name": product.get("name"),
                            "Modifier Name": "Ships Every"
                        })
    return results

def export_to_csv(data, filename="products_without_adjuster.csv"):
    with open(filename, mode='w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=["Product ID", "Product Name", "Modifier Name"])
        writer.writeheader()
        writer.writerows(data)

def main():
    print("Fetching products...")
    products = get_all_products()
    print(f"Total products found: {len(products)}")
    
    print("Filtering products with 'Ships Every' modifier without an adjuster...")
    filtered_products = filter_products(products)
    print(f"Total products found: {len(filtered_products)}")
    
    if filtered_products:
        print("Exporting results to CSV...")
        export_to_csv(filtered_products)
        print("Export completed: products_without_adjuster.csv")
    else:
        print("No products matched the criteria.")

if __name__ == "__main__":
    main()