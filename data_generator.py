import json
import os
import random
import csv
import re

data_folder = "data/"
output_folder = 'output/'
os.makedirs(output_folder, exist_ok=True)

data_files = os.listdir(data_folder)

data_files = [x for x in data_files if '.json' in x]

NUM_STORES = 3

CSV_FILE_FORMAT = "csv"
JSON_FILE_FORMAT = "json"
stores_dict = {"Store_A": CSV_FILE_FORMAT, "Store_B": JSON_FILE_FORMAT, "Store_C": CSV_FILE_FORMAT}

## Containes information regarding related:{also_viewed:[list_of_products], bought_together:[list of products]}, sales_rank: "Rank of the product per category"
schema_columns = ['asin', 'title', 'price', 'imUrl', 'brand', 'categories', 'description']
schema_column_store_c = ['product_id', 'name', 'price', 'img_url', 'description', 'productBrand']

def get_products_from_json(json_path):
    with open(json_path, 'r') as json_file:
        products = []
        lines = json_file.readlines()
        failed = 0
        total = 0
        for line in lines:
            hasFailed = False
            total += 1
            try:
                json_dict = json.loads(r"" + line.replace('"',"").replace("'", '"') + "")
            except:
                hasFailed = True
                failed += 1
            if not hasFailed:
                products.append(json_dict)
    
    return products, total, failed


def assign_products_to_store(store_products_dict, products, stores_dict):
    for store_name, file_type in stores_dict.items():
        sampled_products = random.sample(products, int(len(products) * 0.7))

        if file_type == CSV_FILE_FORMAT:
            generate_csv_prodcucts_for_store(store_products_dict, store_name, sampled_products)
        else:
            generate_json_prodcucts_for_store(store_products_dict, store_name, sampled_products)


def generate_csv_prodcucts_for_store(store_products_dict, store_name, products):
    count = 0
    if not store_name in store_products_dict.keys():
        if store_name == 'Store_C':
            store_products_dict[store_name] = {"schema_type": CSV_FILE_FORMAT, "products": [schema_column_store_c]}
        else:
            store_products_dict[store_name] = {"schema_type": CSV_FILE_FORMAT, "products": [schema_columns]}
    for product in products:
        row = []
        temp_brand = ""
        temp_category = ""
        for field in schema_columns:
            if field not in product.keys():
                row.append(None)
            else:
                if field == "categories":
                    value = str(product[field][0][0]).strip()
                    if store_name == "Store_C":
                        temp_category = value
                        continue
                    row.append(value)
                elif field == "price":
                    price = product[field]
                    price_change = 0.05 * price
                    choice = random.choice(['p','n'])
                    if choice == 'p':
                        row.append(str(price + random.uniform(0.0, price_change)))
                    else:
                        row.append(str(price - random.uniform(0.0, price_change)))
                elif field == "brand":
                    value = str(product[field]).strip()
                    if store_name == "Store_C":
                        temp_brand = value
                        continue
                    row.append(value)

                else:
                    value = str(product[field]).strip()
                    row.append(value)
        if store_name == "Store_C":
            row.append(temp_brand + "_" + temp_category)
        store_products_dict[store_name]["products"].append(row)
        


def generate_json_prodcucts_for_store(store_products_dict, store_name, products):
    if not store_name in store_products_dict.keys(): 
        store_products_dict[store_name] = {"schema_type": JSON_FILE_FORMAT, "products": []}

    for product in products:
        store_products_dict[store_name]['products'].append(product)
        


store_products_dict = {}
for data_file in data_files:
    json_path = os.path.join(data_folder, data_file)
    products, total, failed = get_products_from_json(json_path)
    print(f"Name: {data_file}, Products: {len(products)}, Total: {total}, Failed: {failed}")
    
    assign_products_to_store(store_products_dict, products, stores_dict)


for store_name, store_info in store_products_dict.items():
    file_type = store_info['schema_type']
    if file_type == CSV_FILE_FORMAT:
        with open(os.path.join(output_folder, store_name + "." + file_type), "w", newline="\n") as f:
            writer = csv.writer(f)
            writer.writerows(store_info['products'])
        print(f"SAVED: {os.path.join(output_folder, store_name + '.' + file_type)} ")
    else:
        with open(os.path.join(output_folder, store_name + "." + file_type), 'w') as f:
            json.dump(store_info, f)

        print(f"SAVED: {os.path.join(output_folder, store_name + '.' + file_type)}")


print("Done")



