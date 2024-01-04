import requests


def get_labels(property_ids):
    base_url = "https://www.wikidata.org/w/api.php"
    params = {
        "action": "wbgetentities",
        "ids": "|".join(property_ids),
        "languages": "en",  # You can change the language if needed
        "format": "json",
    }

    response = requests.get(url=base_url, params=params)
    data = response.json()

    property_labels = {}
    for prop_id, prop_data in data["entities"].items():
        if "labels" in prop_data and "en" in prop_data["labels"]:
            label = prop_data["labels"]["en"]["value"]
            property_labels[prop_id] = label

    return property_labels


def get_subclasses(entity_id):
    base_url = "https://query.wikidata.org/sparql"
    query = f"""
    SELECT ?subclass ?subclassLabel WHERE {{
      ?subclass wdt:P279* wd:{entity_id}.
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }}
    }}
    """

    headers = {
        "Accept": "application/json",
    }

    params = {"query": query, "format": "json"}

    response = requests.get(url=base_url, headers=headers, params=params)
    data = response.json()

    subclasses = []
    for item in data["results"]["bindings"]:
        subclass_id = item["subclass"]["value"].split("/")[-1]
        subclass_label = item["subclassLabel"]["value"]
        subclasses.append({"id": subclass_id, "label": subclass_label})

    return subclasses

if __name__ == "__main__":
    property_ids = ["P31", "Q5"]  # Add more property IDs as needed
    labels = get_labels(property_ids)

    for prop_id, label in labels.items():
        print(f"Property ID: {prop_id}, Label: {label}")

#     entity_id = "Q13406463"  # Replace with the desired Wikidata entity ID
#     subclasses = get_subclasses(entity_id)
#
#     print(f"Subclasses of {entity_id}:")
#     for subclass in subclasses:
#         print(subclass)
