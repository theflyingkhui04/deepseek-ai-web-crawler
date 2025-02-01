import csv


def is_duplicate_venue(venue_name: str, seen_names: set) -> bool:
    return venue_name in seen_names


def is_complete_venue(venue: dict, required_keys: list) -> bool:
    return all(key in venue for key in required_keys)


def save_venues_to_csv(venues: list, filename: str):
    with open(filename, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=venues[0].keys())
        writer.writeheader()
        writer.writerows(venues)
    print(f"Saved {len(venues)} venues to '{filename}'.")
