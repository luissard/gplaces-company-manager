from google_places_manager import GooglePlacesManager

# Initialization and usage of the class
manager = GooglePlacesManager()

frequency_days_to_update = 30
limit = 200

manager.update_company_details(frequency_days_to_update, limit)
