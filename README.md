
# GooglePlacesManager

The `GooglePlacesManager` class is designed to manage interactions with the Google Places API, including searching for companies, storing company details in a SQLite database, and managing API usage costs.

## Features

- Initialize Google Maps client with an API key.
- Connect to a SQLite database for data persistence.
- Perform Google Places API requests based on predefined queries.
- Store and update company details based on API responses.
- Limit API usage based on cost considerations.

## Configuration

Before using the `GooglePlacesManager`, you need to set up a configuration file (`config.ini`) with the following structure:

```ini
[DEFAULT]
GoogleApiKey = YOUR_GOOGLE_PLACES_API_KEY
DatabasePath = PATH_TO_YOUR_SQLITE_DATABASE
MaxMonthlyCost = MAXIMUM_API_COST_YOU_WANT_TO_ALLOW
PlaceDetailsQueryCost = COST_PER_PLACE_DETAILS_QUERY
PlaceSearchQueryCost = COST_PER_PLACE_SEARCH_QUERY

[QUERIES]
CompanyQueries = QUERY_1, QUERY_2, QUERY_3, ...
```

Replace the placeholders with your actual API key, database path, cost limits, and queries.

You can just copy the config.example.ini in config.ini and fill the information.

## Usage

To use the `GooglePlacesManager`, instantiate the class and call its methods as needed:

```python
from GooglePlacesManager import GooglePlacesManager

# Initialize the manager
manager = GooglePlacesManager()

# Update company details for companies not updated in the last 30 days
manager.update_company_details(frequency_days_to_update=30, limit=200)

# Update companies for outdated sections
manager.update_companies(limit=20)
```

## Predefined Sections Data

The `sections.json` file contains predefined data about geographic sections, obtained from [Simplemaps](https://simplemaps.com/). It includes all cities in Spain with more than 20,000 inhabitants, providing their latitude, longitude, and population. The script uses this data as a base for searching for companies.

## Dependencies

- `googlemaps`: For interacting with the Google Maps API.
- `sqlite3`: For database management.
- `configparser`: For managing configuration files.
- Standard libraries: `json`, `math`, `time`, `datetime`.

Ensure all dependencies are installed before using the `GooglePlacesManager` class.

## Limitations

- API costs are managed simplistically; ensure you monitor your actual usage via the Google Cloud Console.
- The database schema is predefined; customizations require modifications to the class methods.

## Contributors

https://github.com/ansardg
https://github.com/luissard

## Contributing

Feel free to fork the repository and submit pull requests with enhancements or bug fixes.
