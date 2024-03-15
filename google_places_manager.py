import json
import math
import re
import time
import googlemaps
import sqlite3
import datetime
import configparser


class GooglePlacesManager:

    def __init__(self):
        """Constructor initializing the Google Maps client, SQLite database connection, and API consumption limits."""
        config = configparser.ConfigParser()
        config.read("config.ini")

        try:
            self.gmaps = googlemaps.Client(key=config['DEFAULT']['GoogleApiKey'])
            self.conn = sqlite3.connect(config['DEFAULT']['DatabasePath'])
            self.cursor = self.conn.cursor()
            self.max_monthly_cost = config['DEFAULT'].getfloat('MaxMonthlyCost')
            self.place_details_query_cost = config['DEFAULT'].getfloat('PlaceDetailsQueryCost')
            self.place_search_query_cost = config['DEFAULT'].getfloat('PlaceSearchQueryCost')
            self.place_photo_query_cost = config['DEFAULT'].getfloat('PlacePhotoQueryCost')
            self.current_company_queries = config['QUERIES']['CompanyQueries'].split(', ')
            self.debug_mode = config['DEFAULT']['DEBUG'] == '1'
            self.default_query_cost = 1
            self._create_tables()
            self.current_company_details = None
        except Exception as e:
            self.error('Please complete your config.ini #Error: ' + repr(e), True)

    def error(self, msg, do_exit=False):
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(msg)

        ''' Writes in the log file the actual hour and date with the error messagge if debug mode is enabled. '''
        if self.debug_mode:
            with open('log/debug.log', 'a') as log_file:
                log_file.write(f'[{timestamp}] ERROR: {msg}\n')

        if do_exit:
            exit(0)

    def _register_api_cost(self, cost):
        """Registers the cost of API calls and checks if it exceeds the monthly limit."""
        year, month = datetime.date.today().year, datetime.date.today().month

        # Update or insert the cost
        self.cursor.execute('SELECT cost, query_count FROM api_costs WHERE year = ? AND month = ?', (year, month))
        result = self.cursor.fetchone()

        if result:
            total_cost = round(result[0] + cost, 6)
            query_count = result[1] + 1
            if total_cost > self.max_monthly_cost:
                return False
            self.cursor.execute('UPDATE api_costs SET cost = ?, query_count = ? WHERE year = ? AND month = ?',
                                (total_cost, query_count, year, month))
        else:
            if cost > self.max_monthly_cost:
                return False
            self.cursor.execute('INSERT INTO api_costs (year, month, cost, query_count) VALUES (?, ?, ?, ?)',
                                (year, month, cost, 1))

        self.conn.commit()
        return True

    def get_query_cost_by_type(self, query_type):
        """ Calculates the cost of API queries based on the type of query assuming all queries are Preferred
            IMPORTANT!!: frequently update this values based on the Google documentation
            @see https://developers.google.com/maps/documentation/places/web-service/usage-and-billing
            Please add as many types as needed
        """
        if query_type == 'place_details':
            return self.place_details_query_cost
        elif query_type == 'text_search':
            return self.place_search_query_cost
        elif query_type == 'place_photo':
            return self.place_photo_query_cost
        else:
            return self.default_query_cost

    def google_places_request(self, request_type, query_model, params, tries=0):
        """Check if we have monthly cost available before performing the query"""
        if 'page_token' in params:
            time.sleep(2)
        try:
            cost = self.get_query_cost_by_type(request_type)
            if not self._register_api_cost(cost):
                self.error('Monthly API cost limit reached. Exiting', True)
            if query_model == 'place':
                return self.gmaps.place(**params)
            elif query_model == 'places':
                return self.gmaps.places(**params)
            elif query_model == 'photo':
                if 'photo_reference' in params:
                    return self.gmaps.places_photo(**params)
                self.error('Missing photo reference for photos request. Exiting.', True)

            self.error('The query model is invalid. Exiting', True)
        except Exception as e:
            time.sleep(10)
            if tries < 1:
                self.google_places_request(request_type, query_model, params, tries=tries + 1)
                self.error('The query returned an error' + repr(e), True)

    def update_company_details(self, frequency_days_to_update, limit=200):
        """Updates the details of the companies stored in the database not updated in the last
        frequency_days_to_update"""
        self.cursor.execute('''
            SELECT place_id, name FROM company 
            WHERE (julianday(?) - julianday(detail_updated_at)) > (?) OR detail_updated_at IS NULL
            ORDER BY section_id ASC LIMIT ?          
            ''', (datetime.date.today(), frequency_days_to_update, limit)
                            )
        companies_to_update = self.cursor.fetchall()
        print(f"Updating {len(companies_to_update)} company details...")

        for company in companies_to_update:
            print(f"Updating {company[1]} ...")
            place_id = company[0]
            params = {
                'place_id': place_id,
                'fields': ['website', 'formatted_phone_number', 'rating', 'reviews', 'user_ratings_total', 'opening_hours', 'photo'],
                'language': 'es'
            }
            self.current_company_details = self.google_places_request('place_details', 'place', params)

            website = self.current_company_details['result'].get('website')
            phone_number = self.current_company_details['result'].get('formatted_phone_number')
            avg_reviews = self.current_company_details['result'].get('rating')
            all_reviews_json = self.get_all_reviews_json()
            total_reviews = self.current_company_details['result'].get('user_ratings_total')
            opening_hours = self.get_opening_hours_json()
            photo = self.get_company_photo()

            self.cursor.execute('''
                INSERT INTO company_details 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(place_id) DO 
                UPDATE SET website = ?, phone_number = ?, total_reviews = ?, avg_reviews = ?, reviews = ?
                , opening_hours = ?, place_photo = ?, updated_at = ?                
            ''', (
                place_id, website, phone_number, total_reviews, avg_reviews, all_reviews_json, opening_hours, photo,
                datetime.date.today().strftime('%Y-%m-%d'), website, phone_number, total_reviews, avg_reviews,
                all_reviews_json, opening_hours, photo, datetime.date.today().strftime('%Y-%m-%d')
            )
                                )

            self.cursor.execute("UPDATE company SET detail_updated_at = ? WHERE place_id = ?"
                                , (datetime.date.today().strftime('%Y-%m-%d'), place_id))

            self.conn.commit()

    def get_company_photo(self):
        """ Get first company photo from company using google place photos API request """

        if 'photos' in self.current_company_details['result']:
            try:
                params = {
                    'photo_reference': self.current_company_details['result']['photos'][0].get('photo_reference'),
                    'max_height': 1600,
                    'max_width': 1600
                }
                place_photo = self.google_places_request('place_photo', 'photo', params)
                return place_photo.gi_frame.f_locals['self'].request.url
            except Exception:
                return ''

        return ''

    def get_opening_hours_json(self):
        weekday_text = []

        if 'opening_hours' in self.current_company_details['result']:
            opening_hours = self.current_company_details['result'].get('opening_hours')
            if 'weekday_text' in opening_hours:
                weekday_text = opening_hours['weekday_text']

        return json.dumps(weekday_text)

    def get_all_reviews_json(self):
        """ We can only get 5 reviews per query to API, pagination token can be adquired """
        all_reviews_data = []

        if 'reviews' in self.current_company_details['result']:
            reviews = self.current_company_details['result']['reviews']
            for review in reviews:
                review_dict = {
                    'author_name': review.get('author_name'),
                    'author_url': review.get('author_url'),
                    'language': review.get('language'),
                    'original_language': review.get('original_language'),
                    'profile_photo_url': review.get('profile_photo_url'),
                    'rating': review.get('rating'),
                    'relative_time_description': review.get('relative_time_description'),
                    'text': review.get('text'),
                    'time': review.get('time'),
                    'translated': review.get('translated')
                }

                all_reviews_data.append(review_dict)

        return json.dumps(all_reviews_data)

    def search_and_store_companies(self, lat, lon, section_id, population):
        """Searches for companies in the vicinity and stores them in the database."""
        if len(self.current_company_queries) == 0:
            self.error('There are no queries to get companies, please review your config.ini', True)

        """
        Dynamically adjust the number of queries based on the population size.
        If the population is below certain thresholds, fewer queries are used.
        This is determined by comparing the population to a list of thresholds and counting
        how many thresholds are exceeded, which corresponds to the number of queries to use.
        """
        population_threshold = [0, 50000, 150000, 300000]
        queries = []
        num_queries = sum(population >= threshold for threshold in population_threshold)
        queries.extend(self.current_company_queries[:num_queries])

        for query in queries:
            if query == "":
                self.error('There are no queries to get companies, please review your config.ini', True)

            next_page = None
            cont = 0
            max_companies_per_section = 1000  # Update as desired
            companies_per_page = 20  # Assuming Google Place API returns 20 results per request

            while (cont == 0 or next_page) and cont <= math.ceil(max_companies_per_section / companies_per_page):
                cont += 1

                params = {
                    'query': query,
                    'location': f'{lat},{lon}',
                    'radius': 50000,
                    'language': 'es'
                }

                if next_page:
                    params['page_token'] = next_page

                search_results = self.google_places_request('text_search', 'places', params)

                if 'next_page_token' in search_results:
                    next_page = search_results.get('next_page_token')
                    params['next_page_token'] = next_page
                else:
                    next_page = None

                committed = False
                if 'results' in search_results:
                    for result in search_results['results']:
                        if 'place_id' in result and 'name' in result:
                            today_str = datetime.date.today().strftime('%Y-%m-%d')
                            country, state, city, address, postal_code = self.parse_address(result['formatted_address'])

                            self.cursor.execute('''INSERT INTO company (place_id, name, section_id, country, state, 
                                                    city, address, postal_code, updated_at)
                                                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(place_id) DO 
                                                   UPDATE SET name = ?, section_id = ?, country = ?, state = ?, city = ?, 
                                                   address = ?, postal_code = ?, updated_at = ?''',
                                                (result['place_id'], result['name'],
                                                 section_id, country, state, city, address, postal_code, today_str,
                                                 result['name'], section_id, country, state, city, address, postal_code,
                                                 today_str))
                            self.conn.commit()
                            committed = True
                if not committed:
                    self.error(f'Could not find result for latitude {lat} and longitude {lon}.')

    @staticmethod
    def has_postal_code(address_element):
        # Regular expression to find if the address element has a spanish postal code (5 digits)
        postal_code_match = re.search(r'\d{5}\b', address_element)
        if postal_code_match:
            return True
        return False

    @staticmethod
    def remove_postal_code(text):
        """Remove postal code from passed text"""
        return re.sub(r'\b\d{5}\b', '', text).strip()

    def parse_address(self, address_string):

        country = state = city = address = postal_code = ''
        splitted_result = address_string.split(',')
        splitted_result.reverse()
        address_parts = []

        for part in splitted_result:
            part = part.strip()
            if postal_code == '' and self.has_postal_code(part):
                subpart = part.split(' ', 1)
                if len(subpart) == 2:
                    postal_code = subpart[0]
                    city = subpart[1]
                else:
                    postal_code = subpart[0]

            if country == '':
                country = part
            elif state == '':
                state = part
            elif city == '' or city in part:
                city = part
            else:
                address_parts.append(part)

        country = self.remove_postal_code(country)
        state = self.remove_postal_code(state)
        city = self.remove_postal_code(city)
        address_parts.reverse()
        address_parts = [self.remove_postal_code(part) for part in address_parts]
        address = ', '.join(address_parts)

        if city == '':
            city = state

        return country, state, city, address, postal_code

    def get_most_outdated_sections(self, limit=20):
        """ Gets sections ordering by date, then returns the section_ids """
        self.cursor.execute('''
            SELECT s.section_id FROM section s
            LEFT JOIN company c ON c.section_id = s.section_id
            ORDER BY c.updated_at ASC, RANDOM()
            LIMIT ?
        ''', (limit,))

        return self.cursor.fetchall()

    def get_section(self, selected_section):
        self.cursor.execute("SELECT * FROM section WHERE section_id = ?"
                            , selected_section)
        section_row = self.cursor.fetchone()
        section = {
            'section_id': section_row[0],
            'name': section_row[1],
            'lat': section_row[2],
            'lon': section_row[3],
            'population': section_row[4]
        }
        return section

    def update_companies(self, sections_limit=10):

        outdated_sections = self.get_most_outdated_sections(sections_limit)
        for outdated_section in outdated_sections:
            selected_section = self.get_section(outdated_section)

            if selected_section:
                print(f"Querying {selected_section.get('name')}: Coordinates: {selected_section.get('lat')} "
                      f"| {selected_section.get('lon')}")
                self.search_and_store_companies(
                    selected_section['lat'], selected_section['lon'], selected_section['section_id'],
                    selected_section['population']
                )
            else:
                print("There are no sections in the database... Exiting")
                exit(0)

        self.close_connection()

    def insert_section_data_samples(self):
        """Inserts predefined geographic sections covering different parts of Spain."""
        self.cursor.execute("SELECT COUNT(*) FROM section")
        count = self.cursor.fetchone()[0]

        if count > 0:
            return

        # Obtained from https://simplemaps.com/
        with open('sections.json', 'r') as file:
            section_data = json.load(file)

        for section, data in section_data.items():
            self.cursor.execute(
                f"INSERT INTO section (name, lat, lon, population) "
                f"VALUES ('{section}', {data['lat']}, {data['lon']}, {data['population']});"
            )
            self.conn.commit()

    def _create_tables(self):
        """Creates necessary tables in the database if they do not already exist."""
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS company_details (
                place_id TEXT PRIMARY KEY,                
                website TEXT,
                phone_number TEXT,
                total_reviews INTEGER,
                avg_reviews FLOAT,
                reviews TEXT,
                opening_hours TEXT,
                place_photo TEXT,
                updated_at DATE
            )
        ''')
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS company (
                place_id TEXT PRIMARY KEY,
                name TEXT,
                section_id INTEGER,
                country TEXT,                
                state TEXT,
                city TEXT,
                address TEXT,                
                postal_code TEXT,                
                updated_at DATE,
                detail_updated_at DATE
            )
        ''')
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS section (
                section_id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT,
                lat FLOAT,
                lon FLOAT,
                population FLOAT
            )
        ''')

        self.insert_section_data_samples()

        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS api_costs (
                year INTEGER,
                month INTEGER,
                query_count INTEGER,
                cost REAL,
                PRIMARY KEY (year, month)
            )
        ''')
        self.conn.commit()

    def close_connection(self):
        """Closes the SQLite database connection."""
        self.conn.close()
