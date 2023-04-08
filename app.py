import mysql.connector
import requests
from flask import Flask, request, jsonify

app = Flask(__name__)

# MySQL connection settings
mysql_host = 'localhost'
mysql_user = 'weather'
mysql_password = 'password'
mysql_database = 'weather_data'

# OpenWeatherMap API settings
api_key = '2a440a9bef8974cd9f89fd9976ce8bae'
api_url = 'http://api.openweathermap.org/data/2.5/weather?q={}&appid={}'

# Helper function to get weather data from OpenWeatherMap API
def get_weather_data(city_name):
    response = requests.get(api_url.format(city_name, api_key))
    if response.status_code == 200:
        data = response.json()
        temperature_kelvin = data['main']['temp']
        temperature_celsius = temperature_kelvin - 273.15
        return {
            'city': data['name'],
            'temperature_celsius': temperature_celsius,
            'description': data['weather'][0]['description']
        }
    else:
        return None

def get_weather_data(city):
    url = f'http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric'
    response = requests.get(url)
    try:
        data = response.json()
        temperature = data['main']['temp']
        description = data['weather'][0]['description']
        return {'temperature': temperature, 'description': description}
    except:
        print('Error occurred while getting weather data')
        return None



# Helper function to create MySQL table if it doesn't exist
def create_table_if_not_exists(cursor):
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS weather (
            city VARCHAR(255) PRIMARY KEY,
            temperature FLOAT,
            description VARCHAR(255)
        )
    ''')

# Endpoint to search for weather data by city name
@app.route('/weather')
def search_weather():
    city_name = request.args.get('city')
    print('search_weather: city_name =', city_name)
    if not city_name:
        return jsonify({'error': 'City name is required'}), 400

    # Connect to MySQL database
    mysql_conn = mysql.connector.connect(
        host=mysql_host,
        user=mysql_user,
        password=mysql_password,
        database=mysql_database
    )
    cursor = mysql_conn.cursor()

    # Create table if it doesn't exist
    create_table_if_not_exists(cursor)

    # Check if city exists in database
    cursor.execute('SELECT * FROM weather WHERE city = %s', (city_name,))
    result = cursor.fetchone()
    if result:
        # City exists, return weather data from database
        data = {
            'city': result[0],
            'temperature_celsius': result[1],
            'description': result[2]
        }
        print('search_weather: returning data from database')
    else:
        # City doesn't exist, get weather data from OpenWeatherMap API and store in database
        data = get_weather_data(city_name)
        if data:
            cursor.execute('INSERT INTO weather VALUES (%s, %s, %s)', (
                data['city'],
                data['temperature_celsius'],
                data['description']
            ))
            mysql_conn.commit()
            print('search_weather: inserted data into database')
        else:
            print('search_weather: could not retrieve weather data from API')

    # Close database connection
    cursor.close()
    mysql_conn.close()

    if not data:
        return jsonify({'error': 'Could not retrieve weather data for {}'.format(city_name)}), 404

    return jsonify(data)


@app.route('/weather', methods=['POST'])
def add_city():
    city_name = request.json.get('city')
    if not city_name:
        return jsonify({'error': 'City name is required'}), 400

    # Get weather data from OpenWeatherMap API and store in database
    data = get_weather_data(city_name)
    if data:
        mysql_conn = mysql.connector.connect(
            host=mysql_host,
            user=mysql_user,
            password=mysql_password,
            database=mysql_database
        )
        cursor = mysql_conn.cursor()
        create_table_if_not_exists(cursor)

        # check if 'temperature' key exists in data before inserting into database
        if 'temperature_celsius' not in data:
            print(f"Temperature data not found. Data: {data}")
            return jsonify({'error': 'Temperature data not found'}), 404

        try:
            cursor.execute(
                'INSERT INTO weather (city, temperature, description) VALUES (%s, %s, %s)',
                (data['city'], data['temperature_celsius'], data['description'])
            )
            mysql_conn.commit()
            print(f"Data inserted successfully. Data: {data}")
            return jsonify({'success': 'City added successfully'}), 201

        except Exception as e:
            mysql_conn.rollback()
            print(f"Error inserting data. Exception: {e}")
            return jsonify({'error': 'Error inserting data'}), 500

        finally:
            cursor.close()
            mysql_conn.close()
    else:
        return jsonify({'error': 'City not found'}), 404

#UPDATE CITY WEATHER
@app.route('/weather/<city>', methods=['PUT'])
def update_city(city):
    # Check if city exists in database
    mysql_conn = mysql.connector.connect(
        host=mysql_host,
        user=mysql_user,
        password=mysql_password,
        database=mysql_database
    )
    cursor = mysql_conn.cursor()
    cursor.execute('SELECT * FROM weather WHERE city = %s', (city,))
    result = cursor.fetchone()
    if not result:
        return jsonify({'error': 'City not found'}), 404

    # Get weather data from OpenWeatherMap API and update in database
    data = get_weather_data(city)
    print('Data:', data)
    if data:
        temperature = data.get('temperature', None)
        description = data.get('description', None)
        if temperature is not None and description is not None:
            cursor.execute(
                'UPDATE weather SET temperature = %s, description = %s WHERE city = %s',
                (temperature, description, city)
            )
            mysql_conn.commit()
            cursor.close()
            mysql_conn.close()
            return jsonify({'success': 'Weather updated successfully'}), 200
    return jsonify({'error': 'Weather data not found'}), 404


#DELETING CITY WEATHER
@app.route('/weather/<city>', methods=['DELETE'])
def delete_city(city):
    # Check if city exists in database
    mysql_conn = mysql.connector.connect(
        host=mysql_host,
        user=mysql_user,
        password=mysql_password,
        database=mysql_database
    )
    cursor = mysql_conn.cursor()
    cursor.execute('SELECT * FROM weather WHERE city = %s', (city,))
    result = cursor.fetchone()
    if not result:
        return jsonify({'error': 'City not found'}), 404

    # Delete city weather data from database
    cursor.execute('DELETE FROM weather WHERE city = %s', (city,))
    mysql_conn.commit()
    cursor.close()
    mysql_conn.close()

    return jsonify({'success': 'Weather data deleted successfully'}), 200


if __name__ == '__main__':
    app.run(debug=True)