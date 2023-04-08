import mysql.connector
import requests

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
        return {
            'city': data['name'],
            'temperature': data['main']['temp'],
            'description': data['weather'][0]['description']
        }
    else:
        return None

# Lambda function handler
def lambda_handler(event, context):
    # Connect to MySQL database
    mysql_conn = mysql.connector.connect(
        host=mysql_host,
        user=mysql_user,
        password=mysql_password,
        database=mysql_database
    )
    cursor = mysql_conn.cursor()

    # Get list of cities to update
    cursor.execute('SELECT DISTINCT city FROM weather')
    cities = [row[0] for row in cursor.fetchall()]

    # Update weather data for each city
    for city in cities:
        data = get_weather_data(city)
        if data:
            cursor.execute('UPDATE weather SET temperature = %s, description = %s WHERE city = %s', (
                data['temperature'],
                data['description'],
                data['city']
            ))
            mysql_conn.commit()

    # Close database connection
    cursor.close()
    mysql_conn.close()

    return {
        'statusCode': 200,
        'body': 'Weather data updated'
    }
