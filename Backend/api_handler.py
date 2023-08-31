# api_handler.py
import getpass
from flask import Flask, jsonify
from database_manager import DatabaseManager
# from flask_cors import CORS
import json

# Create a Flask app instance
app = Flask(__name__)
# CORS(app)

db_manager = DatabaseManager(
    host="ta21-2023s2.mysql.database.azure.com",
    user="TA21",
    password=getpass.getpass(),
    database="energy"
)

# Route to get data
@app.route('/api/get_data', methods=['GET'])
def get_data():
    # SQL query to get data from the database
    query = "SELECT r.region_id, r.region_name, c.financial_start_year, c.electricity_usage, c.gas_usage, g.non_renewable_electricity_total, g.renewable_electricity_total, g.total_electricity_generation, g.total_gas_generation FROM regions r LEFT JOIN energy_consumption c ON r.region_id = c.region_id RIGHT JOIN energy_generation g ON r.region_id = g.region_id AND c.financial_start_year = g.financial_start_year"

    # Execute the query using the DatabaseManager
    result = db_manager.execute_query(query)

    # Process the query result and format it as JSON
    data = []
    for each in result:
        region = each[1]
        year = each[2] + 1
        electricity_usage = round(each[3])
        gas_usage = round(each[4])
        elect_non_renewable_generated = round(each[5])
        elect_renewable_generated = round(each[6])
        total_elect_generated = round(each[7])
        total_gas_generated = round(each[8])

        data.append({
            'region': region,
            'financial year': year,
            'electricity_usage': electricity_usage,
            'gas_usage': gas_usage,
            'non_renewable_source_electricity_generated': elect_non_renewable_generated,
            'renewable_source_electricity_generated': elect_renewable_generated,
            'total_electricity_generated': total_elect_generated,
            'total_gas_generated': total_gas_generated
        })

    result_json = json.dumps(data)
    return result_json

if __name__ == '__main__':
    app.run()