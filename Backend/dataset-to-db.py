import getpass
import pandas as pd
from mysql.connector import connect

CUTOFF_YEAR_START = 2008 # Table O only has data for 2008 onwards, so we'll make all the data start from 2008 onwards
CUTOFF_YEAR_END = 2020 # Use pre-covid data only as discussed with mentor (range is exclusive at the end so this'll end at 2019)

def create_db_and_tables(connection):
	with connection.cursor() as cursor:
		cursor.execute("CREATE DATABASE IF NOT EXISTS energy")
		cursor.execute("USE energy")
		cursor.execute("""CREATE TABLE IF NOT EXISTS regions (
			region_id INT PRIMARY KEY AUTO_INCREMENT,
			region_name VARCHAR(20)
		)""")
		cursor.execute("""CREATE TABLE IF NOT EXISTS energy_consumption (
			ID INT PRIMARY KEY AUTO_INCREMENT,
			region_id INT,
			financial_start_year INT,
			electricity_usage DOUBLE,
			gas_usage DOUBLE,
		 	FOREIGN KEY (region_id)
				REFERENCES regions(region_id)
		)""")
		cursor.execute("""CREATE TABLE IF NOT EXISTS energy_generation (
			ID INT PRIMARY KEY AUTO_INCREMENT,
			region_id INT,
			financial_start_year INT,
			non_renewable_electricity_total DOUBLE,
			renewable_electricity_total DOUBLE,
		 	total_electricity_generation DOUBLE,
		 	total_gas_generation DOUBLE,
		 	FOREIGN KEY (region_id)
				REFERENCES regions(region_id)
		)""")
		connection.commit()

def start_year_and_filter(df: pd.DataFrame):
	"""Create a "start year" column from the financial year, then filter the data to start from the cutoff year."""
	# For the database we need to extract the start year
	# e.g. "1960-61" should be transformed to start year: 1960
	# End year can always be obtained by adding 1 to the start year
	df["Start Year"] = df["Financial Year"].map(lambda years: int(years.split("-")[0]))
	df.drop(columns = ["Financial Year"], inplace = True)

	# Only use data from the cutoff year onwards
	return df[(CUTOFF_YEAR_START <= df["Start Year"]) & (df["Start Year"] < CUTOFF_YEAR_END)]

def load_table_L():
	"""Table L: Electricity consumption"""
	# 4 unnecessary rows at the top, column A is empty
	df = pd.read_excel("Australian Energy Statistics 2022 Table L.xlsx",
		    sheet_name = "Aus",
			skiprows = 4,
			usecols = "B:J")
	# Remove the row that just says "GWh"
	df.drop(index = 0, inplace = True)
	df.rename(columns = {"Unnamed: 1": "Financial Year"}, inplace = True)
	# The last 3 rows are just notes
	df = df.iloc[:-3]
	
	return start_year_and_filter(df)

def load_table_O_helper(sheet_name: str, cols: str):
	df = pd.read_excel("Australian Energy Statistics 2022 Table O.xlsx",
		    sheet_name = sheet_name,
			skiprows = 4,
			usecols = cols)
	df.rename(columns = {"Unnamed: 1": "Source"}, inplace = True)

	# We only need the rows with the total non-renewable, total renewable, and overall total numbers
	df = df[(df.Source == "Total non-renewable") | (df.Source == "Total renewable") | (df.Source == "Total")]
	# Transpose because the data should be the other way around
	# Required for the transpose to work, otherwise pandas will add its own indexes at the top
	df.set_index("Source", inplace = True)
	df = df.transpose()
	# Pandas starts being weird here - we need to get the row numbers back and rename the column to Financial Year
	# But for some reason this moves the "Source" name to the row numbers column so we need to get rid of that
	df.reset_index(names = "Financial Year", inplace = True)
	df.rename_axis(None, axis = 1, inplace = True)

	return start_year_and_filter(df)

def load_table_O():
	"""Table O: Electricity generation"""
	# The AUS sheet has more data than the sheets for each state/territory
	# but thankfully all the states/territories have the same amount of data as each other
	# So we use the columns parameter to specify which columns the data is in
	dataframes_per_region = {
		"AUS": load_table_O_helper("AUS FY", "B:AH")
	}
	for region in ["NSW", "VIC", "QLD", "WA", "SA", "TAS", "NT"]:
		dataframes_per_region[region] = load_table_O_helper(f"{region} FY", "B:O")
	
	return dataframes_per_region

def load_table_Q_helper(sheet_name: str, cols: str, number_of_notes_rows: int):
	# The dataset only provides Mcm (million cubic metres) rather than energy in Joules
	# Table 7 of the dataset guide (https://www.energy.gov.au/sites/default/files/Guide%20to%20the%20Australian%20Energy%20Statistics%202022.pdf)
	# provides a conversion factor of 37.9 megajoules / cubic metre
	# Hence, 0.0379 for petajoules / million cubic metres
	MCM_TO_PJ_CONVERSION_FACTOR = 0.0379

	df = pd.read_excel("Australian Energy Statistics 2022 Table Q.xlsx",
		    sheet_name = sheet_name,
			skiprows = 4,
			usecols = cols)
	df.rename(columns = {"Unnamed: 1": "Financial Year"}, inplace = True)
	df.drop(index = 0, inplace = True) # Remove the row that just says "Mcm"
	df = df.iloc[:-number_of_notes_rows] # The last 3-4 rows are just notes (depends on sheet)

	# Convert all the numeric values from million cubic metres to petajoules
	for column in df.columns:
		if column != "Financial Year":
			df[column] = df[column].map(lambda value: round(value * MCM_TO_PJ_CONVERSION_FACTOR, 3))
	
	return start_year_and_filter(df)

def load_table_Q():
	"""Table Q: Production and consumption of gas"""
	# The generation table is missing data for Tasmania (are they not a gas producer?)
	# so the list of columns are slightly different for each sheet
	# The generation table also has one more row of notes than the consumption table
	consumption = load_table_Q_helper("Consumption physical units", "B:J", 3)
	generation = load_table_Q_helper("Production physical units", "B:I", 4)
	# To deal with missing Tasmania data, add a new column and set it all to 0.0 (float because it's a double in the database)
	generation["Tasmania"] = 0.0
	return (consumption, generation)

def load_data_into_db(connection):
	elec_consumption_df = load_table_L()
	elec_generation_df = load_table_O()
	gas_consumption_df, gas_generation_df = load_table_Q()

	region_to_id = {"Australia": 1, "Victoria": 2, "New South Wales": 3, "Queensland": 4, "South Australia": 5, "Northern Territory": 6, "Western Australia": 7, "Tasmania": 8}
	regions = ["Australia", "Victoria", "New South Wales", "Queensland", "South Australia", "Northern Territory", "Western Australia", "Tasmania"]
	abbrevs = ["AUS", "VIC", "NSW", "QLD", "SA", "NT", "WA", "TAS"] # Needed because of table O's sheet names

	with connection.cursor() as cursor:
		# Create the regions first, otherwise the foreign key constraints will fail in the later tables
		for region in regions:
			cursor.execute("INSERT INTO regions (region_name) VALUES (%s)", (region, ))

		# Once all the regions are added, extract the data from the dataframes and insert it
		for region, abbrev in zip(regions, abbrevs):
			for year in range(CUTOFF_YEAR_START, CUTOFF_YEAR_END):
				# energy_consumption table
				elec_usage = elec_consumption_df[elec_consumption_df["Start Year"] == year][region].values[0]
				gas_usage = gas_consumption_df[gas_consumption_df["Start Year"] == year][region].values[0]
				cursor.execute(
					"INSERT INTO energy_consumption (region_id, financial_start_year, electricity_usage, gas_usage) VALUES (%s, %s, %s, %s)",
					(region_to_id[region], year, elec_usage, gas_usage))

				# energy_generation table
				elec_gen_region = elec_generation_df[abbrev] # Get the dataset for the specific state/territory or whole country
				elec_gen_row = elec_gen_region[elec_gen_region["Start Year"] == year]
				# Extract the 3 values out of the row
				non_renewable = elec_gen_row["Total non-renewable"].values[0]
				renewable = elec_gen_row["Total renewable"].values[0]
				elec_gen_total = elec_gen_row["Total"].values[0]
				gas_gen = gas_generation_df[gas_generation_df["Start Year"] == year][region].values[0]
				cursor.execute(
					"INSERT INTO energy_generation (region_id, financial_start_year, non_renewable_electricity_total, renewable_electricity_total, total_electricity_generation, total_gas_generation) VALUES (%s, %s, %s, %s, %s, %s)",
					(region_to_id[region], year, non_renewable, renewable, elec_gen_total, gas_gen))
		
		connection.commit()
	
# mysql -h ta21-2023s2.mysql.database.azure.com -u TA21 -p
def main():
	with connect(host = "ta21-2023s2.mysql.database.azure.com", user = "TA21", password = getpass.getpass()) as connection:
		print("Connected!")
		create_db_and_tables(connection)
		load_data_into_db(connection)

main()
