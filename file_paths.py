"""
File paths to data
"""

from pathlib import Path

# the data storage
base_data_folder = Path.home() / "Analysis/Australian economy/Data"


dict_data_folder = base_data_folder / "Dictionaries"

abs_data_folder = base_data_folder / "ABS"

# Home Affairs stock data
stock_data_folder = base_data_folder / "Stock"

#NOM unit record
unit_record_folder = base_data_folder / "NOM unit record data"
individual_movements_folder = unit_record_folder / "NOM individual movements"
abs_nom_propensity = unit_record_folder / "ABS propensity"
abs_nom_data_parquet_folder = unit_record_folder / "Traveller Characteristics Parquet"

# Grant data
grant_data_folder = base_data_folder / "Grant"

#visa_data
program_data_folder = base_data_folder / "Visa"

# Australian Statistical Geography Standard (ASGS)
asgs_folder = abs_data_folder / "ASGS"
shapely = asgs_folder / "shapely"