"""
File paths to data
"""

from pathlib import Path

# the data storage
base_data_folder = Path.home() / "Analysis/Australian economy/Data"


dict_data_folder = base_data_folder / "Dictionaries"

abs_data_folder = base_data_folder / "ABS"

abs_leading_indicator = base_data_folder / "Leading indicator"

#ABS audit download
abs_audit_folder = abs_data_folder / "ABS data audit"

# Home Affairs stock data
stock_data_folder = base_data_folder / "Stock"

#NOM unit record
unit_record_folder = base_data_folder / "NOM unit record data"
individual_movements_folder = unit_record_folder / "NOM individual movements"
abs_nom_propensity = unit_record_folder / "ABS propensity"
abs_traveller_characteristics = unit_record_folder / "Traveller Characteristics Parquet"

# Grant data
grant_data_folder = base_data_folder / "Grant"

#visa_data
program_data_folder = base_data_folder / "Visa"
ha_grant_data_folder = program_data_folder / "Grants"

# Australian Statistical Geography Standard (ASGS)
asgs_folder = abs_data_folder / "ASGS"
shapely = asgs_folder / "shapely"

# Internet vacancy data
internet_vacancy_folder = base_data_folder / "internet_vacancy"

# Profiles analysis
profiles_folder = Path.home() / "Analysis/Australian economy/Visa analysis"