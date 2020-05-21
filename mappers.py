"""
dicts for mapping NOM data
"""


# Traveller characteristic dictionaries
state_residence_original = {
    '09': "not stated",
    '01': "NSW",
    '02': "Victoria",
    '03': "Queensland",
    '04': "SA",
    '05': "WA",
    '06': "Tasmania",
    '07': "Northern Territory",
    '08': "ACT",
    'I': "Norfolk Island",
    'C': "Christmas",
    'K': "Cocos",
    'J': "Jervis Bay",
}

state_residence_other_territories = {
    '09': "not stated",
    '01': "NSW",
    '02': "Victoria",
    '03': "Queensland",
    '04': "SA",
    '05': "WA",
    '06': "Tasmania",
    '07': "Northern Territory",
    '08': "ACT",
    'I': "Other Territories",
    'C': "Other Territories",
    'K': "Other Territories",
    'J': "Other Territories",
}

nom_visa_group_dict = {
        "nom": "Total NOM",
        "special_eligibility_and_humanitarian": "Humanitarian",
        "australian_citizen": "Australian citizen",
        "temporary_work_skilled": "Temporary",
        "visitor": "Temporary",
        "bridging": "Temporary",
        "new_zealand_citizen": "New Zealand citizen",
        "student": "Temporary",
        "other_temporary": "Temporary",
        "working_holiday": "Temporary",
        "family": "Permanent",
        "skill": "Permanent",
        "other": "unknown",
        "other_permanent": "Permanent",
    }

nom_group_label_dict = {
        "nom": "Total NOM",
        "special_eligibility_and_humanitarian": "Humanitarian",
        "australian_citizen": "Australian",
        "temporary_work_skilled": "Skilled: temp",
        "visitor": "Visitor",
        "bridging": "Bridging",
        "new_zealand_citizen": "New Zealander",
        "student": "Student",
        "other_temporary": "Other: temp",
        "working_holiday": "Working holiday",
        "family": "Family",
        "skill": "Skilled: perm",
        "other": "Unknown",
        "other_permanent": "Other: perm",
    }