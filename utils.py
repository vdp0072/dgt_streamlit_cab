import json
import re
import logging


KNOWN_CITIES = [
    "pune",
    "mumbai",
    "nashik",
    "nagpur",
    "solapur",
    "kolhapur",
    "satara",
    "ahmednagar",
    "jalgaon",
    "thane",
]


def clean_text(x):
    # normalize whitespace, lower-case, strip punctuation except commas
    if not x:
        return ""
    s = str(x).strip()
    # collapse multiple spaces
    s = re.sub(r"\s+", " ", s)
    return s.lower()


def normalize_phone(number: str):
    """Normalize phone to national format (10 digits) and E.164 (+91...) when possible.

    Returns (phone, e164) where phone is the 10-digit string or None if invalid.
    """
    if not number:
        return None, None
    s = str(number)
    # remove non-digits
    digits = re.sub(r"\D", "", s)
    if len(digits) == 10:
        return digits, f"+91{digits}"
    # handle leading country code like 91XXXXXXXXXX
    if len(digits) == 12 and digits.startswith("91"):
        core = digits[-10:]
        return core, f"+{digits}"
    if len(digits) == 11 and digits.startswith("0"):
        core = digits[-10:]
        return core, f"+91{core}"
    # e164 full with + and country
    if len(digits) > 10 and digits.endswith(digits[-10:]):
        core = digits[-10:]
        return core, f"+{digits}"
    return None, None


def extract_state_country(belong_area):
    if not belong_area:
        return None, None
    parts = clean_text(belong_area).split(",")
    state = parts[0].strip() if len(parts) > 0 else None
    country = parts[1].strip() if len(parts) > 1 else None
    return state, country


def extract_city(address, result_loc, belong_area):
    text = " ".join([clean_text(address), clean_text(result_loc), clean_text(belong_area)])
    for city in KNOWN_CITIES:
        if city in text:
            return city.title()
    return None


def classify(city, state):
    if city == "Pune":
        return True, "Pune"
    if state and state.lower() == "maharashtra":
        return False, "Maharashtra_Other"
    return False, "Other_State"


def parse_raw_json_field(raw_field):
    """raw_field is a string that should contain JSON. Return dict or {}"""
    if not raw_field:
        return {}
    try:
        return json.loads(raw_field)
    except Exception:
        # raw_field might be already a dict
        if isinstance(raw_field, dict):
            return raw_field
        return {}
