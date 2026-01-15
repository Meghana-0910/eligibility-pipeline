from dataclasses import dataclass

@dataclass
class EligibilityRecord:
    external_id: str
    first_name: str
    last_name: str
    dob: str
    email: str
    phone: str
    partner_code: str
