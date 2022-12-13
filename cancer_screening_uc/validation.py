from pyspark.sql.functions import *
from pyspark.sql.types import *

@udf(returnType=StringType())
def strict_validation(smpi_first_name,
                      smpi_last_name,
                      smpi_dob,
                      smpi_gender,
                      qe_first_name,
                      qe_last_name,
                      qe_dob,
                      qe_gender):
    import json
    validation_errors = []
    if smpi_first_name != qe_first_name:
        error = {'msg': 'smpi_first_name != qe_first_name', 'smpi_first_name': smpi_first_name,
                 'qe_first_name': qe_first_name}
        validation_errors.append(error)
    if smpi_last_name != qe_last_name:
        error = {'msg': 'smpi_last_name!=qe_last_name', 'smpi_last_name': smpi_last_name, 'qe_last_name': qe_last_name}
        validation_errors.append(error)
    if smpi_dob != qe_dob:
        error = {'msg': 'smpi_dob!=qe_dob', 'smpi_dob': smpi_dob, 'qe_dob': qe_dob}
        validation_errors.append(error)
    if smpi_gender != qe_gender:
        error = {'msg': 'smpi_gender!=qe_gender', 'smpi_gender': smpi_gender, 'qe_gender': qe_gender}
        validation_errors.append(error)
    return json.dumps(validation_errors)


@udf(returnType=StringType())
def fuzzy_validation(smpi_first_name,
                     smpi_last_name,
                     smpi_dob,
                     smpi_gender,
                     qe_first_name,
                     qe_last_name,
                     qe_dob,
                     qe_gender):
    import json
    import editdistance
    import phonetics
    validation_errors = []
    if smpi_first_name != qe_first_name and phonetics.soundex(smpi_first_name) != phonetics.soundex(qe_first_name):
        error = {'msg': 'phonetics between smpi_first_name and qe_first_name are not the same',
                 'smpi_first_name': smpi_first_name, 'qe_first_name': qe_first_name}
        validation_errors.append(error)
    if editdistance.eval(smpi_last_name, qe_last_name) > 2:
        error = {'msg': 'Distiance between smpi_last_name and qe_last_name > 2', 'smpi_last_name': smpi_last_name,
                 'qe_last_name': qe_last_name}
        validation_errors.append(error)
    if smpi_dob != qe_dob:
        error = {'msg': 'smpi_dob!=qe_dob', 'smpi_dob': smpi_dob, 'qe_dob': qe_dob}
        validation_errors.append(error)
    if smpi_gender != qe_gender:
        error = {'msg': 'smpi_gender!=qe_gender', 'smpi_gender': smpi_gender, 'qe_gender': qe_gender}
        validation_errors.append(error)
    return json.dumps(validation_errors)



@udf(returnType=IntegerType())
def score(smpi_phone,
          smpi_first_name,
          smpi_last_name,
          smpi_street_1,
          smpi_city,
          smpi_state,
          smpi_zipcode,
          smpi_gender,
          smpi_dob,
          smpi_ssn,
          smpi_day_phone,
          smpi_night_phone,
          hixny_first_name,
          hixny_last_name,
          hixny_street_1,
          hixny_city,
          hixny_state,
          hixny_zipcode,
          hixny_gender,
          hixny_dob,
          hixny_ssn) -> int:
    import editdistance
    import phonetics
    score = 0
    # first name	5
    # last name	15
    # street 1	15
    # city	10
    # state	5
    # zip	10
    # phone	10
    # gender	5
    # dob	10
    # ssn	15

    def standardize_zip(zip: str) -> str:
        if zip is not None and len(zip) >= 5:
            return zip.lower()[0:5]
        else:
            return zip

    def standardize_phone(phone: str) -> str:
        if phone is not None:
            return phone.lower() \
                .replace('-', '') \
                .replace('(', '') \
                .replace(')', '') \
                .replace('+1', '') \
                .replace(' ', '')
        else:
            return phone

    def standardize_ssn(ssn: str) -> str:
        if ssn is not None:
            return ssn.lower() \
                .replace('-', '') \
                .replace(' ', '')
        else:
            return ssn

    def standardize_gender(gender: str) -> str:
        if gender is not None:
            return gender.lower() \
                .replace('female', 'F') \
                .replace('male', 'M')
        else:
            return gender

    def standardize_state(state: str) -> str:
        if state is not None:
            return state.lower().replace('new york', 'NY')
        else:
            return state

    def standardize_street(street: str) -> str:
        if street is not None:
            return street.lower() \
                .replace(' ', '') \
                .replace('-', '') \
                .replace(' street', '') \
                .replace(' st', '') \
                .replace(' ave', '') \
                .replace(' avenue', '') \
                .replace(' ln', '') \
                .replace(' lane', '') \
                .replace(' road', '') \
                .replace(' rd', '') \
                .replace(' drive', '') \
                .replace(' dr', '')
        else:
            return street
    def check_sound(str1: str, str2: str) -> str:
        if str1 is not None and str2 is not None and len(str1.strip()) > 1 and len(str2.strip()) > 1:
            same = False
            try:
                same = phonetics.soundex(str1) == phonetics.soundex(str2)
            except Exception as err:
                print(f'------------->>> {err} : str1: {str1} and str2: {str2}')
                return False
            else:
                return same
        else:
            return False

    def to_lower(value: str) -> str:
        if value is not None:
            return value.lower().strip()
        else:
            return value
    # def edit_distance(str1: str, str2: str) -> int:
    #     return editdistance.eval(str1, str2)
    #
    smpi_street_1 = standardize_street(smpi_street_1)
    hixny_street_1 = standardize_street(hixny_street_1)

    smpi_state = standardize_state(smpi_state)
    hixny_state = standardize_state(hixny_state)

    smpi_zipcode = standardize_zip(smpi_zipcode)
    hixny_zipcode = standardize_zip(hixny_zipcode)

    smpi_gender = standardize_gender(smpi_gender)
    hixny_gender = standardize_gender(hixny_gender)

    smpi_ssn = standardize_ssn(smpi_ssn)
    hixny_ssn = standardize_ssn(hixny_ssn)

    smpi_phone = standardize_phone(smpi_phone)
    smpi_day_phone = standardize_phone(smpi_day_phone)
    smpi_night_phone = standardize_phone(smpi_night_phone)

    smpi_first_name = to_lower(smpi_first_name)
    hixny_first_name = to_lower(hixny_first_name)
    smpi_last_name = to_lower(smpi_last_name)
    hixny_last_name = to_lower(hixny_last_name)
    smpi_city = to_lower(smpi_city)

    if (smpi_first_name is not None and hixny_first_name is not None and \
        len(smpi_first_name) > 1 and len(hixny_first_name) > 1) and \
        (smpi_first_name == hixny_first_name or
         check_sound(smpi_first_name, hixny_first_name) or
         editdistance.eval(smpi_first_name, hixny_first_name) <= 2):
        score += 5

    if (smpi_last_name is not None and hixny_last_name is not None) and \
            (smpi_last_name == hixny_last_name or \
                editdistance.eval(smpi_last_name, hixny_last_name) <= 2):
        score += 15

    if (smpi_street_1 is not None and hixny_street_1 is not None) and \
            (smpi_street_1 == hixny_street_1 or \
            editdistance.eval(smpi_street_1, hixny_street_1) <= 3):
        score += 15

    if smpi_city == hixny_city:
        score += 10

    if smpi_state == hixny_state:
        score += 5

    if smpi_zipcode == hixny_zipcode:
        score += 10

    if (smpi_phone == smpi_day_phone) or (smpi_phone == smpi_night_phone):
        score += 10

    if smpi_gender == hixny_gender:
        score += 5

    if smpi_dob == hixny_dob:
        score += 5

    if smpi_ssn == hixny_ssn:
        score += 15

    return score
