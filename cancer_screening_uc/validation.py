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


@udf(returnType=DataType('IntegerType'))
def score(smpi,
          smpi_phone,
          smpi_first_name,
          smpi_last_name,
          smpi_street_1,
          smpi_city,
          smpi_state,
          smpi_zipcode,
          smpi_gender,
          smpi_dob,
          smpi_ssn,
          hixny_mpi,
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
          hixny_ssn):
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
    # 	100

    def standardize_zip(zip: str) -> str:
        return zip.lower()[0:5]

    def standardize_phone(phone: str) -> str:
        return phone.lower() \
            .replace('-', '') \
            .replace('(', '') \
            .replace(')', '') \
            .replace('+1', '') \
            .replace(' ', '')

    def standardize_ssn(ssn: str) -> str:
        return ssn.lower() \
            .replace('-', '') \
            .replace(' ', '')

    def standardize_gender(gender: str) -> str:
        return gender.lower() \
            .replace('female', 'F') \
            .replace('male', 'M')

    def standardize_state(state: str) -> str:
        return state.lower().replace('new york', 'NY')

    def standardize_street(street: str) -> str:
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

    smpi_street_1 = standardize_street(smpi_street_1)
    hixny_street_1 = standardize_street(hixny_street_1)

    smpi_state = standardize_street(smpi_state)
    hixny_state = standardize_street(hixny_state)

    smpi_zipcode = standardize_street(smpi_zipcode)
    hixny_zipcode = standardize_street(hixny_zipcode)

    smpi_gender = standardize_street(smpi_gender)
    hixny_gender = standardize_street(hixny_gender)

    smpi_ssn = standardize_street(smpi_ssn)
    hixny_ssn = standardize_street(hixny_ssn)

    smpi_phone = standardize_street(smpi_phone)
    smpi_day_phone = standardize_street(smpi_day_phone)
    smpi_night_phone = standardize_street(smpi_night_phone)

    if smpi_first_name == hixny_first_name or \
            phonetics.soundex(smpi_first_name) == phonetics.soundex(hixny_first_name) or \
            editdistance.eval(smpi_first_name, hixny_first_name) <= 2:
        score += 5

    if smpi_last_name == hixny_last_name or \
            editdistance.eval(smpi_last_name, hixny_last_name) <= 2:
        score += 15

    if smpi_street_1 == hixny_street_1 or \
            editdistance.eval(smpi_street_1, hixny_street_1) <= 3:
        score += 15

    if smpi_state == hixny_state:
        score += 5

    if smpi_zipcode == hixny_zipcode:
        score += 10

    if smpi_gender == hixny_gender:
        score += 5

    if smpi_ssn == hixny_ssn:
        score += 15

    if smpi_dob == hixny_dob:
        score += 5

    if (smpi_phone == smpi_day_phone) or (smpi_phone == smpi_night_phone):
        score += 15

    return score
