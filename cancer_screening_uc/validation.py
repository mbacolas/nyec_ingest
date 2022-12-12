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
        error = {'msg': 'smpi_first_name != qe_first_name', 'smpi_first_name': smpi_first_name, 'qe_first_name': qe_first_name}
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
        error = {'msg': 'phonetics between smpi_first_name and qe_first_name are not the same', 'smpi_first_name': smpi_first_name, 'qe_first_name': qe_first_name}
        validation_errors.append(error)
    if editdistance.eval(smpi_last_name, qe_last_name) > 2:
        error = {'msg': 'Distiance between smpi_last_name and qe_last_name > 2', 'smpi_last_name': smpi_last_name, 'qe_last_name': qe_last_name}
        validation_errors.append(error)
    if smpi_dob != qe_dob:
        error = {'msg': 'smpi_dob!=qe_dob', 'smpi_dob': smpi_dob, 'qe_dob': qe_dob}
        validation_errors.append(error)
    if smpi_gender != qe_gender:
        error = {'msg': 'smpi_gender!=qe_gender', 'smpi_gender': smpi_gender, 'qe_gender': qe_gender}
        validation_errors.append(error)
    return json.dumps(validation_errors)
    #
    # withColumn('fuzzy_error', fuzzy_validation(col('MPIID'.upper()),
    #                                                                        col('smpi_first_name'.upper()),
    #                                                                        col('smpi_last_name'.upper()),
    #                                                                        col('smpi_street_1'.upper()),
    #                                                                        col('smpi_day_phone'.upper()),
    #                                                                        col('smpi_night_phone'.upper()),
    #                                                                        col('smpi_gender'.upper()),
    #                                                                        col('smpi_dob'.upper()),
    #                                                                        col('smpi_city'.upper()),
    #                                                                        col('smpi_state'.upper()),
    #                                                                        col('smpi_zipcode'.upper()),
    #                                                                        col('smpi_ssn'.upper()),
    #
    #                                                                        col('MPI_ID'.upper()),
    #                                                                        col('smpi_day_phone'.upper()),
    #                                                                        col('smpi_night_phone'.upper()),
    #                                                                        col('hixny_street_1'.upper()),
    #                                                                        col('hixny_first_name'.upper()),
    #                                                                        col('hixny_last_name'.upper()),
    #                                                                        col('smpi_last_name'.upper()),
    #                                                                        col('hixny_city'.upper()),
    #                                                                        col('hixny_state'.upper()),
    #                                                                        col('hixny_zipcode'.upper()),
    #                                                                        col('hixny_gender'.upper()),
    #                                                                        col('hixny_dob'.upper()),
    #                                                                        col('hixny_ssn'.upper())

@udf(returnType=DataType('IntegerType'))
def fuzzy_validation(smpi,
                 smpi_first_name,
                 smpi_last_name,
                 smpi_street_1,
                 smpi_phone,
                 smpi_gender,
                 smpi_dob,
                 smpi_city,
                 smpi_state,
                 smpi_zipcode,
                 smpi_ssn,


                 hixny_mpi,
                 smpi_day_phone,
                 smpi_night_phone,
                 hixny_street_1,
                 hixny_first_name,
                 hixny_last_name,
                 hixny_city,
                 hixny_state,
                 hixny_zipcode,
                 hixny_gender,
                 hixny_dob,
                 hixny_ssn):
    import json
    import editdistance
    import phonetics
    validation_errors = []
    if smpi_first_name != qe_first_name and phonetics.soundex(smpi_first_name) != phonetics.soundex(qe_first_name):
        error = {'msg': 'phonetics between smpi_first_name and qe_first_name are not the same', 'smpi_first_name': smpi_first_name, 'qe_first_name': qe_first_name}
        validation_errors.append(error)
    if editdistance.eval(smpi_last_name, qe_last_name) > 2:
        error = {'msg': 'Distiance between smpi_last_name and qe_last_name > 2', 'smpi_last_name': smpi_last_name, 'qe_last_name': qe_last_name}
        validation_errors.append(error)
    if smpi_dob != qe_dob:
        error = {'msg': 'smpi_dob!=qe_dob', 'smpi_dob': smpi_dob, 'qe_dob': qe_dob}
        validation_errors.append(error)
    if smpi_gender != qe_gender:
        error = {'msg': 'smpi_gender!=qe_gender', 'smpi_gender': smpi_gender, 'qe_gender': qe_gender}
        validation_errors.append(error)
    return json.dumps(validation_errors)