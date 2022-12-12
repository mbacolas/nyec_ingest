import glob

criteria_files = []
for file in glob.glob("/Users/emmanuel.bacolas/projects/nyec_ingest/cancer_screening_uc/criteria/*.csv"):
    criteria_files.append(file)

from csv import DictReader
sql = []
csv = ['test_name,code,code_system,is_numerator,is_denominator_inclusion,is_denominator_exclusion']
for file in criteria_files:
    with open(file, 'r', encoding='utf-8-sig') as f:
        dict_reader = DictReader(f)
        list_of_dict = list(dict_reader)
        # print(list_of_dict)
        # list_of_dict=[[(str.strip(k), str.strip(v)) for k, v in d.items()] for d in list_of_dict]
        details_stripped = [{key.strip(): value.strip() for key, value in d.items()} for d in list_of_dict]

        is_numerator = False
        is_denominator_exclusion = False
        is_denominator_inclusion = False
        if 'Denom' not in file:
            is_numerator = True
        elif 'Denom Exclusion':
            is_denominator_exclusion = True
        elif 'Denom Inclusion':
            is_denominator_inclusion = False

        for row in details_stripped:
            csv_row = f'{file.split("/")[-1].split(".")[0]},{row["Procedure Code"]},{row["Code Standard"]},{is_numerator},{is_denominator_exclusion},{is_denominator_inclusion}'
            insert_stmt = f'insert into colorectal_cancer_screening ' \
                          f'(test_name, code, ' \
                          f'code_system, ' \
                          f'is_numerator, ' \
                          f'is_denominator_inclusion, ' \
                          f'is_denominator_exclusion) ' \
                          f'values (\'{file.split("/")[-1].split(".")[0]}\', ' \
                          f'\'{row["Procedure Code"]}\', ' \
                          f'\'{row["Code Standard"]}\', ' \
                          f'{is_numerator}, ' \
                          f'{is_denominator_exclusion}, ' \
                          f'{is_denominator_inclusion});'
            sql.append(insert_stmt)
            csv.append(csv_row)

# print(csv)

for r in sql:
    print(r)

# import csv
# with open('/Users/emmanuel.bacolas/Desktop/criteria/test.csv', 'w') as f:
#     write = csv.writer(f)
#     # write = csv.writer(f, delimiter = '\n')
#     write.writerows(csv)
    # write.writerow(csv)

