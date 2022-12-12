

count = 0
with open("/tmp/BronxRHIO_BloodPressure_07.1.2022.09.30.2022_10.11.2022.csv", "r") as input:
    with open("/tmp/blood_pressure.csv", "w") as output:
        for line in input:
            bp = line.split(',')[8]

            if bp != 'Not Performed':
                txt_corrected = line.encode(encoding="ascii", errors="ignore").decode('ascii')
                output.write(txt_corrected)
                if txt_corrected != line:
                    print(txt_corrected)
                    count = count + 1

print(f'count: {count}')