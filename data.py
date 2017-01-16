import csv
import numpy as np
import pandas as pd
from pprint import pprint as pp


def pivot_immunization():
    data = list()

    with open('data/bcg_immunization.csv') as f:
        f.next()
        keys = list()
        reader = csv.reader(f)

        i = 0
        for row in reader:
            if i == 0:
                keys = row
            else:
                country = row[0]
                for j in range(1, len(keys)):
                    year = keys[j]
                    elem = dict()
                    elem['Country'] = country.strip()
                    elem['Year'] = year.strip()

                    # if value cannot be parsed, exclude from data set
                    try:
                        elem['BCG immunization coverage among 1-year-olds (%)'] = np.float(row[j])/100
                    except ValueError:
                        continue

                    data.append(elem)

            i += 1

    return pd.DataFrame(data)


def load(filename):
    with open(filename) as f:
        reader = csv.DictReader(f)
        data = list()
        for row in reader:
            for col in row.keys():
                if col != 'Year' and col != 'Country':
                    value = row[col]
                    try:
                        if '[' in col:
                            value = value.split('[')[0].replace(' ', '')

                        value = np.float(value)
                        if '(%)' in col:
                            value /= 100

                        # maybe convert these values?  need to adjust column labels

                        # if '(per 100 000 population)' in col:
                        #     value *= 100000
                        # if '(in thousands)' in col:
                        #     value *= 1000

                    except ValueError:
                        value = np.nan
                    row[col] = value
                else:
                    row[col] = row[col].strip()

            data.append(row)

        return pd.DataFrame(data)


def load_countries():
    with open('data/countries.csv') as f:
        reader = csv.DictReader(f)
        data = list()
        for row in reader:
            data.append(row)

    return pd.DataFrame(data)


def main():

    immunization = pivot_immunization()
    resistance = load('data/drug_resistence.csv')
    df = immunization.merge(resistance, how='outer', on=['Country', 'Year'])

    incidence = load('data/incidence.csv')
    df = df.merge(incidence, how='outer', on=['Country', 'Year'])

    mortality = load('data/mortality.csv')
    df = df.merge(mortality, how='outer', on=['Country', 'Year'])

    case_notifications = load('data/new_case_notifications.csv')
    df = df.merge(case_notifications, how='outer', on=['Country', 'Year'])

    previously_treated = load('data/previously_treated.csv')
    df = df.merge(previously_treated, how='outer', on=['Country', 'Year'])

    tb_hiv_coepidemics = load('data/tb_hiv_coepidemics.csv')
    df = df.merge(tb_hiv_coepidemics, how='outer', on=['Country', 'Year'])

    treatment = load('data/treatment.csv')
    df = df.merge(treatment, how='outer', on=['Country', 'Year'])

    treatment_coverage = load('data/treatment_coverage.csv')
    df = df.merge(treatment_coverage, how='outer', on=['Country', 'Year'])

    countries = load_countries()
    df = df.merge(countries, how='outer', left_on='Country', right_on='DisplayString')

    population = load('data/population.csv')
    df = df.merge(population, how='outer', on=['Country', 'Year'])

    df.to_csv('data/merge_out.csv')

if __name__ == '__main__':
    main()
