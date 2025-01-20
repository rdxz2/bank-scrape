import pandas as pd
import re

from datetime import datetime
from loguru import logger
from PyPDF2 import PdfReader
from queue import Queue

import util

REGEX_CARD_NUMBER = r'NO\. REKENING :\s*([0-9]+)$'
REGEX_SETTLEMENT_DATE = r'^PERIODE :\s*([A-Z]+ \d{4})$'

REGEX_TRANSACTION_START = r'^(\d{2}/\d{2}) (.*)'
REGEX_TRANSACTION_AMOUNT = r'(?<!\d)\d{1,3}(?:\,\d{3})*\.\d{1,2}(?: DB)?'
REGEX_TRANSACTION_BALANCE = r'(?<!\d)\d{1,3}(,\d{3})*(\.\d{2})$'

REGEX_TRANSACTION_VALIDATION = r'(\d{2}/\d{2})'

LINESTART_TRANSACTION_END = (
    'Bersambung ke Halaman berikut',  # End of page
    'Bersambung ke halaman berikut',  # End of page
    'SALDO AWAL :',  # End of document
)

SETTLEMENT_MONTHS_MAP = {
    'JANUARI': 1,
    'FEBRUARI': 2,
    'MARET': 3,
    'APRIL': 4,
    'MEI': 5,
    'JUNI': 6,
    'JULI': 7,
    'AGUSTUS': 8,
    'SEPTEMBER': 9,
    'OKTOBER': 10,
    'NOVEMBER': 11,
    'DESEMBER': 12,
}

PG_TABLE_NAME = 'public.stmt_bca_debit'
PG_COLS = {
    'settlement_date': 'DATE',
    'card_number': 'VARCHAR(19)',
    'transaction_date': 'DATE',
    'description': 'TEXT',
    'amount': 'DECIMAL',
    'order': 'SMALLINT',
}


def get_description_and_amount_from_descriptions(descriptions: list[str]) -> str:
    description = ' '.join(descriptions)

    if match_amount := re.search(REGEX_TRANSACTION_AMOUNT, description):
        amount = match_amount.group(0)

        # The amount is always at the first or last line of descriptions
        # Remove the amount
        descriptions[0] = descriptions[0].replace(amount, '')
        descriptions[-1] = descriptions[-1].replace(amount, '')
        # Remove the ending balance if exists
        descriptions[0] = re.sub(REGEX_TRANSACTION_BALANCE, '', descriptions[0])
        descriptions[-1] = re.sub(REGEX_TRANSACTION_BALANCE, '', descriptions[-1])

        # # The amount and ending transaction balance can be anywhere in the descriptions
        # for i in range(len(descriptions)):
        #     descriptions[i] = descriptions[i].replace(amount, '')
        #     descriptions[i] = re.sub(REGEX_TRANSACTION_BALANCE, '', descriptions[i])
    else:
        raise Exception(f'Amount not detected in descriptions: {descriptions}')

    final_description = ' '.join(descriptions)
    # Remove the remaining double whitespaces
    final_description = re.sub(r'\s+', ' ', final_description.strip())

    return final_description, amount


def parse(files: list[str], password: str):
    # files = glob.glob(f'{FOLDER}/*.pdf')
    # files = ['/home/ubuntu/Downloads/bca/debit/5500002095Sep2024.pdf']

    # Part 1: extract raw data without modifications
    all_data = []

    for file in files:
        logger.info(f'Processing {file}')
        pdf = PdfReader(file)
        data = []
        validation_transaction_count = 0

        if pdf.is_encrypted:
            pdf.decrypt(password)

        q = Queue()
        lines = '\n'.join([page.extract_text() for page in pdf.pages])
        [q.put(line) for line in lines.split('\n')]

        # Main data
        settlement_date = None
        card_number = None
        # Aux
        beginning_of_file = True
        datum = {}
        while not q.empty():
            line = q.get()
            if not line:
                continue

            line = util.clean_line(line)

            # Validation: add line to validation sets
            if re.match(REGEX_TRANSACTION_VALIDATION, line):
                validation_transaction_count += 1

            # Get settlement date
            if match_settlement_date := re.match(REGEX_SETTLEMENT_DATE, line):
                month, year = match_settlement_date.group(1).split(' ')
                settlement_date = datetime(int(year), SETTLEMENT_MONTHS_MAP[month], 1).date()
                continue

            # Detect a card number line, the beginning of statements
            if match_card_number := re.search(REGEX_CARD_NUMBER, line):
                card_number, = match_card_number.groups()
                beginning_of_file = False
                continue
            # For optimization, skip the line if it's the beginning of the file because it's not corelated to any transaction
            elif beginning_of_file:
                continue

            if line.startswith(LINESTART_TRANSACTION_END):
                # Pop data if exists
                if datum:
                    datum['description'], datum['amount'] = get_description_and_amount_from_descriptions(datum['description'])
                    data.append(datum)
                    datum = {}

            # Get a beginning of transaction
            if match_transaction := re.match(REGEX_TRANSACTION_START, line):
                # Pop data if exists
                if datum:
                    datum['description'], datum['amount'] = get_description_and_amount_from_descriptions(datum['description'])
                    data.append(datum)
                    datum = {}

                # Start of a new transaction
                transaction_date, rest = match_transaction.groups()

                datum['settlement_date'] = settlement_date
                datum['transaction_date'] = transaction_date
                datum['card_number'] = card_number
                datum['description'] = [rest]

            # Else this is a multi-line transaction
            elif 'description' in datum:
                if line == '':
                    if datum:
                        datum['description'], datum['amount'] = get_description_and_amount_from_descriptions(datum['description'])
                        data.append(datum)
                        datum = {}
                        continue

                datum['description'].append(line)

        # Validate the transaction count
        if len(data) != validation_transaction_count:
            raise Exception(f'Validation failed: {len(data)} != {validation_transaction_count}')

        all_data.extend(data)

    # Part 2: data formatting
    for i, datum in enumerate(all_data):
        # Amount
        if datum['amount'].endswith(' DB'):
            datum['amount'] = -float(datum['amount'].removesuffix(' DB').replace(',', ''))
        else:
            datum['amount'] = float(datum['amount'].replace(',', ''))

        # Transaction date
        day, month = datum['transaction_date'].split('/')
        year = datum['settlement_date'].year
        transaction_date = datetime(year, int(month), int(day))
        datum['transaction_date'] = transaction_date

        datum['order'] = i

    # Part 3: data cleansing
    df = pd.DataFrame(all_data)[PG_COLS.keys()]

    # Exclude SALDO AWAL
    df = df[df['description'] != 'SALDO AWAL']

    return df, PG_COLS

    # # Part 4: save to database
    # pg = PG('postgres-local-postgres')
    # cols_str = ', '.join([f'{col} {data_type}' for col, data_type in PG_COLS.items()])
    # pg.execute_query(
    # f'''
    # DROP TABLE IF EXISTS {PG_TABLE_NAME};
    # CREATE TABLE {PG_TABLE_NAME} ({cols_str});
    # ''', return_df=False
    # )

    # pg.upload_df(df, PG_TABLE_NAME)
    # pg.close()
