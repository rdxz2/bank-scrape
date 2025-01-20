import pandas as pd
import re

from datetime import datetime, timedelta
from loguru import logger
from PyPDF2 import PdfReader
from queue import Queue

import util

REGEX_CARD_NUMBER = r'^(\d{4}-\d{2}XX-XXXX-\d{4})\s+([A-Za-z]+(?:\s+[A-Za-z]+)*)$'
REGEX_SETTLEMENT_DATE = r'^TANGGAL REKENING :\s*(\d{2} [A-Z]+ \d{4})$'

REGEX_TRANSACTION_SINGLE_LINE = r'^(\d{2}-[A-Z]{3}) (\d{2}-[A-Z]{3}) (.*?) (\d{1,3}(?:\.\d{3})*\s?(?:CR)?)$'
REGEX_TRANSACTION_MULTI_LINE_START = r'^(\d{2}-[A-Z]{3}) (\d{2}-[A-Z]{3}) (.*?)$'
REGEX_TRANSACTION_MULTI_LINE_MIDDLE = r'^(.*?)$'
REGEX_TRANSACTION_MULTI_LINE_END = r'^(.*?)(\d{1,3}(?:\.\d{3})*\s?(?:CR)?)$'

REGEX_TRANSACTION_VALIDATION = r'^(\d{2}-[A-Z]{3})'

TRANSACTION_MONTHS_MAP = {
    'JAN': 1,
    'FEB': 2,
    'MAR': 3,
    'APR': 4,
    'MEI': 5,
    'JUN': 6,
    'JUL': 7,
    'AGS': 8,
    'AGU': 8,
    'SEP': 9,
    'OKT': 10,
    'NOV': 11,
    'DES': 12,
}

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

PG_TABLE_NAME = 'public.stmt_bca_credit'
PG_COLS = {
    'settlement_date': 'DATE',
    'card_number': 'VARCHAR(19)',
    'owner': 'VARCHAR(255)',
    'transaction_date': 'DATE',
    'description': 'TEXT',
    'amount': 'DECIMAL',
    'order': 'SMALLINT',
}


def parse(files: list[str], password: str):

    # files = glob.glob(f'{FOLDER}/*.pdf')
    # files = ['/home/ubuntu/Downloads/bca/credit_decrypted/20241025-17665783_25102024_1729907816344_1729963828426.pdf']

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
        is_multiline = False
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
                day, month, year = match_settlement_date.group(1).split(' ')
                settlement_date = datetime(int(year), SETTLEMENT_MONTHS_MAP[month], int(day)).date()
                continue

            # Detect a card number line, the beginning of statements
            if match_card_number := re.match(REGEX_CARD_NUMBER, line):
                card_number, owner = match_card_number.groups()
                beginning_of_file = False
                continue
            # For optimization, skip the line if it's the beginning of the file because it's not corelated to any transaction
            elif beginning_of_file:
                continue

            # Validation: card number must be found before any transaction
            if card_number is None:
                raise Exception(f'Card number not found, current line: {line}')

            # Validation: settlement date must be found before any transaction
            if settlement_date is None:
                raise Exception(f'Settlement date not found, current line: {line}')

            # Evaluate a continuation of a multi-line transaction
            if is_multiline:
                if match_transaction := re.match(REGEX_TRANSACTION_MULTI_LINE_END, line):
                    description, amount = match_transaction.groups()
                    final_description = f'{final_description} {description}'
                    data.append({
                        'settlement_date': settlement_date,
                        'card_number': card_number,
                        'owner': owner,
                        'transaction_date': transaction_date,
                        'description': final_description,
                        'amount': amount,
                    })
                    is_multiline = False
                    continue
                elif match_transaction := re.match(REGEX_TRANSACTION_MULTI_LINE_MIDDLE, line):
                    final_description = f'{final_description} {match_transaction.group(0)}'
                    continue

            # Get a single transaction line
            if match_transaction := re.match(REGEX_TRANSACTION_SINGLE_LINE, line):
                transaction_date, _, final_description, amount = match_transaction.groups()

                data.append({
                    'settlement_date': settlement_date,
                    'card_number': card_number,
                    'owner': owner,
                    'transaction_date': transaction_date,
                    'description': final_description,
                    'amount': amount,
                })
                continue

            # Get a multi-line transaction description
            if match_transaction := re.match(REGEX_TRANSACTION_MULTI_LINE_START, line):
                transaction_date, _, final_description = match_transaction.groups()
                is_multiline = True
                continue

        # Validate the transaction count
        if len(data) != validation_transaction_count:
            raise Exception(f'Validation failed: {len(data)} != {validation_transaction_count}')

        all_data.extend(data)

    # Part 2: data formatting
    for i, datum in enumerate(all_data):
        # Amount
        if datum['amount'].endswith('CR'):
            datum['amount'] = float(datum['amount'].removesuffix('CR').replace('.', '').replace(',', '.'))
        else:
            datum['amount'] = -float(datum['amount'].replace('.', '').replace(',', '.'))

        # Transaction date
        # Handle year transition
        day, month = datum['transaction_date'].split('-')
        month = TRANSACTION_MONTHS_MAP[month]
        year = datum['settlement_date'].year if datum['settlement_date'].month == month else (datum['settlement_date'].replace(day=1) + timedelta(days=-1)).year
        transaction_date = datetime(year, month, int(day))
        datum['transaction_date'] = transaction_date

        datum['order'] = i

    # Part 3: data cleansing
    df = pd.DataFrame(all_data)[PG_COLS.keys()]

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
