import pandas as pd
import re

from datetime import datetime, timedelta
from loguru import logger
from PyPDF2 import PdfReader
from queue import Queue

import util

REGEX_TRANSACTION_VALIDATION = r'^\d{1,2} [A-Z][a-z]{2} \d{4}\b$'
REGEX_TRANSACTION_START = r'^\d{1,2} [A-Z][a-z]{2} \d{4}\b$'
REGEX_TRANSACTION_AMOUNT = r'^\d{1,3}(,\d{3})*\.\d{2}$'

LINE_CARD_NUMBER = 'Nomor Kartu'
LINE_OWNER = 'Pemegang Kartu'
LINE_SETTLEMENT_DATE = 'Tanggal Cetak Tagihan'
LINE_CR = 'CR'
LINE_FILE_END = 'Pembayaran Tagihan'

TRANSACTION_MONTHS_MAP = {
    'Jan': 1,
    'Feb': 2,
    'Mar': 3,
    'Apr': 4,
    'Mei': 5,
    'Jun': 6,
    'Jul': 7,
    'Agt': 8,
    'Sep': 9,
    'Okt': 10,
    'Nov': 11,
    'Des': 12,
}

SETTLEMENT_MONTHS_MAP = {
    'Januari': 1,
    'Februari': 2,
    'Maret': 3,
    'April': 4,
    'Mei': 5,
    'Juni': 6,
    'Juli': 7,
    'Agustus': 8,
    'September': 9,
    'Oktober': 10,
    'November': 11,
    'Desember': 12,
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

    if not files:
        raise Exception('No files to process')

    # Part 1: extract raw data without modifications
    all_data = []
    for file in files:
        logger.info(f'Processing {file}')
        data = []
        validation_transaction_count = 0

        lines = util.read_pdf_lines(file, password)
        q = util.EnumeratedQueue(lines)

        # Main data
        settlement_date = None
        card_number = None
        owner = None
        while not q.empty():
            line, i = q.get()
            line = util.clean_line(line)

            if not line:
                continue

            # End of file
            if line == LINE_FILE_END:
                break

            # Validation: add line to validation sets
            if re.match(REGEX_TRANSACTION_VALIDATION, line):
                validation_transaction_count += 1

            # Get settlement date
            if line == LINE_SETTLEMENT_DATE:
                settlement_date = util.clean_line(q.get()[0])
                day, month, year = settlement_date.split(' ')
                settlement_date = datetime(int(year), SETTLEMENT_MONTHS_MAP[month], int(day)).date()
                continue

            # Get card number
            if line == LINE_CARD_NUMBER:
                card_number = util.clean_line(q.get()[0])
                continue

            # Get owner
            if line == LINE_OWNER:
                owner = util.clean_line(q.get()[0])
                continue

            if match_transaction_start := re.match(REGEX_TRANSACTION_START, line):
                transaction_date = match_transaction_start.group(0)

                # Directly get the transaction date
                tranasction_settlement_date = util.clean_line(q.get()[0])

                # Get description
                descriptions = []
                while True:
                    line, i = q.get()
                    line = util.clean_line(line)

                    # Get until an amount is found
                    if match_amount := re.match(REGEX_TRANSACTION_AMOUNT, line):
                        amount = match_amount.group(0)

                        # Detect CR transaction, which also the end of a transaction
                        next_line = util.clean_line(lines[i + 1])
                        if next_line == LINE_CR:
                            amount = f'{amount} CR'

                        break

                    descriptions.append(line)

                data.append({
                    'settlement_date': settlement_date,
                    'card_number': card_number,
                    'owner': owner,
                    'transaction_date': transaction_date,
                    'description': ' '.join(descriptions),
                    'amount': amount,
                    'order': i,
                })

        # Validate the transaction count
        if len(data) != validation_transaction_count:
            raise Exception(f'Validation failed: {len(data)} != {validation_transaction_count}')

        # Validate each rows
        pg_cols_set = set(PG_COLS.keys())
        for datum in data:
            missing_cols = pg_cols_set - set(datum.keys())
            if missing_cols:
                raise Exception(f'Columns {missing_cols} not found: {datum}')
            if datum['settlement_date'] is None:
                raise Exception(f'Settlement date not found: {datum}')
            if datum['card_number'] is None:
                raise Exception(f'Card number not found: {datum}')

        all_data.extend(data)

    # Part 2: data formatting
    for datum in all_data:
        # Amount
        if datum['amount'].endswith(' CR'):
            datum['amount'] = float(datum['amount'].removesuffix(' CR').replace(',', ''))
        else:
            datum['amount'] = -float(datum['amount'].replace(',', ''))

        # Transaction date
        # Handle year transition
        day, month, year = datum['transaction_date'].split(' ')
        transaction_date = datetime(int(year), TRANSACTION_MONTHS_MAP[month], int(day)).date()
        datum['transaction_date'] = transaction_date

    # Part 3: data cleansing
    df = pd.DataFrame(all_data)[PG_COLS.keys()]

    return df, PG_COLS
