import re

from datetime import date, timedelta
from pydantic import BaseModel
from PyPDF2 import PdfReader
from queue import Queue

from .utils.common import clean_line

REGEX_CARD_NUMBER = r'^(\d{4}-\d{2}XX-XXXX-\d{4})\s+([A-Za-z]+(?:\s+[A-Za-z]+)*)$'
REGEX_SETTLEMENT_DATE = r'^TANGGAL REKENING :\s*(\d{2} [A-Z]+ \d{4})$'

REGEX_TRANSACTION_BEGIN_EMPTY = r'^SALDO SEBELUMNYA'
REGEX_TRANSACTION_SINGLE_LINE = r'^(\d{2}-[A-Z]{3}) (\d{2}-[A-Z]{3}) (.*?) (\d{1,3}(?:\.\d{3})*\s?(?:CR)?)$'
REGEX_TRANSACTION_MULTI_LINE_START = r'^(\d{2}-[A-Z]{3}) (\d{2}-[A-Z]{3}) (.*?)$'
REGEX_TRANSACTION_MULTI_LINE_MIDDLE = r'^(.*?)$'
REGEX_TRANSACTION_MULTI_LINE_END = r'^(.*?)(\d{1,3}(?:\.\d{3})*\s?(?:CR)?)$'

REGEX_TRANSACTION_VALIDATION = r'^(\d{2}-[A-Z]{3})'

EMPTY_CARD_PLACEHOLDER = 'XXXX-XXXX-XXXX-XXXX'

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


class PdfParsedRow(BaseModel):
    card_number: str
    owner: str
    transaction_date: date
    posting_date: date
    settlement_date: date
    amount: float
    description: str
    order: int


def parse(pdf: PdfReader) -> list[PdfParsedRow]:
    # Part 1: extract raw data without modifications
    data: list[dict] = []
    validation_transaction_count = 0

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

        line = clean_line(line)

        # Validation: add line to validation sets
        if re.match(REGEX_TRANSACTION_VALIDATION, line):
            validation_transaction_count += 1

        # Get settlement date
        if match_settlement_date := re.match(REGEX_SETTLEMENT_DATE, line):
            day, month, year = match_settlement_date.group(1).split(' ')
            settlement_date = date(int(year), SETTLEMENT_MONTHS_MAP[month], int(day))
            continue

        # Detect a card number line, the beginning of statements
        if match_card_number := re.match(REGEX_CARD_NUMBER, line):
            card_number, owner = match_card_number.groups()
            beginning_of_file = False
            continue
        # There's a case when transactions begin without card number, identified by line 'SALDO SEBELUMNYA'
        elif re.match(REGEX_TRANSACTION_BEGIN_EMPTY, line):
            card_number = EMPTY_CARD_PLACEHOLDER
            owner = EMPTY_CARD_PLACEHOLDER
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
                    'card_number': card_number,
                    'owner': owner,
                    'transaction_date': transaction_date,
                    'posting_date': posting_date,
                    'settlement_date': settlement_date,
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
            transaction_date, posting_date, final_description, amount = match_transaction.groups()

            data.append({
                'card_number': card_number,
                'owner': owner,
                'transaction_date': transaction_date,
                'posting_date': posting_date,
                'settlement_date': settlement_date,
                'description': final_description,
                'amount': amount,
            })
            continue

        # Get a multi-line transaction description
        if match_transaction := re.match(REGEX_TRANSACTION_MULTI_LINE_START, line):
            transaction_date, posting_date, final_description = match_transaction.groups()
            is_multiline = True
            continue

    # Validate the transaction count
    if len(data) != validation_transaction_count:
        raise Exception(f'Validation failed: {len(data)} != {validation_transaction_count}')

    # Part 2: data formatting
    for i, datum in enumerate(data, 1):
        # Amount
        if datum['amount'].endswith('CR'):
            datum['amount'] = float(datum['amount'].removesuffix('CR').replace('.', '').replace(',', '.'))
        else:
            datum['amount'] = -float(datum['amount'].replace('.', '').replace(',', '.'))

        # Transaction date
        # Handle year transition because the transaction date doesn't have year part
        day, month = datum['transaction_date'].split('-')
        month = TRANSACTION_MONTHS_MAP[month]
        year = datum['settlement_date'].year if datum['settlement_date'].month == month else (datum['settlement_date'].replace(day=1) + timedelta(days=-1)).year
        transaction_date = date(year, month, int(day))
        datum['transaction_date'] = transaction_date

        # Posting date
        # Handle year transition because the posting date doesn't have year part
        day, month = datum['posting_date'].split('-')
        month = TRANSACTION_MONTHS_MAP[month]
        year = datum['settlement_date'].year if datum['settlement_date'].month == month else (datum['settlement_date'].replace(day=1) + timedelta(days=-1)).year
        posting_date = date(year, month, int(day))
        datum['posting_date'] = posting_date

        datum['order'] = i

    return [PdfParsedRow(
        card_number=datum['card_number'],
        owner=datum['owner'],
        transaction_date=datum['transaction_date'],
        posting_date=datum['posting_date'],
        settlement_date=datum['settlement_date'],
        amount=datum['amount'],
        description=datum['description'],
        order=datum['order'],
    ) for datum in data]
