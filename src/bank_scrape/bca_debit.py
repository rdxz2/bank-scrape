import re

from datetime import date
from datetime import datetime
from pydantic import BaseModel
from PyPDF2 import PdfReader
from queue import Queue

from .utils.common import clean_line

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


class PdfParsedRow(BaseModel):
    card_number: str
    transaction_date: date
    settlement_date: date
    amount: float
    description: str
    order: int


def get_description_and_amount_from_descriptions(descriptions: list[str]) -> str:
    """
    Known bugs:
    1. Cannot correctly parse amount when the end of description is numbers.
        Lets say the pdf's actual description is 'SOME DESCRIPTION 999', and the amount is '10,000,000 DB'.
        The description to be parsed would contains value like this: 'SOME DESCRIPTION 99910,000,000 DB'.
        Thus we cannot correctly determine the real amount. Is it 910,000,000? Or 10,000,000?
        It will parse the amount as 0 (zero), which can be easily distinguished since there's not much transaction with 0 amount.
    """

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
    datum = {}
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

    # Part 2: data & cleansing
    for i, datum in enumerate(data):
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

    return [PdfParsedRow(
        card_number=datum['card_number'],
        transaction_date=datum['transaction_date'],
        settlement_date=datum['settlement_date'],
        amount=datum['amount'],
        description=datum['description'],
        order=datum['order'],
    ) for datum in data]
