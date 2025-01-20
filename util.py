import re

from PyPDF2 import PdfReader
from queue import Queue
from typing import Any


class EnumeratedQueue(Queue):

    def __init__(self, initial_list: list[str] = [], maxsize: int = 0) -> None:
        super().__init__(maxsize)
        self.ever_get = False
        self.counter = 0

        [self.put(item) for item in initial_list]

    def put(self, item: Any, block: bool = True, timeout: float | None = None) -> None:
        if self.ever_get:
            raise ValueError('Cannot put() item after get() is called')
        return super().put(item, block, timeout)

    def get(self, block: bool = True, timeout: float | None = None) -> Any:
        self.ever_get = True

        counter = self.counter
        self.counter += 1
        return super().get(block, timeout), counter


def read_pdf_lines(file: str, password: str = None) -> list[str]:
    pdf = PdfReader(file)
    if pdf.is_encrypted:
        pdf.decrypt(password)

    return '\n'.join([page.extract_text() for page in pdf.pages]).split('\n')


def clean_line(line: str) -> str:
    # Remove all double space occurrences
    # Clean up leading or trailing whitespaces
    # Convert all tab character into whitespace
    return re.sub(r'\s+', ' ', line.strip()).replace('\t', ' ')
