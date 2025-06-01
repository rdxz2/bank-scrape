# Using this library

```py
from PyPDF2 import PdfReader

from bank_scrape.parse_bca_credit import parse

pdf = PdfReader(__FILE_PATH__)

# Decrypt the pdf if required
pdf.decrypt(__PASSWORD__)

rows = parse(pdf)
for row in rows:
    ...
```
