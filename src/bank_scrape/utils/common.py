import re


def clean_line(line: str) -> str:
    # Remove all double space occurrences
    # Clean up leading or trailing whitespaces
    # Convert all tab character into whitespace
    return re.sub(r'\s+', ' ', line.strip()).replace('\t', ' ')
