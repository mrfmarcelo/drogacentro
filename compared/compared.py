import json  # noqa: F401
import os  # noqa: F401
import pandas as pd  # noqa: F401
from datetime import datetime  # noqa: F401
from tqdm import tqdm  # noqa: F401

# External catalog containing compared prices.
OUTSIDE_CATAOLOG = 'cat.json'
# Internal spreadsheet containing source data.
INTERNAL_SHEET = 'my_cat.xlsx'



excel_frame = pd.read_excel(INTERNAL_SHEET, usecols='A, B, J')
excel_dict = excel_frame.to_dict(excel_frame)

