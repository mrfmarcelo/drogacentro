import json
import os
import pandas as pd
from tqdm import tqdm

# External catalog containing compared prices.
OUTSIDE_CATAOLOG = 'cat.json'
# Internal spreadsheet containing source data.
INTERNAL_SHEET = 'my_cat.xlsx'

