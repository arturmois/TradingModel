from datetime import datetime
from TwitterSearch import *
import json

api = {
    'consumer_key': 'gORLvj5lfeC2UkOsXIJ5dTg4R',
    'consumer_secret': 'CdQNwCzZXmPpmx0j0Etyy5Uh4PO1kAQR0XAi2eOALBBxIrACOr',
    'access_token': '3423223480-JfVbA7J1NttvD95fMMEO8xlIYeoWRDUZHEjbEO5',
    'access_token_secret': 'mhFKauBYIaIxUtISXYOtjjFoMkofexc98ajmeSSMdc9EK'}

try:
    ts = TwitterSearch(
        consumer_key=api['consumer_key'],
        consumer_secret=api['consumer_secret'],
        access_token=api['access_token'],
        access_token_secret=api['access_token_secret']
    )

    tso = TwitterSearchOrder()
    tso.set_keywords(['bitcoin', 'cryptocurrency'], or_operator=True)
    tso.set_language('en')

except:
    pass
