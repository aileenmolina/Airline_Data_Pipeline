src_array = [
    {"src": "bookings"},
    {"src": "airports"},
    {"src": "customers"},
    {"src": "flights"}


dbutils.jobs.taskValues.set(key="output_key", value=src_array)