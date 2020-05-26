import multiprocessing

# According to the docs given in data.gov
datagovbaseurl = "http://catalog.data.gov/api/3/"

# REST result iterator
maxrows = 1000 

# Accepted formats
ok_formats = ["CSV", "csv", "Csv"]

# Worker pool configs
num_pool_workers = multiprocessing.cpu_count() * 2
