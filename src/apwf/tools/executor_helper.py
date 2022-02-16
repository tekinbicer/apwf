import csv
import datetime

def append_activated_iids(data, csv_path):
    print("adding row to activated: {}".format(data))
    with open(csv_path, 'a', newline='') as f:
        writer = csv.writer(f)
        for row in data:
            writer.writerow([row, datetime.datetime.now()])
            
def read_activated_iids(csv_path):
    vals = []
    with open(csv_path, newline='') as f:
        reader = csv.reader(f)
        for row in reader:
            vals.append(row)
    return vals