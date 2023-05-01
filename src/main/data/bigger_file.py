import csv
import os

print(os.listdir())
read_file = open('src/main/data/ticket_flights_copy.csv', 'r')
write_file = open('src/main/data/ticket_flights_copy.csv', 'a', newline='')

csv_reader = csv.reader(read_file)
csv_writer = csv.writer(write_file)

rows = []

for row in csv_reader:
    rows.append(row)
    
csv_writer.writerows(rows)
read_file.close()
write_file.close()