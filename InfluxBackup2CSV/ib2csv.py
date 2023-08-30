#!/usr/bin/env python3

# This script can be used to parse InfluxDB backup files and export them to CSV files.
# Usage: python3 ib2csv.py <backup_file> <csv output file>

import os
import sys

if len(sys.argv) != 3:
    print("Usage: python3 ib2csv.py <backup_file> <csv output file>")
    sys.exit(1)

input_file = sys.argv[1]
output_file = sys.argv[2]

if os.path.exists(output_file):
    print("Output file already exists. Exiting.")
    sys.exit(1)

f = None
if input_file.endswith(".gz"):
    import gzip

    f = gzip.open(input_file, "rt")
else:
    f = open(input_file, "r")

measurements = []
for line in f.readlines():
    line = line.strip()
    parts = line.split(" ")
    if len(parts) != 3:
        continue
    tag = parts[0]
    measurement = parts[1]
    timestamp = parts[2]

    key = measurement.split("=")[0]
    value = measurement.split("=")[1]

    measurements.append((timestamp, key, value))

f.close()

print(f"processing {len(measurements)} measurements...")
measurements.sort(key=lambda x: x[0])

# write output
print(f"writing output to {output_file}...")
with open(output_file, "w") as f:
    for measurement in measurements:
        f.write(f"{measurement[0]};{measurement[1]};{measurement[2]}\n")
