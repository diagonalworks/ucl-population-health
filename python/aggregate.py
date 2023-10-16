#!/usr/bin/env python3
#
# Read a CSV file with individual-level age, diagnosis and appointment
# usage rows. Aggregate appointment usage by age decile and diagnosis,
# writing a new CSV file with the mean number of appointments by
# age decile and diagnosis. The contribution of any one individual to the
# mean is protected using differential privacy, computing a laplacian
# bounded mean for each aggregation bucket.

import argparse
import csv
import os.path
import pydp as dp

from pydp.algorithms.laplacian import BoundedMean

# We choose an epsilon value that's generally agreed to be a conservative
# tradeoff of privacy, see:
# https://desfontain.es/privacy/real-world-differential-privacy.html
# Use of this script for purposes beyond demonstraion would require
# determinting a value through a deliberative process, see, for example:
# https://www.usenix.org/system/files/usenixsecurity23-nanayakkara.pdf 
EPSILON = 2.0
APPOINTMENTS_UPPER_BOUND = 40

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--appointments", default="output/population-appointments.csv", help="The appointments to aggregate")
    parser.add_argument("--output", default="output", help="Directory to write output files")
    flags = parser.parse_args()

    appointment_counts = {}
    with open(flags.appointments, "r") as f:
        r = csv.reader(f)
        headers = next(r)
        conditions = [(i, h) for (i, h) in enumerate(headers) if h.startswith("condition_")]        
        age = headers.index("age")
        appointments = headers.index("appointments")
        for row in r:
            key = (int(row[age]) // 10,) + tuple([row[i] == "1" for (i, _) in conditions])
            appointment_counts.setdefault(key, []).append(int(row[appointments]))

    with open(os.path.join(flags.output, "appointments-aggregated.csv"), "w") as f:
        w = csv.writer(f)
        w.writerow(("age",) + tuple([h for (_, h) in conditions]) + ("mean_appointments",))
        for (age, *diagnoses) in sorted(appointment_counts.keys()):
            row = [str(age * 10)]
            row.extend(["1" if d else "0" for d in diagnoses])
            mean = BoundedMean(epsilon=EPSILON, lower_bound=0, upper_bound=APPOINTMENTS_UPPER_BOUND, dtype="float")
            row.append(str(mean.quick_result(appointment_counts[(age, *diagnoses)])))
            w.writerow(row)

if __name__ == "__main__":
    main()


