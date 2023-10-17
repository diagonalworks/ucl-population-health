#!/usr/bin/env python3
#
# Train a model to predict the number of primary care appointments used by
# an individual in a year, taking into account their age, and diagnosis of
# certain chronic conditions.
#
# We use a simulated population of individuals, including an assignment
# to a real GP practice, as training data. We form batches by taking
# a random sample of the patient population for each GP practice, and
# compute a loss metric for the batch as a whole by comparing the number
# of modelled appointments with:
# - the NHS supplied data on the actual number of appointents per year
#   taking place at the relevant practice.
# - the national appointments per person, per-year, breakdowns from the
#   health survey for England (which uses buckets of 0, 1-2, 3+).
# We alternate the loss metric used (total number of appointments or
# appointment distribution) between training epochs.

import argparse
import csv
import numpy as np
import os.path
import progressbar
import random
import tensorflow as tf

# The diagnoses to take into account for an individual when modelling their
# appointment use.
CONDITIONS = ("dm", "hyp", "copd")
# The number of training examples per a batch. This is chosen to be the
# same order of magnitude as the patient population average GP practice,
# since batches are formed from the patient population of a single practice.
BATCH_SIZE = 4000
EPOCHS = 100
# The length of the noise vector fed to the model
NOISE = 3

# The distribution of GP appointment usage by by age, taken from the
# Health Survey for England 2019: Use of health care services (Table 4)
# Fields are (lower age bound, upper age bound, appointment bucket,
# percentage) where the appointment buckets represent the following number
# of appointments over 12 months: 0: 0, 1: 1-2, 2: 3+.
GP_CONSULTATIONS_BY_AGE = [
    (16, 24, 0, 32.0),
    (16, 24, 1, 37.0),
    (16, 24, 2, 30.0),
    (25, 34, 0, 30.0),
    (25, 34, 1, 36.0),
    (25, 34, 2, 34.0),
    (35, 44, 0, 26.0),
    (35, 44, 1, 40.0),
    (35, 44, 2, 33.0),
    (45, 54, 0, 23.0),
    (45, 54, 1, 40.0),
    (45, 54, 2, 37.0),
    (55, 64, 0, 22.0),
    (55, 64, 1, 36.0),
    (55, 64, 2, 42.0),
    (65, 74, 0, 19.0),
    (65, 74, 1, 34.0),
    (65, 74, 2, 47.0),
    (75, 100, 0, 14.0),
    (75, 100, 1, 35.0),
    (75, 100, 2, 51.0),
]

GP_CONSULTATION_BUCKETS = 3

# As the distribution of the number of appointments per individual
# isn't normal (rather negatively skewed with a long positive tail),
# we model it using a sum of three distinct normal distributions, fed
# from a model output vector of length 3.
APPOINTMENT_MEANS = tf.constant([1.0, 4.0, 16.0])
APPOINTMENT_VARIANCES = tf.constant([1.0, 4.0, 32.0])

def build_model(mean_age, var_age):
    age_input = tf.keras.layers.Input(shape=(1,), name="age_input")
    conditions_input = tf.keras.layers.Input(shape=(len(CONDITIONS),))
    noise_input = tf.keras.layers.Input(shape=(NOISE,), name="noise_input")
    age_normalised = tf.keras.layers.Normalization(axis=1, mean=mean_age, variance=var_age)(age_input)
    concat = tf.keras.layers.Concatenate(axis=1)([age_normalised, conditions_input, noise_input])
    dense1 = tf.keras.layers.Dense(8, activation="relu", name="dense1")(concat)
    dense2 = tf.keras.layers.Dense(8, activation="relu", name="dense2")(dense1)
    dense3 = tf.keras.layers.Dense(8, activation="relu", name="dense3")(dense2)
    dense4 = tf.keras.layers.Dense(8, activation="relu", name="dense4")(dense3)
    output = tf.keras.layers.Dense(3, activation="tanh")(dense4)
    model = tf.keras.Model(inputs=[age_input, conditions_input, noise_input], outputs=[output], name="model")
    model.compile()
    return model

# Run one training step, using a loss function that optimises for the
# total number of appointments allocated to the batch, when compared to
# the real values for the relevant GP practice.
@tf.function
def train_step_magnitude(ages, conditions, expected, buckets, model, optimizer):
    batch_size = ages.shape[0]
    noise = tf.random.normal((batch_size, NOISE))
    with tf.GradientTape() as t:
        output = model([ages, conditions, noise])
        appointments = tf.reduce_sum((output * APPOINTMENT_VARIANCES) + APPOINTMENT_MEANS, axis=-1)
        total_appointments = tf.reduce_sum(appointments, axis=-1)
        magnitude_loss = tf.math.squared_difference(total_appointments, expected) / batch_size
    g = t.gradient(magnitude_loss, model.trainable_variables)
    optimizer.apply_gradients(zip(g, model.trainable_variables))
    return magnitude_loss

# Run one training step, using a loss function that optimises for the
# distribution of appointments across individuals in the batch. We use
# the sigmoid function to estimate the number of appointments in a bucket
# in a differentiable way.
@tf.function
def train_step_distribution(ages, conditions, expected, buckets, model, optimizer):
    batch_size = ages.shape[0]
    noise = tf.random.normal((batch_size, NOISE))
    with tf.GradientTape() as t:
        output = model([ages, conditions, noise])
        appointments = tf.reduce_sum((output * APPOINTMENT_VARIANCES) + APPOINTMENT_MEANS, axis=-1)
        zero_appointments = batch_size - tf.math.reduce_sum(tf.math.sigmoid(appointments - 1.0))
        three_or_more_appointments = tf.math.reduce_sum(tf.math.sigmoid(appointments - 3.0))
        one_or_two_appointments = batch_size - zero_appointments - three_or_more_appointments
        distribution_loss = 0.0
        distribution_loss += tf.math.squared_difference(zero_appointments, buckets[0])
        distribution_loss += tf.math.squared_difference(one_or_two_appointments, buckets[1])
        distribution_loss += tf.math.squared_difference(three_or_more_appointments, buckets[2])
    g = t.gradient(distribution_loss, model.trainable_variables)
    optimizer.apply_gradients(zip(g, model.trainable_variables))
    return distribution_loss

def conditions_from_row(row, condition_columns):
    return [1.0 if row[column] == "1" else -1.0 for column in condition_columns]

def fit(population_csv, gps_csv, output_directory):
    by_gp = {}
    with open(population_csv) as f:
        r = csv.reader(f)
        population_headers = next(r)
        condition_columns = [population_headers.index(f"condition_{c}") for c in CONDITIONS]
        age_column = population_headers.index("age")
        gp_column = population_headers.index("gp")
        ages = []
        for row in r:
            ages.append(int(row[age_column]))
            values = by_gp.setdefault(row[gp_column], [[], []])
            values[0].append(int(row[age_column]))
            values[1].append(conditions_from_row(row, condition_columns))
    mean_age = np.mean(ages)
    var_age = np.var(ages)

    appointments_per_person = []
    zeros = set()
    with open(gps_csv) as f:
        r = csv.DictReader(f)
        for row in r:
            if int(row["list_size"]) == 0:
                zeros.add(row["code"])
            else:
                appointments_per_person.append(float(row["appointments"])/float(row["list_size"]))
    mean_appointments = np.mean(appointments_per_person)
    std_appointments = np.std(appointments_per_person)
    outliers = set()
    with open(gps_csv) as f:
        r = csv.DictReader(f)
        for row in r:
            if row["code"] not in zeros:
                a = float(row["appointments"])/float(row["list_size"])
                if abs(a - mean_appointments) > (4.0 * std_appointments):
                    outliers.add(row["code"])
    print("outliers", outliers)

    appointments = {}
    with open(gps_csv) as f:
        r = csv.DictReader(f)
        for row in r:
            if row["code"] in zeros or row["code"] in outliers:
                continue
            if int(row["list_size"]) > 0:
                a = float(row["appointments"]) * (float(row["simulated_list_size"])/float(row["list_size"]))
                a = (a * 365.0) / 31.0 # Appointments are for March, scale to the year
                appointments_per_batch = (a * BATCH_SIZE) / float(row["simulated_list_size"])
                appointments[row["code"]] = appointments_per_batch

    model = build_model(mean_age, var_age)
    model.summary()
    optimizer = tf.keras.optimizers.Adam(1e-4)
    distribution_loss = tf.keras.metrics.Mean("distribution_loss", dtype=tf.float32)
    magnitude_loss = tf.keras.metrics.Mean("magnitude_loss", dtype=tf.float32)
    log_writer = tf.summary.create_file_writer(os.path.join(output_directory, "appointments-model-logs"))
    bar = progressbar.ProgressBar()
    for epoch in bar(range(0, EPOCHS)):
        for distribution in [False, True]:
            for gp, values in by_gp.items():
                if gp in appointments:
                    ages, conditions = values
                    while len(ages) < BATCH_SIZE:
                        ages.extend(ages)
                        conditions.extend(conditions)
                    buckets = [0.0 for i in range(0, GP_CONSULTATION_BUCKETS)]
                    for age in ages:
                        for bucket in GP_CONSULTATIONS_BY_AGE:
                            if age >= bucket[0] and age <= bucket[1]:
                                buckets[bucket[2]] += bucket[3]
                    buckets = [i / 100.0 for i in buckets]
                    people = random.sample(range(len(ages)), BATCH_SIZE)
                    age_sample = [ages[p] for p in people]
                    conditions_sample  = [conditions[p] for p in people]
                    if distribution:
                        loss = train_step_distribution(tf.convert_to_tensor(age_sample), tf.convert_to_tensor(conditions_sample), tf.convert_to_tensor(float(appointments[gp])), [tf.convert_to_tensor(b) for b in buckets], model, optimizer)
                        distribution_loss(loss)
                    else:
                        loss = train_step_magnitude(tf.convert_to_tensor(age_sample), tf.convert_to_tensor(conditions_sample), tf.convert_to_tensor(float(appointments[gp])), [tf.convert_to_tensor(b) for b in buckets], model, optimizer)
                        magnitude_loss(loss)
        with log_writer.as_default():
            tf.summary.scalar("distribution_loss", distribution_loss.result(), step=epoch)
            tf.summary.scalar("magnitude_loss", magnitude_loss.result(), step=epoch)
    model.save(os.path.join(output_directory, "appointments-model"))

def predict(population_csv, output_directory):
    model = tf.keras.models.load_model(os.path.join(output_directory, "appointments-model"))
    model.compile()
    with open(population_csv) as f:
        r = csv.reader(f)
        population_headers = next(r)
        condition_columns = [population_headers.index(f"condition_{c}") for c in CONDITIONS]
        age_column = population_headers.index("age")
        population = []
        conditions = []
        for row in r:
            population.append(row)
            conditions.append(conditions_from_row(row, condition_columns))

    batch = 10000
    appointments_summary = [0 for i in range(0, 100, 10)]
    with open(os.path.join(output_directory, "population-appointments.csv"), "w") as f:
        w = csv.writer(f)
        w.writerow(population_headers + ["appointments"])
        bar = progressbar.ProgressBar()
        for i in bar(range(0, len(population), batch)):
            population_subset = population[i:i+batch]
            age_tensor = tf.constant([int(row[age_column]) for row in population_subset])
            conditions_subset = conditions[i:i+batch]
            conditions_tensor = tf.constant(conditions_subset)
            noise_tensor = tf.random.normal((len(population_subset), NOISE))
            output = model([age_tensor, conditions_tensor, noise_tensor])
            output = (output * APPOINTMENT_VARIANCES) + APPOINTMENT_MEANS
            totals = tf.reduce_sum(output, axis=-1)
            for (row, total) in zip(population_subset, totals):
                w.writerow(row + [str(int(total))])
                appointments_summary[int(row[age_column])//10] += float(total)
    for age, appointments in enumerate(appointments_summary):
        print(age, appointments)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--fit", action="store_true", help="Fit a model")
    parser.add_argument("--predict", action="store_true", help="Predict using a model")
    parser.add_argument("--aggregate", action="store_true", help="Aggregate appointments by number of conditions")
    parser.add_argument("--population", default="output/population.csv", help="Simulated population")
    parser.add_argument("--gps", default="output/gps.csv", help="GP list sizes")
    parser.add_argument("--output", default="output", help="Directory to write output files")
    flags = parser.parse_args()
    if flags.fit:
        fit(flags.population, flags.gps, flags.output)
    if flags.predict:
        predict(flags.population, flags.output)

if __name__ == "__main__":
    main()