# Population health modelling

Exploratory population health modelling, by [Diagonal](https://diagonal.works), on behalf of [UCL Partners](https://uclpartners.com/). The repository contains:
- A [tool to generate a synthetic population](src/diagonal.works/ucl-population-health/cmd/population/population.go) for the North Central London ICB, derived from [census data](https://www.ons.gov.uk/census), [GP location data](https://digital.nhs.uk/services/organisation-data-service/export-data-files/csv-downloads/gp-and-gp-practice-related-data), [GP QOF data](https://qof.digital.nhs.uk/), [condition prevalence data](data/prevalences.yaml) and the [Health Survey for England](https://digital.nhs.uk/data-and-information/publications/statistical/health-survey-for-england) responses. The population has age, sex, LSOA level home location and GP practice attributes, together with diagnoses of diabetes, hypertension and COPD.
- A [tool to estimate the primary care appointment load of an individual](python/appointments.py), via a simple neural network trained on aggregate GP practice level appointment data.
- A [tool to aggregate primary care appointment load](python/appointments.py), using differentially private means.

Datasets used by the tools are [cached in this repository](data/README.md). All licenses permit derived analysis; for futher uses, consult the specific licenses, linked from a comment at the head of each file.

## Population synthesis

The population synthesis tool is a golang binary which:
- Uses LSOA level census data to synthesise a individuals with the relevant age and sex distribition.
- Allocates individuals to a nearby GP practice, using [Diagonal's b6](https://diagonal.works/b6).
- Assigns diagnoses to those individuals, using [national prevalance data](data/prevalences.yaml), biased such that practive-level prevalences match those those reported in the [GP QOF data](https://qof.digital.nhs.uk/).

### Using a prebuilt docker image

You can generate a synthetic population using our prebuilt docker image with:

```
docker run -v ${PWD}:/output europe-west1-docker.pkg.dev/diagonal-public/ucl-population-health/population bin/population --population --output=/output
```

A number of files will be written to the current directory:
- `population.csv` contains the synthetic individuals and their attributes.
- `gps.csv` contains the GP practices, together with aggregate statistics for the synthetic individuals assigned to them.
- `population.json` contains aggregate statistics of the synthetic individuals in a format suitable for web based visualisation.

### Building from source

You can build the population binary locally with:

```
make
```

You'll need [Go 1.20](https://go.dev/dl/) or above, and [GDAL 3.7](https://gdal.org/download.html). The build process will download prebuilt [b6](https://diagonal.works/b6) world data for UK postcodes and LSOAs, and will generate `bin/population`.

### Building within docker

You can build the population binary within docker with:
```
docker build .
```

## Appointment prediction

You can train a model that attempts to predict the primary care appointment load of an indvidual with:

```
pip3 install -r python/requirements.txt
python3 python/appointments.py --fit --population=population.csv --gps=gps.csv --output=.
```

Where `population.csv` and `gps.csv` are the synthetic population from the tool above, and `--output` specifies the directory in which to write the trained model. The model itself is [described in the source](python/appointments.py), using GP practice level appointment aggregates as training data.
The model can then be used assign appointments for individuals with:

```
python3 python/appointments.py --predict --population=population.csv --output=.
```

Where `--output` specifies both the location to read the trained model from, and the directory in which to write `population-appointments.csv`, a copy of the input `population.csv` file, with an extra column specifying the predicted number of primary care appointments for that individual over a year.

## Appointment aggregation

You can calculate the average number of appointments by age and diagnosis with:

```
python3 python/aggregate.py --appointments=population-appointments.csv --output=.
```

Where `--output` specifies the directory in which to write `appointments-aggregated.csv`.
We use a differentially private bounded mean to protect the impact of any one individual on each metric, potentially making the tool suitable for use with both synthetic and real data, subject to the relavant organisational policies.
