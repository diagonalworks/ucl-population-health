all: population cached/nearby-gps.csv

population: world
	mkdir -p output
	cd src/diagonal.works/ucl-population-health/cmd/population; go build -o ../../../../../bin/population

world: world/lsoa-2011.index world/codepoint-open-2023-02.index

cached/nearby-gps.csv:
	mkdir -p cached
	bin/population --nearby-gps

world/%.index:
	mkdir -p world
	curl --output $@ http://static.diagonal.works/ucl-population-health/$*.index
