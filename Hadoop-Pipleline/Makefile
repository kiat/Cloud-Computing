TASK_1_OUTPUT := OUTPUT1
TASK_2_TMP := TMP2
TASK_2_OUTPUT := OUTPUT2
TASK_3_TMP := TMP3 
TASK_3_OUTPUT := OUTPUT3

# Entrypoints.

GPS_CLASS := -Dexec.mainClass="edu.cs.utexas.Taxi.GPSError"
TAXI_ERROR_CLASS := -Dexec.mainClass="edu.cs.utexas.Taxi.TaxiError"
TAXI_DRIVER_CLASS := -Dexec.mainClass="edu.cs.utexas.Taxi.Driver"


# Dataset.
DATASET := taxi-data-sorted-small.csv

target:
	mvn clean install

# Task 1
.PHONY: gps-error
gps-error: target
	mvn -q exec:java $(GPS_CLASS) -Dexec.args="$(DATASET) $(TASK_1_OUTPUT)"

# Task 2
.PHONY: taxi-error
taxi-error: target
	mvn -q exec:java $(TAXI_ERROR_CLASS) -Dexec.args="$(DATASET) $(TASK_2_TMP) $(TASK_2_OUTPUT)"

# Task 3
.PHONY: driver
driver: target
	mvn -q exec:java $(TAXI_DRIVER_CLASS) -Dexec.args="$(DATASET) $(TASK_3_TMP) $(TASK_3_OUTPUT)"

.PHONY: clean-output
clean-output:
	rm -rf OUTPUT/
	rm -rf OUTPUT1/
	rm -rf TMP2/
	rm -rf OUTPUT2/
	rm -rf TMP3/
	rm -rf OUTPUT3/
	rm -f log.txt

.PHONY: clean
clean: clean-output
	rm -rf target/

