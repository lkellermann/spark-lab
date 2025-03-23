# Spark Lab

This repository provides a small environment for `Spark 3.5.3` experiments.

## Objectives

The objective of this project is to provide files to a developer run experiments in a pseudo `Spark 3.5.3` cluster in client mode. This environment is suitable for running small experiments as the ones provided under the `spark-apps` subdirectory.

## What not to expect?

This repository is not about a data analytics project. It's about analyzing Spark applications. Do not expect to find complex transformations or examples of data enrichment.

## Hardware utilized in this project

All experiments here ran in the following hardware:

- Intel i5 8th generation.
- 20 GB of memory.
- Debian 11.

Unix-like environments are highly recommended.

## Dependencies

- [Docker Engine](https://docs.docker.com/engine/install/)
- [Docker Compose](https://docs.docker.com/compose/)
- [Make](https://www.gnu.org/software/make/)

## How to start?

To start the cluster for the first time, run the following commands:

```sh
make build
make run
```

If an user wants to include more than a worker, they can use the following command

```sh
make build
make run-scaled spark-worker=<number of workers>
```

For example, to create a cluster with 3 worker nodes, run:

```sh
make build
make run spark-worker=3
```

## How to run a Spark  application?

To run a Spark application in this project, save it in the subdirectory `spark-apps`. Then run the following command:
```sh
make submit app=relative/pat/under/spark-apps/app.py
```

For example, to run an application under `my-apps/individual_incident.py`, run:

```sh
make submit app=my-apps/individual_incident.py
```

## How to monitor an application execution?

To monitor the application execution, the developer can access the `Spark Master UI` at `localhost:9090`

![Spark Master UI](images/spark-master.png "Spark Master UI")

## How to access the Spark UI for previously executed applications?

To access the Spark UI for previously executed application, just access the `Spark History Server UI`at `localhost:18080`.

![Spark History Server UI](images/spark-history.png "Spark History Server UI")

## How to add new datasets?

To include new datasets, just put the files in `data` subdirectory in a proper structure. For example, we have the directory `data/landing/individual_incident_archive_csv`, which has 5 aCSV files. This structure emulates the `landing` in a data lake..

## Docker image details
The docker image is available at my personal public [DockerHub](https://hub.docker.com/layers/kellermann92/spark-lab-base/python3.13.0-alpine3.20/images/sha256-104aa71f580dadf49410d198f369f1a0f50ea42e1fa89deb6b045a8ce14b777f?context=repo) repository

## Datasets

- [National Incident Based Reporting System](https://dasil.grinnell.edu/DataRepository/NIBRS/Individual_Incident_Archive_CSV.zip):
  - Formato: CSV.
  - Tamanho comprimido: ~2.1 GB
  - Tamanho total: ~24 GB.

## ~~References~~

- [Data Analysis and Social Inquiry Lab](https://dasil.sites.grinnell.edu/downloadable-data/)
- [Setting up a Spark standalone cluster on Docker in layman terms](https://medium.com/@MarinAgli1/setting-up-a-spark-standalone-cluster-on-docker-in-layman-terms-8cbdc9fdd14b)
