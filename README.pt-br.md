# Spark Lab

Este repositório fornece um ambiente `Spark 3.5.3` para experimentação.

## Objetivo

O objetivo deste repositório é fornecer os arquivos para o desenvolvedor executar experimentos em um cluster `Spark 3.5.3`, em modo client, simulado a partir do Docker. Com isso, o desenvolvedor pode experimentar diversas configurações para a sessão de sua aplicação (`SparkSession`) sem se preocupar com custos de uso de cloud, limitado apenas às condições do seu equipamento.

## O que não esperar?

O objetivo deste repositório **não** é apresentar um projeto de produto de dados. Portanto, não trataremos de integrações, transformações ou enriquecimento de dados a fim de suprir uma necessidade de negócio específica.

## Sistema de referência

Este projeto foi testado em um hardware de prateleira com as seguintes configurações:

- Processador Intel i5 de 8a geração.
- 20GB de RAM
- Sistema Operacional Debian 11.

É recomendado o uso de sistemas operacionais GNU/Linux, porém nada impede do usuário realizar as adaptações deste projeto para o seu sistema operacional de preferência.

## Software necessarios

- [Docker Engine](https://docs.docker.com/engine/install/)
- [Docker Compose](https://docs.docker.com/compose/)
- [Make](https://www.gnu.org/software/make/)

## Como iniciar o cluster pela primeira vez?

Para iniciar o cluster pela primeira vez, execute os seguintes comandos:

```sh
make build
make run
```

Caso o usuário deseje criar um cluster com mais de um nó trabalhador, basta substituir o segundo comando acima `make run-scaled` por `make run-scaled spark-worker=x`, onde `x` corresponde a um número inteiro. Por exemplo, o par de comandos abaixo seria usado para criar o cluster pela primeira vez, com 3 nós trabalhadores.

```sh
make build
make run spark-worker=3
```

> O tempo para construir as imagens docker pela primeira vez depende da velocidade de conexão com a internet. Com uma conexão de 500MB, o tempo médio foi de 3 minutos.

## Como executar uma aplicação Spark?

Para executar uma aplicação Spark utilizando este projeto, é necessário colocar o diretório contendo os arquivos da aplicação no subdiretório `spark-apps`. Em seguida, basta executar o seguinte comando:

```sh
make submit app=caminho/relativo/a/spark-apps/app.py
```

Por exemplo, para executar a aplicação `my-apps/individual_incident.py`, executamos o seguinte comando:

```sh
make submit app=my-apps/individual_incident.py
```

## Como monitorar a execução da minha aplicação?

Para monitorar a execução da sua aplicação, basta acessar o `Spark Master UI` pelo seu navegador preferido a partir do endereço`localhost:9090`.

![Spark Master UI](images/spark-master.png "Spark Master UI")

## Como avaliar as aplicações já executadas?

Para avaliar aplicações cuja execução já fora finalizada, basta acessar o  `Spark History Server UI` pelo seu navegador preferido a partir do endereço `localhost:18080`.

![Spark History Server UI](images/spark-history.png "Spark History Server UI")

## Como incluir novos datasets?

Para utilizar outros datasets que não foram incluidos aqui, basta criar a estrutura de pastas dentro do subdiretório `data` com os arquivos que serão utilizados.

Por exemplo, neste projeto temos o dataset `data/landing/individual_incident_archive_csv`, composto por 5 arquivos CSVs. A estrutura de pasta emula a camada `landing` de um data lake.

## Detalhes sobre a imagem docker
A imagem Docker aqui utilizada está disponível no [DockerHub](https://hub.docker.com/layers/kellermann92/spark-lab-base/python3.13.0-alpine3.20/images/sha256-104aa71f580dadf49410d198f369f1a0f50ea42e1fa89deb6b045a8ce14b777f?context=repo)

> Não é recomendável utilizar a imagem sem antes avaliar se as vulnerabilidades apresentadas pela mesma são consideradas graves ou não pelo time de compilance da sua organização. Recomenda-se utilizar o [trivy](https://trivy.dev/) e o [docker scout](https://docs.docker.com/scout/) para avaliar a vunerabilidade da imagem.

## Datasets utilizados

- [National Incident Based Reporting System](https://dasil.grinnell.edu/DataRepository/NIBRS/Individual_Incident_Archive_CSV.zip):
  - Formato: CSV.
  - Tamanho comprimido: ~2.1 GB
  - Tamanho total: ~24 GB.

## Referências

- [Data Analysis and Social Inquiry Lab](https://dasil.sites.grinnell.edu/downloadable-data/)
- [Setting up a Spark standalone cluster on Docker in layman terms](https://medium.com/@MarinAgli1/setting-up-a-spark-standalone-cluster-on-docker-in-layman-terms-8cbdc9fdd14b)
