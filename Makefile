DCOMPOSE = docker compose -f
spark-worker = 1
file_compose = ./docker/docker-compose.yml # ./docker/composeAlpine.yml to alpine_compose

build:
	${DCOMPOSE} $(file_compose) build

build-nc:
	${DCOMPOSE} $(file_compose) build --no-cache

down:
	 ${DCOMPOSE} $(file_compose) down --volumes

run-scaled: # ./docker/composeAlpine.yml to alpine_compose
	make down && ${DCOMPOSE} $(file_compose) up --scale spark-worker=$(spark-worker)

stop:
	${DCOMPOSE} $(file_compose) stop

submit:
	docker exec spark-master spark-submit --master spark://spark-master:7077 --deploy-mode client ./apps/$(app)

submit-alpine:
	docker exec spark-master-alpine spark-submit --master spark://spark-master:7077 --deploy-mode client ./apps/$(app)

