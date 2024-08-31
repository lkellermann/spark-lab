DCOMPOSE = docker compose -f ./docker/docker-compose.yml
spark-worker = 1

build:
	${DCOMPOSE} build

build-nc:
	${DCOMPOSE} build --no-cache

down:
	 ${DCOMPOSE} down --volumes

run-scaled:
	make down && ${DCOMPOSE} up --scale spark-worker=$(spark-worker)

stop:
	${DCOMPOSE} stop

submit:
	docker exec spark-master spark-submit --master spark://spark-master:7077 --deploy-mode client ./apps/$(app)

