main:
	reset
	sudo docker exec -it spark-master bin/spark-submit --master spark://spark-master:7077 --total-executor-cores 2 --executor-memory 512m /tmp/data/main.py

test:
	sudo docker exec -it spark-master bin/spark-submit --master spark://spark-master:7077 --total-executor-cores 2 --executor-memory 512m /tmp/data/main.py > results.output

hello:
	sudo docker exec -it spark-master bin/spark-submit --master spark://spark-master:7077 --total-executor-cores 2 --executor-memory 512m /tmp/data/hello.py

setup_server:
	sudo docker-compose up
