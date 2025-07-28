Install all the libs using: pip install -r requirements.txt

Start docker container using: docker-compose up -d 
Check the docker containers status using: docker ps

Steps to clean all the kafka queue:
1- Connect into the docker container using: docker exec -it <container-id> bash
2- List the topics: kafka-topics --bootstrap-server <host:port> --list
3- Delete kafka topics: kafka-topics --bootstrap-server localhost:9092 --delete --topic <topic-name>
