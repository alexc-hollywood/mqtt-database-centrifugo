# MQTT - Database - Centrifugo Bridge

This script subscribes to an authentication-enabled MQTT server, stores the messages in Redis / MongoDB / Postgres, then broadcasts them into Centrifugo.

## Python

```
mv ./example.env ./.env
pip install paho-mqtt redis requests psycopg2-binary pymongo python-dotenv
```

Then 

```
python3 bridge.py
```

## Go

Dependencies:

```
mv ./example.env ./.env
go get github.com/go-redis/redis/v8
go get github.com/eclipse/paho.mqtt.golang
go get github.com/joho/godotenv
go get github.com/lib/pq
go get go.mongodb.org/mongo-driver/mongo
go get github.com/lib/pq
```

Then:

```
go run bridge.go
```