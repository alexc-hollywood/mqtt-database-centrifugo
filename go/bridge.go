package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/go-pg/pg/v10"
	"github.com/joho/godotenv"
	"github.com/lib/pq"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"github.com/go-redis/redis/v8"
	"github.com/joho/godotenv"
)

type Message struct {
	Topic   string `bson:"topic,omitempty"`
	Message string `bson:"message,omitempty"`
}

type CentrifugoMessage struct {
	Method string `json:"method"`
	Params struct {
		Channel string `json:"channel"`
		Data    string `json:"data"`
	} `json:"params"`
}

func main() {
	// Load .env file
	err := godotenv.Load()

	if err != nil {
		log.Fatal("Error loading .env file")
	}

	clientOptions := options.Client().ApplyURI(os.Getenv("MONGODB_URI"))
	mongoClient, err := mongo.Connect(context.TODO(), clientOptions)

	if err != nil {
		log.Fatal(err)
	}

	collection := mongoClient.Database(os.Getenv("MONGODB_DB")).Collection(os.Getenv("MONGODB_COLLECTION"))

	pgDB, err := sql.Open("postgres", os.Getenv("POSTGRES_URI"))
	if err != nil {
		log.Fatal(err)
	}	

	opts := mqtt.NewClientOptions().AddBroker(os.Getenv("MQTT_BROKER"))
	opts.SetUsername(os.Getenv("MQTT_USERNAME"))
	opts.SetPassword(os.Getenv("MQTT_PASSWORD"))
	opts.SetDefaultPublishHandler(func(client mqtt.Client, msg mqtt.Message) {
		fmt.Printf("Received message: %s from topic: %s\n", msg.Payload(), msg.Topic())

		// Store the data in Redis
		rdb := redis.NewClient(&redis.Options{
			Addr:     os.Getenv("REDIS_HOST") + ":" + os.Getenv("REDIS_PORT"),
			Password: "", // no password set
			DB:       0,  // use default DB
		})
		err := rdb.Set(ctx, os.Getenv("REDIS_TOPIC"), msg.Payload(), 0).Err()
		if err != nil {
			fmt.Println("Redis set error: ", err)
		}

		// Store the data in MongoDB
		message := Message{Topic: msg.Topic(), Message: string(msg.Payload())}
		_, err := collection.InsertOne(context.TODO(), message)
		if err != nil {
			fmt.Println("MongoDB insert error: ", err)
		}

		// Store the data in PostgreSQL
		sqlStatement := `INSERT INTO mqtt_data (topic, message) VALUES ($1, $2)`
		_, err = pgDB.Exec(sqlStatement, msg.Topic(), string(msg.Payload()))
		if err != nil {
			fmt.Println("PostgreSQL insert error: ", err)
		}

		// Post the data to Centrifugo server
		centrifugoMessage := CentrifugoMessage{
			Method: "publish",
		}

		centrifugoMessage.Params.Channel = msg.Topic()
		centrifugoMessage.Params.Data = string(msg.Payload())
		jsonData, err := json.Marshal(centrifugoMessage)
		if err != nil {
			fmt.Println("JSON marshal error: ", err)
		}

		req, err := http.NewRequest("POST", os.Getenv("CENTRIFUGO_API_ENDPOINT"), bytes.NewBuffer(jsonData))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "apikey "+os.Getenv("CENTRIFUGO_API_KEY"))
		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			fmt.Println("HTTP request error: ", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != 200 {
			fmt.Println("Failed to post data to Centrifugo server. Status code: ", resp.StatusCode)
		}
	})

	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Fatal(token.Error())
	}

	token := client.Subscribe(os.Getenv("MQTT_TOPIC"), 0, nil)
	token.Wait()
	fmt.Printf("Subscribed to topic: %s\n", os.Getenv("MQTT_TOPIC"))

	for {
		time.Sleep(1 * time.Second)
	}
}
