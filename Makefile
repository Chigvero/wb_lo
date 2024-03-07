all: run

server:
	nats-server

run:
	go run main.go