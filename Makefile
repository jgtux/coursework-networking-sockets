APP=./cmd/main.go
ADDR=127.0.0.1:9000
MAX=5

.PHONY: uber-server uber-client server client clean

uber-server:
	go run $(APP) -mode uber-server -addr $(ADDR) -max $(MAX)

uber-client:
	go run $(APP) -mode uber-client -addr $(ADDR)

server:
	go run $(APP) -mode server -addr $(ADDR) -max $(MAX)

client:
	go run $(APP) -mode client -addr $(ADDR)

