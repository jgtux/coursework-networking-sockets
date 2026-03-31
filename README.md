# coursework-networking-sockets
Uber's driver actions simulation using sockets. 

## Branches

| Branch | Description |
|---|---|
| `main` | Final version of the project |
| `development` | Active development branch |
| `test` | Testing branch |

## How to run

**Server:**
```bash
go run ./cmd/main.go -mode uber-server -addr 127.0.0.1:9000 -max (number)
```

**Client:**
```bash
go run ./cmd/main.go -mode uber-client -addr 127.0.0.1:9000
```

## Available commands

| Command | Description |
|---|---|
| `:accept` | Accept the latest ride request |
| `:start` | Start the accepted ride |
| `:finish` | Finish the started ride |
| `:cancel` | Cancel the current accepted ride |
| `:status` | Display current driver status |
| `:quit` | Disconnect from the system |

## Available commands in server test terminal

| Command | Description |
|---|---|
| `:clients` | Show which clients are connected |
| `:slots` | Show how many slots are being used and available |
| `:state` | Show the state of each conected session FREE, ON RIDE |
| `:active` | Show rides to be accepted |
| `:help` | Display commands  |
| `:quitadmin` | Close the test terminal |

## GoMaxProcs
"GOMAXPROCS is a Go runtime variable that limits the number of OS threads that can execute user-level Go code simultaneously"
