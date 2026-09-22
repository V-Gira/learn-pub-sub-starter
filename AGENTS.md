# AGENTS.md

This document provides guidance for AI agents working with the **learn-pub-sub-starter** codebase, a Go-based Peril game that demonstrates RabbitMQ Pub/Sub patterns.

---

## Project Overview

This is a Go project implementing the Peril game (a turn-based strategy game where players move armies across continents). It uses **RabbitMQ** as the message broker for pub/sub communication between the server and client instances.

---

## Essential Commands

### Build and Run

```bash
# Build the server binary
go build -o server ./cmd/server

# Build the client binary
go build -o client ./cmd/client

# Run server directly (requires RabbitMQ running on localhost:5672)
go run ./cmd/server

# Run client directly (requires RabbitMQ running on localhost:5672)
go run ./cmd/client

# Start multiple server instances in parallel
./multiserver.sh <number-of-instances>

# Start/stop RabbitMQ container using podman
./rabbit.sh start
./rabbit.sh stop
./rabbit.sh logs
```

### Testing

```bash
# Run all tests
go test ./...

# Run tests with verbose output
go test -v ./...

# Run tests with race detector
go test -race ./...
```

---

## Code Organization

```
learn-pub-sub-starter/
├── cmd/
│   ├── client/    # Client-side application
│   └── server/    # Server-side application
├── internal/
│   ├── gamelogic/   # Game logic (state management, move handling)
│   ├── pubsub/      # RabbitMQ pub/sub operations
│   └── routing/     # Message routing constants and types
├── go.mod          # Go module definition
└── README.md
```

---

## Architecture

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Peril Game System                       │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  ┌────────────────┐         RabbitMQ         ┌──────────────┐│
│  │   Server       │◄───────┤───────►         │    Client    ││
│  │   Instance     │  │   │   │   │          │    Instance  ││
│  │  - Game logic  │  │   │   │   │          │  - Game logic││
│  │  - Input loop  │  │   │   │   │          │  - Input loop││
│  │  - Publishes   │  │   │   │   │          │  - Subscribes││
│  └────────────────┘  │   │   │   │          └──────────────┘│
│                      │   │   │                  │           │
│                      │   │   │  (Pub/Sub)       │           │
│                      │   │   │                  │           │
└──────────────────────┼───┼───┼──────────────────┼───────────┘
                       │   │   │                  │
                       ▼   ▼   ▼                  ▼
                Exchange   Exchange      Exchange   Exchange
                (Topic)   (Direct)       (Topic)   (Direct)
```

### Control Flow

1. **Client Startup**:
   - Connects to RabbitMQ (`amqp://guest:guest@localhost:5672/`)
   - Authenticates via username prompt
   - Subscribes to:
     - `peril_direct.pause.<username>` for pause state updates
     - `peril_topic.army_moves.<username>` for move messages
   - Enters input loop

2. **Server Startup**:
   - Connects to RabbitMQ (`amqp://guest:guest@127.0.0.1:5672/`)
   - Declares and binds to game log queue
   - Enters input loop

3. **Communication Patterns**:
   - **Pause/Resume**: Server publishes to `peril_direct.pause` exchange (direct exchange)
   - **Moves**: Client publishes to `peril_topic` exchange (topic exchange)
   - **Game Logs**: Server publishes to `game_logs` queue (durable queue)

---

## Data Models

### Core Types (`internal/gamelogic/gamedata.go`)

```go
type Player struct {
    Username string
    Units    map[int]Unit
}

type Unit struct {
    ID       int
    Rank     UnitRank // "infantry", "cavalry", or "artillery"
    Location Location // "americas", "europe", etc.
}

type ArmyMove struct {
    Player     Player
    Units      []Unit
    ToLocation Location
}

type RecognitionOfWar struct {
    Attacker Player
    Defender Player
}
```

### Routing Constants (`internal/routing/routing.go`)

```go
const (
    // Exchanges
    ExchangePerilDirect = "peril_direct"   // Direct exchange for pause/resume
    ExchangePerilTopic  = "peril_topic"    // Topic exchange for army moves

    // Keys
    PauseKey      = "pause"
    GameLogSlug   = "game_logs"
    ArmyMovesPrefix = "army_moves"
    WarRecognitionsPrefix = "war"
)
```

### Game State (`internal/gamelogic/gamestate.go`)

The `GameState` struct maintains per-player state with thread-safe access via `sync.RWMutex`:

- **Player**: Username and units map
- **Paused**: Boolean pause state
- **Mutex**: Protects concurrent access

---

## Key Functions

### Client (`cmd/client/main.go`)

- `handlerPause(gs, ps)`: Pause state handler, returns `Ack` or `NackDiscard`
- `handlerMove(gs, move)`: Move handler with outcome-based ack/nack:
  - `MoveOutcomeSamePlayer` → `NackDiscard` (ignore own moves)
  - `MoveOutComeSafe` → `Ack` (valid move)
  - `MoveOutcomeMakeWar` → `Ack` (valid move that makes war)
- `SubscribeJSON()`: Subscribes to pub/sub channels with JSON unmarshaling
- `CommandMove()`: Parses move command, validates, updates units, publishes
- `CommandSpawn()`: Spawns units at location (signature exists but implementation incomplete)
- `CommandStatus()`: Displays player's current units and location

### Server (`cmd/server/main.go`)

- `GetInput()`: Reads stdin with `> ` prompt
- Input loop handles: `pause`, `resume`, `quit`
- **Gotcha**: Uses hardcoded RabbitMQ connection string `amqp://guest:guest@127.0.0.1:5672/`

### Pub/Sub (`internal/pubsub/pubsub.go`)

- `PublishJSON()`: Publishes JSON to RabbitMQ channel
- `SubscribeJSON()`: Subscribes with goroutine for async message handling
- `DeclareAndBind()`: Creates and binds queue to exchange

### Game Logic (`internal/gamelogic/*.go`)

- `HandleMove()`: Validates move, checks for wars, returns `MoveOutcome`
- `getOverlappingLocation()`: Finds overlapping unit locations
- `getAllLocations()`: Returns valid locations map
- `isPaused()`: Thread-safe pause check using `sync.RWMutex`

---

## Important Gotchas

### RabbitMQ Connection Strings

- **Server**: `amqp://guest:guest@127.0.0.1:5672/`
- **Client**: `amqp://guest:guest@localhost:5672/`

These are **hardcoded** in `cmd/server/main.go` and `cmd/client/main.go`.

### Message Ack/Nack Semantics

The `Acktype` enum controls message delivery behavior:

```go
const (
    Ack    Acktype = iota  // Acknowledge, deliver to next consumer
    NackRequeue            // Reject and requeue (retry)
    NackDiscard            // Reject and discard (drop)
)
```

In `handlerMove`:
- Your own moves → `NackDiscard` (ignore)
- Safe moves → `Ack` (deliver to next)
- War moves → `Ack` (deliver to next)

### Thread Safety

All game state accesses must use `gs.mu.RLock()` for reads and `gs.mu.Lock()` for writes. The `GameState` struct contains a `sync.RWMutex` that must be held for all operations on `gs.Player`.

### Invalid Locations

The `getAllLocations()` function returns exactly these locations:
- `americas`, `europe`, `africa`, `asia`, `australia`, `antarctica`

Attempting to use any other location will return an error.

### Unit ID Validation

Unit IDs must be valid positive integers. The `CommandMove` function validates this with `strconv.Atoi()`.

### Durable vs Transient Queues

- `SimpleQueueDurable` (durable, auto-delete on close): Used for `game_logs`
- `SimpleQueueTransient` (not durable, auto-delete on close): Used for `pause` and `army_moves`

---

## Testing Patterns

### Test File Structure

Go test files are in the same package as the code being tested:
- `internal/gamelogic/gamedata_test.go` tests `gamedata.go`
- `internal/gamelogic/gamelogic_test.go` tests `gamelogic.go`
- `internal/pubsub/pubsub_test.go` tests `pubsub.go`

### Test Examples

```go
// Basic unit test
func TestGetAllLocations(t *testing.T) {
    locations := getAllLocations()
    assert.Len(t, locations, 6)
}

// Integration test with pubsub
func TestSubscribeJSON(t *testing.T) {
    conn := amqp.NewConnection(nil)
    defer conn.Close()
    // Setup test...
}

// Test game logic
func TestHandleMove(t *testing.T) {
    gs := NewGameState("test_player")
    move := ArmyMove{...}
    outcome := gs.HandleMove(move)
    assert.Equal(t, MoveOutcomeSamePlayer, outcome)
}
```

---

## Common Issues

### RabbitMQ Not Running

Before running the game, ensure RabbitMQ is running:

```bash
# Start RabbitMQ container
./rabbit.sh start

# Verify with podman
podman ps | grep rabbitmq
```

### Connection Errors

Check RabbitMQ logs:
```bash
./rabbit.sh logs
```

### Queue Declaration Errors

Ensure:
1. RabbitMQ is running on `localhost:5672` or `127.0.0.1:5672`
2. Connection string uses correct credentials (`guest:guest`)
3. Exchange and queue names match routing constants

### Move Validation

When testing moves:
1. Check game is not paused (`gs.isPaused()`)
2. Verify all unit IDs exist
3. Verify destination location is valid
4. Check for overlapping locations (war detection)

---

## Extending the Game

### Adding New Game Features

1. **New Game State**: Extend `GameState` in `gamestate.go`
2. **New Types**: Add to `gamedata.go` models
3. **New Routes**: Add to `routing/routing.go` constants
4. **New Handlers**: Implement in client/server main.go
5. **Tests**: Add corresponding tests

### Adding New Unit Types

1. Add to `getAllRanks()` in `gamedata.go`
2. Update any validation logic that checks unit ranks
3. Add test cases for the new rank

### Adding New Locations

1. Add to `getAllLocations()` in `gamedata.go`
2. Update any location-specific logic

---

## RabbitMQ Topics

### Exchange Types Used

- `peril_direct`: Direct exchange (exact routing key match required)
- `peril_topic`: Topic exchange (supports wildcards)

### Topic Patterns

- `pause`: Matches `pause` exactly
- `army_moves.<username>`: Matches `army_moves.<username>` exactly
- `army_moves.*`: Matches any `army_moves.<any>` (wildcard)

### Queue Bindings

- `game_logs`: Bound to `peril_topic` with key `game_logs`
- `pause.<username>`: Bound to `peril_direct` with key `pause.<username>`
- `army_moves.<username>`: Bound to `peril_topic` with key `army_moves.<username>`

---

## Docker Setup

### Container Names

- RabbitMQ: `peril_rabbitmq`
- Port: `5672` (RabbitMQ API), `15672` (Management UI)

### Image

- `rabbitmq:3.13-management` (latest stable with management plugin)

---

## License and Attribution

This starter code is used in Boot.dev's [Learn Pub/Sub](https://learn.boot.dev/learn-pub-sub) course.

---

## Additional Resources

- [RabbitMQ Go Client](https://github.com/rabbitmq/amqp091-go)
- [Go Standard Library](https://pkg.go.dev/)
- [Boot.dev Learn Pub/Sub](https://learn.boot.dev/learn-pub-sub)
