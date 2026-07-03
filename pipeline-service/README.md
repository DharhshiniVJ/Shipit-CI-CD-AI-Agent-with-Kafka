# ShipIt Pipeline Service (Learning Demo)

This is a simple, standard-library-only Go microservice that simulates a CI/CD pipeline trigger. It demonstrates idiomatic Go concepts like goroutines, mutexes for concurrent map access, and standard HTTP routing.

## How to Run

1. Open a terminal and navigate to this directory.
2. Run the server using the Go CLI:

```bash
go run main.go
```

You should see: `🚀 Starting pipeline-service on http://localhost:8080...`

## Testing the API

Open a second terminal window to run these `curl` commands.

### 1. Trigger a Pipeline

This endpoint accepts a JSON payload and returns the created pipeline ID immediately while the "build" runs in the background.

**macOS / Linux / Bash:**
```bash
curl -X POST http://localhost:8080/trigger \
  -H "Content-Type: application/json" \
  -d '{"repo": "github.com/myorg/myapp", "commit": "a1b2c3d4"}'
```

**Windows PowerShell:**
```powershell
Invoke-RestMethod -Uri http://localhost:8080/trigger -Method POST -Headers @{"Content-Type"="application/json"} -Body '{"repo": "github.com/myorg/myapp", "commit": "a1b2c3d4"}'
```

**Expected Output:**
```json
{"id":"...","repo":"github.com/myorg/myapp","commit":"a1b2c3d4","status":"pending","created_at":"..."}
```

Check your first terminal window! You should see a log line saying the pipeline was triggered, and 4 seconds later, a second log line indicating if it succeeded or failed.

### 2. Check Pipeline Status

Copy the `id` from the output of the previous command and use it to check the status.

**macOS / Linux / Bash:**
```bash
curl -X GET http://localhost:8080/status/<YOUR-PIPELINE-ID>
```

**Windows PowerShell:**
```powershell
Invoke-RestMethod -Uri http://localhost:8080/status/<YOUR-PIPELINE-ID> -Method GET
```

**Expected Output (before 4 seconds):**
```json
{"id":"...","repo":"github.com/myorg/myapp","commit":"a1b2c3d4","status":"pending","created_at":"..."}
```

**Expected Output (after 4 seconds):**
```json
{"id":"...","repo":"github.com/myorg/myapp","commit":"a1b2c3d4","status":"success","created_at":"..."}
```
*(Status will randomly be "success" or "failed")*

## Go Concepts Learned Here
- **Standard HTTP Handlers**: Using `net/http` for routing and handling requests.
- **Goroutines**: Using the `go` keyword to fire off background tasks (the build simulation) without blocking the HTTP response.
- **Concurrency Safety**: Using `sync.RWMutex` to safely write and read from an in-memory map across multiple concurrent HTTP requests.
- **JSON Encoding/Decoding**: Using `json.NewDecoder` for efficient streaming and struct tags (`json:"..."`) for field mapping.
