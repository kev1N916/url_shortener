// main.go
package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"time"

	"github.com/go-redis/redis/v8"
	_ "github.com/go-sql-driver/mysql"
	"github.com/gorilla/mux"
	"github.com/joho/godotenv"
	"golang.org/x/net/context"
)

// Configuration from environment variables
var (
	mysqlDSN = os.Getenv("MYSQL_DSN")
	// redisAddr    = os.Getenv("REDIS_ADDR")     // your Redis endpoint: urlshortenercache1-vvl6fd.serverless.use1.cache.amazonaws.com:6379
	shortURLBase = os.Getenv("SHORT_URL_BASE") // e.g., "https://short.example.com"
	listenAddr   = os.Getenv("LISTEN_ADDR")    // e.g., ":8080"
	codeLength   = 6                           // Length of short code
	cacheExpiry  = 24 * time.Hour              // Cache expiry time
	db           *sql.DB
	redisClient  *redis.Client
	ctx          = context.Background()
)

type URLData struct {
	LongURL  string `json:"long_url"`
	ShortURL string `json:"short_url,omitempty"`
	Code     string `json:"code,omitempty"`
}

type ErrorResponse struct {
	Error string `json:"error"`
}

func initDB() error {
	var err error

	// Step 1: Connect without specifying DB to create it
	db, err = sql.Open("mysql","kevinAdminSqlurl:C5kF8P8DczDzsytvKKov@tcp(urlshortenerdb1.c2lkooss6kgg.us-east-1.rds.amazonaws.com:3306)/")
	if err != nil {
		return err
	}
	defer db.Close()

	if err = db.Ping(); err != nil {
		return err
	}

	// Step 2: Create the database if it doesn't exist
	_, err = db.Exec("CREATE DATABASE IF NOT EXISTS urlshortenerdb1")
	if err != nil {
		return err
	}

	// Step 3: Reconnect using the created database
	db, err = sql.Open("mysql","kevinAdminSqlurl:C5kF8P8DczDzsytvKKov@tcp(urlshortenerdb1.c2lkooss6kgg.us-east-1.rds.amazonaws.com:3306)/urlshortenerdb1")
	if err != nil {
		return err
	}

	// Step 4: Test connection again
	if err = db.Ping(); err != nil {
		return err
	}

	// Step 5: Create table if it doesn't exist
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS urls (
			id BIGINT AUTO_INCREMENT PRIMARY KEY,
			code VARCHAR(10) NOT NULL UNIQUE,
			long_url TEXT NOT NULL,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			hits INT DEFAULT 0
		) ENGINE=InnoDB;
	`)
	return err
}

// Initialize Redis connection
func initRedis() error {
	redisClient = redis.NewClient(&redis.Options{
		Addr:        "redis-cache-url-shortener1.vvl6fd.clustercfg.use1.cache.amazonaws.com:6379",
		Password:    "", // If you have AUTH enabled
		DB:          0,
		DialTimeout: 10 * time.Second,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Test connection
	_, err := redisClient.Ping(ctx).Result()
	return err
}

// Generate a random short code
func generateCode() string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	code := make([]byte, codeLength)
	for i := range code {
		code[i] = charset[rand.Intn(len(charset))]
	}
	return string(code)
}

// Create short URL handler
func createShortURLHandler(w http.ResponseWriter, r *http.Request) {
	// Only accept POST requests
	if r.Method != http.MethodPost {
		respondWithError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	// Parse request body
	var urlData URLData
	if err := json.NewDecoder(r.Body).Decode(&urlData); err != nil {
		respondWithError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	// Validate input
	if urlData.LongURL == "" {
		respondWithError(w, http.StatusBadRequest, "URL is required")
		return
	}

	// Generate a unique code
	var code string
	for {
		code = generateCode()

		// Check if code exists in DB
		var exists bool
		err := db.QueryRow("SELECT 1 FROM urls WHERE code = ?", code).Scan(&exists)
		if err == sql.ErrNoRows {
			// Code is available
			break
		}

		if err != nil {
			log.Printf("Database error: %v", err)
			respondWithError(w, http.StatusInternalServerError, "Error generating short URL")
			return
		}

		// If we get here, code exists, try again
	}

	// Store in database
	_, err := db.Exec("INSERT INTO urls (code, long_url) VALUES (?, ?)", code, urlData.LongURL)
	if err != nil {
		log.Printf("Failed to insert URL: %v", err)
		respondWithError(w, http.StatusInternalServerError, "Database error")
		return
	}

	// Store in cache
	err = redisClient.Set(ctx, code, urlData.LongURL, cacheExpiry).Err()
	if err != nil {
		log.Printf("Cache error: %v", err)
		// Continue even if cache fails
	}

	// Create response
	shortURL := fmt.Sprintf("%s/%s", shortURLBase, code)
	response := URLData{
		LongURL:  urlData.LongURL,
		ShortURL: shortURL,
		Code:     code,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(response)
}

// Redirect handler
func redirectHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	code := vars["code"]

	// Try to get from cache first
	longURL, err := redisClient.Get(ctx, code).Result()
	if err == redis.Nil {
		// Not in cache, try database
		err = db.QueryRow("SELECT long_url FROM urls WHERE code = ?", code).Scan(&longURL)
		if err == sql.ErrNoRows {
			http.NotFound(w, r)
			return
		}
		if err != nil {
			log.Printf("Database error: %v", err)
			respondWithError(w, http.StatusInternalServerError, "Error retrieving URL")
			return
		}

		// Store in cache for future requests
		redisClient.Set(ctx, code, longURL, cacheExpiry)

		// Update hit counter asynchronously
		go func() {
			_, err := db.Exec("UPDATE urls SET hits = hits + 1 WHERE code = ?", code)
			if err != nil {
				log.Printf("Failed to update hit counter: %v", err)
			}
		}()
	} else if err != nil {
		log.Printf("Cache error: %v", err)
		// If cache fails, try database
		err = db.QueryRow("SELECT long_url FROM urls WHERE code = ?", code).Scan(&longURL)
		if err == sql.ErrNoRows {
			http.NotFound(w, r)
			return
		}
		if err != nil {
			log.Printf("Database error: %v", err)
			respondWithError(w, http.StatusInternalServerError, "Error retrieving URL")
			return
		}

		// Update hit counter asynchronously
		go func() {
			_, err := db.Exec("UPDATE urls SET hits = hits + 1 WHERE code = ?", code)
			if err != nil {
				log.Printf("Failed to update hit counter: %v", err)
			}
		}()
	} else {
		// Found in cache, update hit counter asynchronously
		go func() {
			_, err := db.Exec("UPDATE urls SET hits = hits + 1 WHERE code = ?", code)
			if err != nil {
				log.Printf("Failed to update hit counter: %v", err)
			}
		}()
	}

	// Redirect to the original URL
	http.Redirect(w, r, longURL, http.StatusTemporaryRedirect)
}

// Error response helper
func respondWithError(w http.ResponseWriter, code int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(ErrorResponse{Error: message})
}

// Health check endpoint
func healthCheckHandler(w http.ResponseWriter, r *http.Request) {
	// Check DB connection
	if err := db.Ping(); err != nil {
		log.Printf("Database health check failed: %v", err)
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprintln(w, "Database connection failed")
		return
	}

	// Check Redis connection
	if _, err := redisClient.Ping(ctx).Result(); err != nil {
		log.Printf("Redis health check failed: %v", err)
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprintln(w, "Redis connection failed")
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintln(w, "Service is healthy")
}

func main() {
	// Set random seed
	rand.Seed(time.Now().UnixNano())

	// Load .env file
	if err := godotenv.Load(); err != nil {
		log.Printf("Warning: could not load .env file: %v", err)
	}

	// Initialize database
	if err := initDB(); err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}
	defer db.Close()
	log.Println("connected to mysql")

	// Initialize Redis
	if err := initRedis(); err != nil {
		log.Fatalf("Failed to initialize Redis: %v", err)
	}
	defer redisClient.Close()

	// Create router
	r := mux.NewRouter()

	// API endpoints
	r.HandleFunc("/api/shorten", createShortURLHandler).Methods("POST")
	r.HandleFunc("/health", healthCheckHandler).Methods("GET")

	// Redirect endpoint
	r.HandleFunc("/{code}", redirectHandler).Methods("GET")

	// Start server
	addr := listenAddr
	if addr == "" {
		addr = ":8080"
	}

	log.Printf("Server starting on %s", addr)
	if err := http.ListenAndServe(addr, r); err != nil {
		log.Fatalf("Server failed to start: %v", err)
	}
}
