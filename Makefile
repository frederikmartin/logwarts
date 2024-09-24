GOCMD := go
GOBUILD := $(GOCMD) build
GOCLEAN := $(GOCMD) clean
GOTEST := $(GOCMD) test
GOFMT := $(GOCMD) fmt
BINARY_NAME := logwarts

SRC_DIRS := $(shell find . -type d -not -path "./vendor/*")

.PHONY: all
all: fmt test build ## Format code, run tests, and build the project

# Format the code
.PHONY: fmt
fmt: ## Format the code using go fmt
	@echo "Formatting Go code..."
	@$(GOFMT) ./...

.PHONY: test
test: ## Run all tests with verbose output
	@echo "Running tests..."
	@$(GOTEST) -v ./...

.PHONY: build
build: ## Build the binary executable
	@echo "Building the project..."
	@$(GOBUILD) -o ./build/$(BINARY_NAME) ./cmd/logwarts/main.go

.PHONY: clean
clean: ## Clean build artifacts and remove the binary
	@echo "Cleaning up..."
	@$(GOCLEAN)
	@rm -rf ./build

.PHONY: run
run: build ## Build and run the application
	@echo "Running the application..."
	@./$(BINARY_NAME)

.PHONY: deps
deps: ## Install project dependencies
	@echo "Installing dependencies..."
	@$(GOCMD) mod download

.PHONY: update-deps
update-deps: ## Update project dependencies to their latest versions
	@echo "Updating dependencies..."
	@$(GOCMD) get -u ./...

.PHONY: check
check: fmt test ## Format code and run tests

.PHONY: help
help: ## Display this help message
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'
