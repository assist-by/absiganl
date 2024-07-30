FROM golang:1.22-alpine

# Set the Current Working Directory inside the container
WORKDIR /app

# Install git and protobuf
RUN apk add --no-cache git protobuf

# Install protoc-gen-go and protoc-gen-go-grpc
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.28
RUN go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.2

# Copy go mod and sum files
COPY go.mod go.sum ./

# Download all dependencies
RUN go mod download

# Copy the entire project
COPY . .

# List contents of the current directory (for debugging)
RUN ls -R

# Generate Go code from protobuf
RUN protoc --go_out=. --go_opt=paths=source_relative \
           --go-grpc_out=. --go-grpc_opt=paths=source_relative \
           $(find . -name "*.proto")

# Build the Go app
RUN go build -o main .

# Expose port 8080 to the outside world
EXPOSE 8080

# Command to run the executable
CMD ["./main"]