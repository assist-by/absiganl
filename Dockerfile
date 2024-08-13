FROM golang:1.22-alpine

# Set the Current Working Directory inside the container
WORKDIR /app

# Copy go mod and sum files
COPY go.mod go.sum ./

# Download all dependencies
RUN go mod download

# Copy the entire project
COPY . .

# List contents of the current directory (for debugging)
RUN ls -R

# Build the Go app
RUN go build -o main .


# Command to run the executable
CMD ["./main"]