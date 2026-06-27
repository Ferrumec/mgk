# MGK - Messaging Gate Keeper

A pluggable channel framework for notification services that wraps server-to-client messaging protocols. This package handles all cross-cutting concerns such as address verification and user preferences management.

## Overview

MGK (Messaging Gate Keeper) is a Rust-based framework that integrates with event streams to manage notification routing. It provides:

- **Pluggable sender implementations** - Easily swap different notification backends (email, SMS, push notifications, etc.)
- **Preference management** - Store and retrieve user notification preferences with validation
- **Event stream integration** - Subscribe to and process notification events
- **Address verification** - OTP-based address verification for secure notification delivery
- **Caching layer** - Built-in caching for frequently accessed preferences

## Architecture

### Core Components

#### `Module`
The main entry point for the framework. It coordinates:
- Event stream subscriptions
- Sender implementations
- Preference management
- HTTP routing configuration

#### `Sender` Trait
A pluggable interface for notification delivery:
```rust
pub trait Sender: Send + Sync {
    async fn send(&self, address: String, subject: String, message: String);
}
```

#### `Preferences`
Manages user notification preferences with:
- **Database storage** - SQLite-backed persistence
- **In-memory caching** - Moka-based cache for performance
- **OTP verification** - One-time password validation for address confirmation

### Data Models

#### `Preference`
```rust
pub struct Preference {
    pub subject: String,      // Notification topic (max 64 chars)
    pub address: String,      // Delivery address (max 64 chars)
}
```

#### `Token`
```rust
pub struct Token {
    pub token: u32,           // OTP token (6-digit number)
}
```

## Features

### Event Handling
- Subscribes to event streams with subject pattern matching
- Processes events containing user metadata
- Routes notifications based on user preferences
- Graceful error handling and logging

### Preference Management
- **Get preferences**: Retrieve user's notification address for a subject
- **Set preferences**: Generate OTP for address verification
- **Confirm preferences**: Store verified preference after OTP validation
- **Validation**: Automatic field validation using the `validator` crate

### Caching Strategy
- **Preference cache**: 1000-entry LRU cache for user preferences
- **Pending cache**: 100-entry cache for OTP verification tokens
- Reduces database queries for frequently accessed data

## Dependencies

- **actix-web** - HTTP framework
- **async-trait** - Async trait support
- **sqlx** - Database access with SQLite
- **serde/serde_json** - JSON serialization
- **moka** - Async caching
- **tracing** - Structured logging
- **validator** - Input validation
- **rand** - Random number generation for OTP

## Usage

### Basic Setup

```rust
use mgk::Module;
use sqlx::SqlitePool;
use std::sync::Arc;

// Create a new module instance
let pool = SqlitePool::connect("sqlite://notifications.db").await?;
let typed_eventbus = Arc::new(/* your EventStream implementation */);

let module = Module::new(pool, typed_eventbus).await;

// Optionally use a custom sender
let module = module.with_sender(Arc::new(MyCustomSender));
```

### HTTP Configuration

Configure with an Actix-web app:

```rust
use actix_web::web;

let mut config = web::ServiceConfig::default();
module.config(&mut config, "/notifications");

// Routes will be available at /notifications/*
```

### Available Endpoints

The module provides the following HTTP routes (prefix: `/notifications`):

- `POST /set` - Set a preference and receive OTP
- `POST /confirm` - Confirm preference with OTP token
- `GET /get/{user}/{subject}` - Retrieve user preference

## Custom Sender Implementation

Implement the `Sender` trait to integrate with your notification service:

```rust
use async_trait::async_trait;
use mgk::Sender;

struct EmailSender {
    // Your email service configuration
}

#[async_trait]
impl Sender for EmailSender {
    async fn send(&self, address: String, subject: String, message: String) {
        // Send notification via your service
        println!("Email to {}: {}", address, message);
    }
}
```

## Database Schema

The framework uses SQLite with the following schema:

```sql
CREATE TABLE preferences (
    user TEXT NOT NULL,
    subject TEXT NOT NULL,
    address TEXT NOT NULL,
    PRIMARY KEY (user, subject)
);
```

## Event Format

Events are expected to contain metadata in the following format:

```json
{
    "metadata": {
        "user_id": "user_123"
    }
}
```

The framework extracts the `user_id` from event metadata and uses it to look up notification preferences.

## Error Handling

The framework implements comprehensive error handling:
- Invalid OTP tokens
- Missing user preferences
- Database errors
- JSON parsing errors
- Missing user ID in event metadata

All errors are logged with context for debugging.

## Performance Considerations

- **Caching**: Preferences are cached in-memory (1000 entries) to reduce database hits
- **Async/await**: All operations are async for non-blocking I/O
- **Connection pooling**: SQLx manages a pool of database connections
- **OTP caching**: Temporary storage of pending verifications (100 entries)

## Project Structure

```
mgk/
├── src/
│   ├── lib.rs           # Main module and Sender trait
│   └── prefs/           # Preference management module
│       ├── mod.rs       # Module exports
│       ├── db.rs        # Database and cache logic
│       ├── handlers.rs  # Request handlers
│       └── routes.rs    # Route configuration
├── migrations/          # Database migrations
├── Cargo.toml          # Package manifest
└── README.md           # This file
```

## Development

### Prerequisites
- Rust 1.70+
- SQLite

### Building
```bash
cargo build
```

### Testing
```bash
cargo test
```

### Running
```bash
cargo run
```

## Contributing

This is an open-source project. Contributions are welcome! Please fork the repository and submit pull requests.

## License

Not currently licensed. See repository for details.

---

**Repository**: https://github.com/Austin-rgb/mgk  
**Language**: Rust  
**Current Version**: 0.1.0  
**Edition**: 2024
