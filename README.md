# MGK - Messaging Gate Keeper

A pluggable channel framework for notification services that wraps server-to-client messaging protocols. This package handles all cross-cutting concerns such as address verification and user preferences.

## Overview

MGK (Messaging Gate Keeper) is a Rust-based framework that integrates with event streams to manage notification routing. It provides:

- **Pluggable sender implementations** - Easily swap different notification backends (email, SMS, push notifications, etc.)
- **Preference management** - Store and retrieve user notification preferences with validation
- **Event stream integration** - Subscribe to and process notification events
- **Address verification** - OTP-based address verification for secure notification delivery
- **Batch preference updates** - Set multiple preferences in a single operation
- **Caching layer** - Built-in caching for frequently accessed preferences
- **Event publishing** - Publishes confirmation events when preferences are verified

## Architecture

### Core Components

#### `Module`
The main entry point for the framework. It coordinates:
- Event stream subscriptions for incoming notification events
- Sender implementations for delivering OTPs and notifications
- Preference management for storing user notification preferences
- HTTP routing configuration with authentication

#### `Sender` Trait
A pluggable interface for notification delivery:
```rust
#[async_trait]
pub trait Sender: Send + Sync {
    async fn send(
        &self,
        address: String,
        subject: String,
        message: String,
    ) -> Result<(), anyhow::Error>;
    fn get_name(&self) -> String;
}
```

The `get_name()` method returns a safe identifier for use as a SQL table-name suffix (lowercase letters, digits, and underscores only).

#### `Preferences`
Manages user notification preferences with:
- **Dynamic table creation** - Creates sender-specific preference tables using the sender name
- **In-memory caching** - Moka-based LRU cache for performance
- **OTP verification** - One-time password validation for address confirmation
- **Batch operations** - Groups multiple preferences for atomic confirmation
- **Event publishing** - Publishes `ChannelConfirmed` events on successful verification

### Data Models

#### `Preference`
```rust
pub struct Preference {
    pub subject: String,      // Notification topic (max 64 chars)
    pub address: String,      // Delivery address (max 64 chars)
}
```

#### `PreferenceBatch`
```rust
pub struct PreferenceBatch {
    pub preferences: Vec<Preference>,  // At least one preference required
}
```
All preferences in a batch must share the same address and will be confirmed together with a single OTP.

#### `Token`
```rust
pub struct Token {
    pub token: u32,           // OTP token (6-digit number, range: 100000-999999)
}
```

## Features

### Event Handling
- Subscribes to event streams with subject pattern matching
- Processes events containing user metadata
- Routes notifications based on user preferences
- Only wakes up for configured subjects (not catch-all subscriptions)
- Graceful error handling and logging

### Preference Management
- **Set preferences**: Generate OTP for address verification (accepts a batch)
- **Confirm preferences**: Store verified preferences after OTP validation
- **Get preferences**: Retrieve user's notification address for a subject
- **Validation**: Automatic field validation using the `validator` crate
- **Batch atomic operations**: All preferences in a batch are confirmed together

### Caching Strategy
- **Preference cache**: 1000-entry LRU cache for user preferences (key: `(user, subject)`)
- **Pending cache**: 100-entry cache for OTP verification tokens with 5-minute TTL
- Reduces database queries for frequently accessed data
- Cache-aside pattern for preference retrieval

### Event Publishing
- Publishes `ChannelConfirmed` events after successful preference verification
- Event subject: `contact.channel.confirmed`
- Includes user, subject, and confirmed address information

## Dependencies

- **actix-web** - HTTP framework
- **async-trait** - Async trait support
- **sqlx** - Database access with SQLite
- **serde/serde_json** - JSON serialization
- **moka** - Async caching
- **tracing** - Structured logging
- **validator** - Input validation
- **rand** - Random number generation for OTP
- **actixutils** - Utilities for Actix authentication
- **typed-eventbus** - Event streaming interface

## Usage

### Basic Setup

```rust
use mgk::Module;
use sqlx::SqlitePool;
use std::sync::Arc;

// Create a database pool
let pool = SqlitePool::connect("sqlite://notifications.db").await?;

// Create your event stream
let typed_eventbus = Arc::new(/* your EventStream implementation */);

// Create your custom sender
let sender = Arc::new(MyCustomSender);

// Define which subjects this module will handle
let subjects = vec!["order.created".to_string(), "payment.completed".to_string()];

// Create the module
let module = Module::new(pool, typed_eventbus, sender, subjects).await?;
```

### HTTP Configuration

Configure with an Actix-web app:

```rust
use actix_web::web;

let mut config = web::ServiceConfig::default();
module.config(&mut config, "/notifications");

// Routes will be available at /notifications/preferences/*
```

### Available Endpoints

All endpoints require authentication (via `Auth<Identity>`).

The module provides the following HTTP routes (prefix: `/notifications`):

- `POST /preferences/set` - Set a batch of preferences and receive OTP
  - Body: `PreferenceBatch` with one or more preferences sharing the same address
  - Response: `{ "nonce": "..." }` - use this nonce in the confirm request
  
- `POST /preferences/confirm` - Confirm preferences with OTP token
  - Body: `{ "nonce": "...", "token": 123456 }`
  - Response: 200 OK if successful
  
- `GET /preferences/get` - Retrieve user preference for a subject
  - Query: `?subject=order.created`
  - Response: The notification address or 404 if not found

### Example Request Flow

1. **Client requests to set preferences:**
   ```bash
   POST /notifications/preferences/set
   {
     "preferences": [
       { "subject": "order.created", "address": "user@example.com" },
       { "subject": "payment.completed", "address": "user@example.com" }
     ]
   }
   ```

2. **Server responds with nonce and sends OTP out-of-band:**
   ```json
   {
     "nonce": "a1b2c3d4e5f6g7h8"
   }
   ```
   (OTP sent to `user@example.com` via the configured sender)

3. **Client confirms with OTP and nonce:**
   ```bash
   POST /notifications/preferences/confirm
   {
     "nonce": "a1b2c3d4e5f6g7h8",
     "token": 123456
   }
   ```

4. **Preferences are now stored and confirmed events are published**

## Custom Sender Implementation

Implement the `Sender` trait to integrate with your notification service:

```rust
use async_trait::async_trait;
use mgk::Sender;

struct EmailSender {
    api_key: String,
}

#[async_trait]
impl Sender for EmailSender {
    async fn send(
        &self,
        address: String,
        subject: String,
        message: String,
    ) -> Result<(), anyhow::Error> {
        // Send notification via your service
        println!("Email to {}: {} - {}", address, subject, message);
        Ok(())
    }

    fn get_name(&self) -> String {
        "email".to_string()
    }
}
```

The sender name (`get_name()`) is used to create a table named `{sender_name}_preferences` in the database.

## Database Schema

The framework dynamically creates SQLite tables based on the sender name. For a sender with name "email", the table structure is:

```sql
CREATE TABLE IF NOT EXISTS email_preferences (
    user    TEXT NOT NULL,
    subject TEXT NOT NULL,
    address TEXT NOT NULL,
    UNIQUE(user, subject)
);
```

Multiple senders can store preferences in the same database (each in their own table).

## Event Format

Incoming events are expected to contain metadata in the following format:

```json
{
    "metadata": {
        "user_id": "user_123"
    },
    "data": {
        /* your event data */
    }
}
```

The framework extracts the `user_id` from event metadata and uses it to look up notification preferences for matching subjects.

### Published Events

When a preference is confirmed, an event is published:

```json
{
    "metadata": {
        "source": "mgk"
    },
    "data": {
        "user": "user_123",
        "subject": "order.created",
        "address": "user@example.com"
    }
}
```

Subject: `contact.channel.confirmed`

## Error Handling

The framework implements comprehensive error handling:
- Invalid OTP tokens (mismatched or expired)
- Invalid batch data (empty preferences, mismatched addresses)
- Missing user preferences
- Database errors
- JSON parsing errors
- Missing user ID in event metadata
- Invalid sender names (non-alphanumeric)
- Unauthorized requests (missing authentication)

All errors are logged with context for debugging. Failed OTP deliveries are logged but do not prevent the pending entry from being recorded.

## Performance Considerations

- **Caching**: Preferences are cached in-memory (1000 entries LRU) to reduce database hits
- **Async/await**: All operations are async for non-blocking I/O
- **Connection pooling**: SQLx manages a pool of database connections
- **OTP caching**: Temporary storage of pending verifications (100 entries with 5-minute TTL)
- **Selective subscriptions**: Module subscribes only to configured subjects, not catch-all events
- **Subject filtering**: Only processes events for subjects with stored preferences

## Project Structure

```
mgk/
├── src/
│   ├── lib.rs              # Main module, Module struct, and Sender trait
│   └── prefs/
│       ├── mod.rs          # Module exports
│       ├── db.rs           # Preferences, database, and cache logic
│       ├── handlers.rs     # Request handlers for HTTP endpoints
│       └── routes.rs       # Route configuration
├── Cargo.toml              # Package manifest
└── README.md               # This file
```

## Development

### Prerequisites
- Rust 1.70+
- SQLite 3.x

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

**Repository**: https://github.com/Ferrumec/mgk  
**Language**: Rust  
**Current Version**: 0.1.0  
**Edition**: 2024
