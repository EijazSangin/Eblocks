use std::{
    collections::HashMap,
    env,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use argon2::{
    Argon2,
    password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
};
use axum::{
    Json, Router,
    extract::{
        Path, Query, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::{HeaderMap, StatusCode, header::AUTHORIZATION},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use futures_util::{SinkExt, StreamExt};
use jsonwebtoken::{DecodingKey, EncodingKey, Header, Validation, decode, encode};
use rand_core::OsRng;
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use uuid::Uuid;

const EXPERIENCES: [Experience; 7] = [
    Experience {
        slug: "vortex-plaza",
        title: "Vortex Plaza",
        description: "Meet other players and explore the central hangout.",
    },
    Experience {
        slug: "block-bash",
        title: "Block Bash",
        description: "Jump into a colorful block-building playground.",
    },
    Experience {
        slug: "sky-sprint",
        title: "Sky Sprint",
        description: "Race across floating platforms above the clouds.",
    },
    Experience {
        slug: "my-first-game",
        title: "My First Game",
        description: "Your own world, built one block at a time.",
    },
    Experience {
        slug: "npc-map",
        title: "NPC Park",
        description: "Explore the leafy NPC park and its winding paths.",
    },
    Experience {
        slug: "coastal-drive",
        title: "Coastal Drive",
        description: "Cruise a giant seaside island in your own car.",
    },
    Experience {
        slug: "box-delivery",
        title: "Box Delivery",
        description: "Drive boxes across the city to customers' houses and earn cash.",
    },
];

#[derive(Clone, Serialize)]
struct Experience {
    slug: &'static str,
    title: &'static str,
    description: &'static str,
}

#[derive(Clone)]
struct AppState {
    db: Arc<Mutex<Connection>>,
    rooms: Arc<Mutex<HashMap<String, broadcast::Sender<RoomMessage>>>>,
    jwt_secret: Arc<Vec<u8>>,
}

#[derive(Deserialize)]
struct Credentials {
    username: String,
    password: String,
}

#[derive(Clone, Deserialize, Serialize)]
struct PublishedBlock {
    position: [f32; 3],
    color_index: usize,
    #[serde(default = "default_block_size")]
    size: [f32; 3],
    #[serde(default)]
    rotation: f32,
    #[serde(default)]
    tilt: [f32; 2],
}

fn default_block_size() -> [f32; 3] {
    [1.0; 3]
}

#[derive(Clone, Deserialize, Serialize)]
struct PublishedGame {
    id: String,
    title: String,
    owner: String,
    blocks: Vec<PublishedBlock>,
    #[serde(default)]
    tools_enabled: bool,
}

#[derive(Deserialize)]
struct PublishGameRequest {
    title: String,
    blocks: Vec<PublishedBlock>,
    #[serde(default)]
    tools_enabled: bool,
}

#[derive(Serialize)]
struct AuthResponse {
    token: String,
    username: String,
}

#[derive(Deserialize)]
struct UsernameSearch {
    username: String,
}

#[derive(Serialize)]
struct UsernameSearchResult {
    username: String,
}

#[derive(Serialize)]
struct ApiError {
    error: String,
}

#[derive(Serialize, Deserialize)]
struct Claims {
    sub: String,
    exp: u64,
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ClientMessage {
    Position {
        position: [f32; 3],
        body_color: [f32; 3],
        #[serde(default)]
        accessory: Option<String>,
    },
    Chat {
        text: String,
    },
}

#[derive(Clone, Serialize, Deserialize)]
struct PeerUpdate {
    id: String,
    username: String,
    position: [f32; 3],
    body_color: [f32; 3],
    #[serde(default)]
    accessory: Option<String>,
    left: bool,
}

#[derive(Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum RoomMessage {
    Peer {
        update: PeerUpdate,
    },
    Chat {
        id: String,
        username: String,
        text: String,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let jwt_secret = env::var("PLAYVORTEX_JWT_SECRET")
        .map_err(|_| "Set PLAYVORTEX_JWT_SECRET before starting the server.")?;
    if jwt_secret.len() < 32 {
        return Err("PLAYVORTEX_JWT_SECRET must be at least 32 characters.".into());
    }

    let database_path =
        env::var("PLAYVORTEX_DATABASE").unwrap_or_else(|_| "playvortex.sqlite3".to_owned());
    let connection = Connection::open(database_path)?;
    connection.execute(
        "CREATE TABLE IF NOT EXISTS users (
            username TEXT NOT NULL UNIQUE COLLATE NOCASE,
            password_hash TEXT NOT NULL
        )",
        [],
    )?;
    init_wallet_table(&connection)?;
    connection.execute(
        "CREATE TABLE IF NOT EXISTS published_games (
            id TEXT NOT NULL PRIMARY KEY,
            title TEXT NOT NULL COLLATE NOCASE,
            owner TEXT NOT NULL,
            blocks_json TEXT NOT NULL,
            updated_at INTEGER NOT NULL,
            UNIQUE(owner, title)
        )",
        [],
    )?;
    // Older databases do not have this column yet; the error means it already exists.
    let _ = connection.execute(
        "ALTER TABLE published_games ADD COLUMN tools_enabled INTEGER NOT NULL DEFAULT 0",
        [],
    );

    let state = AppState {
        db: Arc::new(Mutex::new(connection)),
        rooms: Arc::new(Mutex::new(HashMap::new())),
        jwt_secret: Arc::new(jwt_secret.into_bytes()),
    };
    let app = Router::new()
        .route("/health", get(health))
        .route("/api/register", post(register))
        .route("/api/login", post(login))
        .route("/api/users", get(search_user))
        .route("/api/experiences", get(list_experiences))
        .route("/api/wallet", get(get_wallet))
        .route("/api/wallet/daily", post(claim_daily_tix))
        .route("/api/wallet/convert", post(convert_tix))
        .route("/api/shop/buy", post(buy_shop_item))
        .route("/api/games", get(list_published_games).post(publish_game))
        .route("/ws/{experience}", get(websocket))
        .with_state(state);

    let address: SocketAddr = env::var("PLAYVORTEX_BIND")
        .unwrap_or_else(|_| "0.0.0.0:3000".to_owned())
        .parse()?;
    let listener = tokio::net::TcpListener::bind(address).await?;
    println!("E Blocks server listening on {address}");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn health() -> &'static str {
    "E Blocks server is running"
}

const TIX_PER_EBUCK: i64 = 10;
const DAILY_TIX: i64 = 1;
const SECONDS_PER_DAY: i64 = 86_400;
// Shop items: (wire id, price in EBucks).
const SHOP_ITEMS: [(&str, i64); 3] = [("red_hair", 100), ("traffic_cone", 150), ("sword", 300)];

#[derive(Serialize, Debug, PartialEq)]
struct Wallet {
    ebucks: i64,
    tix: i64,
    can_claim_daily: bool,
    owned: Vec<String>,
}

#[derive(Deserialize)]
struct BuyItemRequest {
    item: String,
}

#[derive(Deserialize)]
struct ConvertRequest {
    ebucks: i64,
}

fn init_wallet_table(connection: &Connection) -> rusqlite::Result<()> {
    connection
        .execute(
            "CREATE TABLE IF NOT EXISTS wallets (
                username TEXT NOT NULL PRIMARY KEY COLLATE NOCASE,
                ebucks INTEGER NOT NULL DEFAULT 0 CHECK (ebucks >= 0),
                tix INTEGER NOT NULL DEFAULT 0 CHECK (tix >= 0),
                last_claim_day INTEGER NOT NULL DEFAULT -1
            )",
            [],
        )
        .map(|_| ())?;
    connection
        .execute(
            "CREATE TABLE IF NOT EXISTS owned_items (
                username TEXT NOT NULL COLLATE NOCASE,
                item TEXT NOT NULL,
                PRIMARY KEY (username, item)
            )",
            [],
        )
        .map(|_| ())
}

fn owned_items(connection: &Connection, username: &str) -> rusqlite::Result<Vec<String>> {
    let mut statement =
        connection.prepare("SELECT item FROM owned_items WHERE username = ?1 ORDER BY item")?;
    statement
        .query_map([username], |row| row.get(0))?
        .collect()
}

fn buy_item(
    connection: &Connection,
    username: &str,
    item: &str,
    day: i64,
) -> Result<Wallet, ApiFailure> {
    let Some((_, price)) = SHOP_ITEMS.iter().find(|(id, _)| *id == item) else {
        return Err(ApiFailure::new(StatusCode::BAD_REQUEST, "Unknown shop item."));
    };
    read_wallet(connection, username, day).map_err(wallet_error)?;
    let transaction = connection.unchecked_transaction().map_err(wallet_error)?;
    let added = transaction
        .execute(
            "INSERT OR IGNORE INTO owned_items (username, item) VALUES (?1, ?2)",
            params![username, item],
        )
        .map_err(wallet_error)?;
    if added == 0 {
        return Err(ApiFailure::new(StatusCode::CONFLICT, "You already own that item."));
    }
    let paid = transaction
        .execute(
            "UPDATE wallets SET ebucks = ebucks - ?2 WHERE username = ?1 AND ebucks >= ?2",
            params![username, price],
        )
        .map_err(wallet_error)?;
    if paid == 0 {
        return Err(ApiFailure::new(
            StatusCode::PAYMENT_REQUIRED,
            "You do not have enough EBucks.",
        ));
    }
    transaction.commit().map_err(wallet_error)?;
    read_wallet(connection, username, day).map_err(wallet_error)
}

async fn buy_shop_item(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<BuyItemRequest>,
) -> Result<Json<Wallet>, ApiFailure> {
    let username = authenticated_username(&state, &headers)?;
    let connection = state
        .db
        .lock()
        .map_err(|_| ApiFailure::internal("Wallet is unavailable."))?;
    buy_item(&connection, &username, &request.item, current_day()).map(Json)
}

fn current_day() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64 / SECONDS_PER_DAY)
        .unwrap_or(0)
}

fn read_wallet(connection: &Connection, username: &str, day: i64) -> rusqlite::Result<Wallet> {
    connection.execute(
        "INSERT OR IGNORE INTO wallets (username) VALUES (?1)",
        [username],
    )?;
    let (ebucks, tix, last_claim_day) = connection.query_row(
        "SELECT ebucks, tix, last_claim_day FROM wallets WHERE username = ?1",
        [username],
        |row| Ok((row.get(0)?, row.get(1)?, row.get::<_, i64>(2)?)),
    )?;
    Ok(Wallet {
        ebucks,
        tix,
        can_claim_daily: last_claim_day < day,
        owned: owned_items(connection, username)?,
    })
}

fn claim_daily(connection: &Connection, username: &str, day: i64) -> Result<Wallet, ApiFailure> {
    read_wallet(connection, username, day).map_err(wallet_error)?;
    let claimed = connection
        .execute(
            "UPDATE wallets SET tix = tix + ?2, last_claim_day = ?3
             WHERE username = ?1 AND last_claim_day < ?3",
            params![username, DAILY_TIX, day],
        )
        .map_err(wallet_error)?;
    if claimed == 0 {
        return Err(ApiFailure::new(
            StatusCode::CONFLICT,
            "You already claimed today's Tix. Come back tomorrow!",
        ));
    }
    read_wallet(connection, username, day).map_err(wallet_error)
}

fn convert_tix_to_ebucks(
    connection: &Connection,
    username: &str,
    ebucks: i64,
    day: i64,
) -> Result<Wallet, ApiFailure> {
    if !(1..=100_000).contains(&ebucks) {
        return Err(ApiFailure::new(
            StatusCode::BAD_REQUEST,
            "Choose between 1 and 100,000 EBucks.",
        ));
    }
    read_wallet(connection, username, day).map_err(wallet_error)?;
    let converted = connection
        .execute(
            "UPDATE wallets SET tix = tix - ?2, ebucks = ebucks + ?3
             WHERE username = ?1 AND tix >= ?2",
            params![username, ebucks * TIX_PER_EBUCK, ebucks],
        )
        .map_err(wallet_error)?;
    if converted == 0 {
        return Err(ApiFailure::new(
            StatusCode::PAYMENT_REQUIRED,
            "You do not have enough Tix.",
        ));
    }
    read_wallet(connection, username, day).map_err(wallet_error)
}

fn wallet_error(_: rusqlite::Error) -> ApiFailure {
    ApiFailure::internal("Could not update your wallet.")
}

async fn get_wallet(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Wallet>, ApiFailure> {
    let username = authenticated_username(&state, &headers)?;
    let connection = state
        .db
        .lock()
        .map_err(|_| ApiFailure::internal("Wallet is unavailable."))?;
    read_wallet(&connection, &username, current_day())
        .map(Json)
        .map_err(wallet_error)
}

async fn claim_daily_tix(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Wallet>, ApiFailure> {
    let username = authenticated_username(&state, &headers)?;
    let connection = state
        .db
        .lock()
        .map_err(|_| ApiFailure::internal("Wallet is unavailable."))?;
    claim_daily(&connection, &username, current_day()).map(Json)
}

async fn convert_tix(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<ConvertRequest>,
) -> Result<Json<Wallet>, ApiFailure> {
    let username = authenticated_username(&state, &headers)?;
    let connection = state
        .db
        .lock()
        .map_err(|_| ApiFailure::internal("Wallet is unavailable."))?;
    convert_tix_to_ebucks(&connection, &username, request.ebucks, current_day()).map(Json)
}


async fn list_experiences() -> Json<[Experience; 7]> {
    Json(EXPERIENCES)
}

async fn search_user(
    State(state): State<AppState>,
    Query(search): Query<UsernameSearch>,
) -> Result<Json<UsernameSearchResult>, ApiFailure> {
    let connection = state
        .db
        .lock()
        .map_err(|_| ApiFailure::internal("Account database is unavailable."))?;
    let username = find_username(&connection, &search.username)
        .map_err(|_| ApiFailure::internal("Could not search the account database."))?
        .ok_or_else(|| ApiFailure::new(StatusCode::NOT_FOUND, "Username not found."))?;
    Ok(Json(UsernameSearchResult { username }))
}

fn find_username(connection: &Connection, username: &str) -> rusqlite::Result<Option<String>> {
    connection
        .query_row(
            "SELECT username FROM users WHERE username = ?1 COLLATE NOCASE",
            [username.trim()],
            |row| row.get(0),
        )
        .optional()
}

async fn list_published_games(
    State(state): State<AppState>,
) -> Result<Json<Vec<PublishedGame>>, ApiFailure> {
    let connection = state
        .db
        .lock()
        .map_err(|_| ApiFailure::internal("Game catalog is unavailable."))?;
    let mut statement = connection
        .prepare(
            "SELECT id, title, owner, blocks_json, tools_enabled
             FROM published_games ORDER BY updated_at DESC LIMIT 100",
        )
        .map_err(|_| ApiFailure::internal("Could not read the game catalog."))?;
    let rows = statement
        .query_map([], |row| {
            let blocks_json: String = row.get(3)?;
            let blocks = serde_json::from_str(&blocks_json).map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(
                    3,
                    rusqlite::types::Type::Text,
                    Box::new(error),
                )
            })?;
            Ok(PublishedGame {
                id: row.get(0)?,
                title: row.get(1)?,
                owner: row.get(2)?,
                blocks,
                tools_enabled: row.get::<_, i64>(4)? != 0,
            })
        })
        .map_err(|_| ApiFailure::internal("Could not read the game catalog."))?;
    rows.collect::<Result<Vec<_>, _>>()
        .map(Json)
        .map_err(|_| ApiFailure::internal("A published game has invalid saved blocks."))
}

async fn publish_game(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<PublishGameRequest>,
) -> Result<Json<PublishedGame>, ApiFailure> {
    let owner = authenticated_username(&state, &headers)?;
    validate_published_game(&request)?;
    let blocks_json = serde_json::to_string(&request.blocks)
        .map_err(|_| ApiFailure::internal("Could not encode the game blocks."))?;
    let updated_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ApiFailure::internal("System time is unavailable."))?
        .as_secs() as i64;
    let connection = state
        .db
        .lock()
        .map_err(|_| ApiFailure::internal("Game catalog is unavailable."))?;
    let existing_id = connection
        .query_row(
            "SELECT id FROM published_games
             WHERE owner = ?1 AND title = ?2 COLLATE NOCASE",
            params![owner, request.title.trim()],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(|_| ApiFailure::internal("Could not find the saved game."))?;
    let id = existing_id.unwrap_or_else(|| Uuid::new_v4().to_string());
    connection
        .execute(
            "INSERT INTO published_games (id, title, owner, blocks_json, updated_at, tools_enabled)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(owner, title COLLATE NOCASE) DO UPDATE SET
                 blocks_json = excluded.blocks_json,
                 updated_at = excluded.updated_at,
                 tools_enabled = excluded.tools_enabled",
            params![id, request.title.trim(), owner, blocks_json, updated_at, request.tools_enabled as i64],
        )
        .map_err(|_| ApiFailure::internal("Could not publish the game."))?;
    Ok(Json(PublishedGame {
        id,
        title: request.title.trim().to_owned(),
        owner,
        blocks: request.blocks,
        tools_enabled: request.tools_enabled,
    }))
}

fn validate_published_game(request: &PublishGameRequest) -> Result<(), ApiFailure> {
    let title = request.title.trim();
    if !(3..=40).contains(&title.chars().count()) || title.chars().any(char::is_control) {
        return Err(ApiFailure::new(
            StatusCode::BAD_REQUEST,
            "Game name must be 3–40 characters and cannot be blank.",
        ));
    }
    if request.blocks.len() > 5000 {
        return Err(ApiFailure::new(
            StatusCode::BAD_REQUEST,
            "A game can contain at most 5,000 parts.",
        ));
    }
    if request.blocks.iter().any(|block| {
        block.color_index >= 8
            || block
                .position
                .iter()
                .any(|coordinate| !coordinate.is_finite() || coordinate.abs() > 100.0)
            || block
                .size
                .iter()
                .any(|size| !size.is_finite() || !(0.1..=20.0).contains(size))
            || !block.rotation.is_finite()
            || block.tilt.iter().any(|angle| !angle.is_finite())
    }) {
        return Err(ApiFailure::new(
            StatusCode::BAD_REQUEST,
            "The game contains an invalid part.",
        ));
    }
    Ok(())
}

fn authenticated_username(state: &AppState, headers: &HeaderMap) -> Result<String, ApiFailure> {
    let token = headers
        .get(AUTHORIZATION)
        .and_then(|header| header.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .ok_or_else(|| ApiFailure::new(StatusCode::UNAUTHORIZED, "Sign in to publish a game."))?;
    decode::<Claims>(
        token,
        &DecodingKey::from_secret(state.jwt_secret.as_slice()),
        &Validation::default(),
    )
    .map(|token| token.claims.sub)
    .map_err(|_| ApiFailure::new(StatusCode::UNAUTHORIZED, "Your sign-in has expired."))
}

async fn register(
    State(state): State<AppState>,
    Json(credentials): Json<Credentials>,
) -> Result<Json<AuthResponse>, ApiFailure> {
    validate_credentials(&credentials)?;
    let username = credentials.username.trim().to_owned();
    let salt = SaltString::generate(&mut OsRng);
    let password_hash = Argon2::default()
        .hash_password(credentials.password.as_bytes(), &salt)
        .map_err(|_| ApiFailure::internal("Could not secure account password."))?
        .to_string();
    {
        let connection = state
            .db
            .lock()
            .map_err(|_| ApiFailure::internal("Account database is unavailable."))?;
        connection
            .execute(
                "INSERT INTO users (username, password_hash) VALUES (?1, ?2)",
                params![username, password_hash],
            )
            .map_err(|error| {
                if error.sqlite_error_code() == Some(rusqlite::ErrorCode::ConstraintViolation) {
                    ApiFailure::new(StatusCode::CONFLICT, "That username is already taken.")
                } else {
                    ApiFailure::internal("Could not create the account.")
                }
            })?;
    }
    create_auth_response(&state, username).map(Json)
}

async fn login(
    State(state): State<AppState>,
    Json(credentials): Json<Credentials>,
) -> Result<Json<AuthResponse>, ApiFailure> {
    validate_credentials(&credentials)?;
    let username = credentials.username.trim();
    let stored_hash = {
        let connection = state
            .db
            .lock()
            .map_err(|_| ApiFailure::internal("Account database is unavailable."))?;
        connection
            .query_row(
                "SELECT password_hash FROM users WHERE username = ?1",
                [username],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|_| ApiFailure::internal("Could not read the account database."))?
    };
    let Some(stored_hash) = stored_hash else {
        return Err(ApiFailure::new(
            StatusCode::UNAUTHORIZED,
            "Invalid username or password.",
        ));
    };
    let parsed_hash = PasswordHash::new(&stored_hash)
        .map_err(|_| ApiFailure::internal("Stored account password is invalid."))?;
    if Argon2::default()
        .verify_password(credentials.password.as_bytes(), &parsed_hash)
        .is_err()
    {
        return Err(ApiFailure::new(
            StatusCode::UNAUTHORIZED,
            "Invalid username or password.",
        ));
    }
    create_auth_response(&state, username.to_owned()).map(Json)
}

fn validate_credentials(credentials: &Credentials) -> Result<(), ApiFailure> {
    let username = credentials.username.trim();
    if !(3..=24).contains(&username.chars().count())
        || !username
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || character == '_')
    {
        return Err(ApiFailure::new(
            StatusCode::BAD_REQUEST,
            "Username must be 3–24 characters using letters, numbers, or underscores.",
        ));
    }
    if !(8..=128).contains(&credentials.password.len()) {
        return Err(ApiFailure::new(
            StatusCode::BAD_REQUEST,
            "Password must be between 8 and 128 characters.",
        ));
    }
    Ok(())
}

fn create_auth_response(state: &AppState, username: String) -> Result<AuthResponse, ApiFailure> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ApiFailure::internal("System time is unavailable."))?
        .as_secs();
    let claims = Claims {
        sub: username.clone(),
        exp: now + 60 * 60 * 24,
    };
    let token = encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(state.jwt_secret.as_slice()),
    )
    .map_err(|_| ApiFailure::internal("Could not issue a sign-in token."))?;
    Ok(AuthResponse { token, username })
}

async fn websocket(
    State(state): State<AppState>,
    Path(experience): Path<String>,
    headers: HeaderMap,
    upgrade: WebSocketUpgrade,
) -> Response {
    let Some(token) = headers
        .get(AUTHORIZATION)
        .and_then(|header| header.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
    else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    let claims = match decode::<Claims>(
        token,
        &DecodingKey::from_secret(state.jwt_secret.as_slice()),
        &Validation::default(),
    ) {
        Ok(token) => token.claims,
        Err(_) => return StatusCode::UNAUTHORIZED.into_response(),
    };
    let is_published_game = Uuid::parse_str(experience.strip_prefix("game-").unwrap_or_default())
        .ok()
        .and_then(|id| {
            state.db.lock().ok().and_then(|connection| {
                connection
                    .query_row(
                        "SELECT 1 FROM published_games WHERE id = ?1",
                        [id.to_string()],
                        |_| Ok(()),
                    )
                    .optional()
                    .ok()
                    .flatten()
            })
        })
        .is_some();
    if !EXPERIENCES.iter().any(|item| item.slug == experience) && !is_published_game {
        return StatusCode::NOT_FOUND.into_response();
    }
    upgrade
        .on_upgrade(move |socket| handle_socket(socket, state, experience, claims.sub))
        .into_response()
}

async fn handle_socket(socket: WebSocket, state: AppState, experience: String, username: String) {
    let (sender, mut receiver) = {
        let Ok(mut rooms) = state.rooms.lock() else {
            return;
        };
        let room = rooms
            .entry(experience.clone())
            .or_insert_with(|| broadcast::channel(256).0);
        (room.clone(), room.subscribe())
    };
    let id = Uuid::new_v4().to_string();
    let joined = PeerUpdate {
        id: id.clone(),
        username: username.clone(),
        position: [0.0, 0.0, 0.0],
        body_color: [0.27, 0.48, 0.96],
        accessory: None,
        left: false,
    };
    let _ = sender.send(RoomMessage::Peer { update: joined });
    let (mut sink, mut stream) = socket.split();
    let mut body_color = [0.27, 0.48, 0.96];
    let mut accessory = None;

    loop {
        let mut close_frame = None;
        let disconnected = tokio::select! {
            incoming = stream.next() => {
                match incoming {
                    Some(Ok(Message::Text(text))) => {
                        if let Ok(message) = serde_json::from_str::<ClientMessage>(text.as_str()) {
                            match message {
                                ClientMessage::Position { position, body_color: color, accessory: equipped } => {
                                    if position.iter().all(|value| value.is_finite() && value.abs() <= 1000.0)
                                        && color.iter().all(|value| value.is_finite() && (0.0..=1.0).contains(value))
                                    {
                                        body_color = color;
                                        accessory = sanitize_accessory(equipped).filter(|item| {
                                            accessory.as_deref() == Some(item.as_str())
                                                || state
                                                    .db
                                                    .lock()
                                                    .ok()
                                                    .and_then(|connection| owned_items(&connection, &username).ok())
                                                    .is_some_and(|owned| owned.contains(item))
                                        });
                                        let update = PeerUpdate {
                                            id: id.clone(),
                                            username: username.clone(),
                                            position,
                                            body_color,
                                            accessory: accessory.clone(),
                                            left: false,
                                        };
                                        let _ = sender.send(RoomMessage::Peer { update });
                                    }
                                }
                                ClientMessage::Chat { text } => {
                                    if let Some(safe_text) = sanitize_chat(&text) {
                                        let _ = sender.send(RoomMessage::Chat {
                                            id: id.clone(),
                                            username: username.clone(),
                                            text: safe_text,
                                        });
                                    }
                                }
                            }
                        }
                        false
                    }
                    Some(Ok(Message::Close(frame))) => {
                        close_frame = Some(frame);
                        true
                    }
                    Some(Ok(_)) => false,
                    Some(Err(_)) | None => true,
                }
            }
            message = receiver.recv() => {
                match message {
                    Ok(message) => {
                        if matches!(
                            &message,
                            RoomMessage::Peer { update } if update.id == id
                        ) || matches!(&message, RoomMessage::Chat { id: sender_id, .. } if sender_id == &id)
                        {
                            continue;
                        }
                        let Ok(text) = serde_json::to_string(&message) else {
                            continue;
                        };
                        sink.send(Message::Text(text.into())).await.is_err()
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => false,
                    Err(broadcast::error::RecvError::Closed) => true,
                }
            }
        };
        if disconnected {
            if close_frame.is_some() {
                if let Err(error) = sink.close().await {
                    eprintln!("WebSocket close handshake failed: {error}");
                }
            }
            break;
        }
    }

    let _ = sender.send(RoomMessage::Peer {
        update: PeerUpdate {
            id,
            username,
            position: [0.0, 0.0, 0.0],
            body_color,
            accessory,
            left: true,
        },
    });
}

fn sanitize_chat(text: &str) -> Option<String> {
    let safe_text: String = text
        .trim()
        .chars()
        .filter(|character| !character.is_control())
        .take(120)
        .collect();
    (!safe_text.is_empty()).then_some(safe_text)
}

fn sanitize_accessory(accessory: Option<String>) -> Option<String> {
    accessory.filter(|value| matches!(value.as_str(), "red_hair" | "traffic_cone" | "sword"))
}

#[derive(Debug)]
struct ApiFailure {
    status: StatusCode,
    message: String,
}

impl ApiFailure {
    fn new(status: StatusCode, message: &str) -> Self {
        Self {
            status,
            message: message.to_owned(),
        }
    }

    fn internal(message: &str) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, message)
    }
}

impl IntoResponse for ApiFailure {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(ApiError {
                error: self.message,
            }),
        )
            .into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn usernames_are_trimmed_and_restricted() {
        let valid = Credentials {
            username: " player_7 ".to_owned(),
            password: "password123".to_owned(),
        };
        assert!(validate_credentials(&valid).is_ok());

        for username in ["ab", "player name", "name!"] {
            let invalid = Credentials {
                username: username.to_owned(),
                password: "password123".to_owned(),
            };
            assert!(validate_credentials(&invalid).is_err());
        }
    }

    #[test]
    fn username_search_is_case_insensitive_and_returns_no_password_data() {
        let connection = Connection::open_in_memory().unwrap();
        connection
            .execute(
                "CREATE TABLE users (username TEXT NOT NULL UNIQUE COLLATE NOCASE, password_hash TEXT NOT NULL)",
                [],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO users (username, password_hash) VALUES (?1, ?2)",
                params!["Builder_7", "private-hash"],
            )
            .unwrap();

        assert_eq!(
            find_username(&connection, "  bUiLdEr_7 "),
            Ok(Some("Builder_7".to_owned()))
        );
        assert_eq!(find_username(&connection, "missing"), Ok(None));
    }

    #[test]
    fn passwords_must_fit_the_supported_byte_length() {
        for (length, expected_valid) in [(7, false), (8, true), (128, true), (129, false)] {
            let credentials = Credentials {
                username: "player_7".to_owned(),
                password: "x".repeat(length),
            };
            assert_eq!(validate_credentials(&credentials).is_ok(), expected_valid);
        }
    }

    #[test]
    fn chat_is_trimmed_limited_and_control_free() {
        assert_eq!(
            sanitize_chat("  hello\nthere  "),
            Some("hellothere".to_owned())
        );
        assert_eq!(sanitize_chat(" \n "), None);
        assert_eq!(sanitize_chat(&"x".repeat(150)).unwrap().len(), 120);
    }

    #[test]
    fn accessory_updates_only_allow_catalog_items() {
        assert_eq!(
            sanitize_accessory(Some("sword".to_owned())),
            Some("sword".to_owned())
        );
        assert_eq!(sanitize_accessory(Some("unknown".to_owned())), None);
        assert_eq!(sanitize_accessory(None), None);
    }

    #[test]
    fn position_messages_without_accessories_remain_compatible() {
        let message: ClientMessage = serde_json::from_str(
            r#"{"type":"position","position":[0,0,0],"body_color":[0.2,0.3,0.4]}"#,
        )
        .unwrap();
        assert!(matches!(
            message,
            ClientMessage::Position {
                accessory: None,
                ..
            }
        ));
    }

    #[test]
    fn daily_tix_can_be_claimed_once_per_day_and_converted() {
        let connection = Connection::open_in_memory().unwrap();
        init_wallet_table(&connection).unwrap();

        assert!(read_wallet(&connection, "Ann", 100).unwrap().can_claim_daily);
        let wallet = claim_daily(&connection, "ann", 100).unwrap();
        assert_eq!((wallet.tix, wallet.can_claim_daily), (1, false));
        assert!(claim_daily(&connection, "ann", 100).is_err());
        assert_eq!(claim_daily(&connection, "ann", 101).unwrap().tix, 2);

        assert!(convert_tix_to_ebucks(&connection, "ann", 1, 101).is_err());
        for day in 102..110 {
            claim_daily(&connection, "ann", day).unwrap();
        }
        let wallet = convert_tix_to_ebucks(&connection, "ann", 1, 109).unwrap();
        assert_eq!((wallet.ebucks, wallet.tix), (1, 0));
        assert!(convert_tix_to_ebucks(&connection, "ann", 0, 109).is_err());

        assert!(buy_item(&connection, "ann", "sword", 109).is_err());
        connection
            .execute("UPDATE wallets SET ebucks = 400 WHERE username = 'ann'", [])
            .unwrap();
        let wallet = buy_item(&connection, "ann", "sword", 109).unwrap();
        assert_eq!((wallet.ebucks, wallet.owned), (100, vec!["sword".to_owned()]));
        assert!(buy_item(&connection, "ann", "sword", 109).is_err());
        assert!(buy_item(&connection, "ann", "nope", 109).is_err());
        let wallet = buy_item(&connection, "ann", "red_hair", 109).unwrap();
        assert_eq!(wallet.ebucks, 0);
        assert!(buy_item(&connection, "ann", "traffic_cone", 109).is_err());
        assert_eq!(read_wallet(&connection, "ann", 109).unwrap().owned.len(), 2);
    }

    #[test]
    fn npc_park_is_listed_as_a_builtin_experience() {
        assert!(
            EXPERIENCES.iter().any(|experience| {
                experience.slug == "npc-map" && experience.title == "NPC Park"
            })
        );
        assert!(EXPERIENCES.iter().any(|experience| {
            experience.slug == "coastal-drive" && experience.title == "Coastal Drive"
        }));
    }

    #[test]
    fn published_games_require_valid_names_and_parts() {
        let valid = PublishedBlock {
            position: [0.0, 0.0, 0.0],
            color_index: 0,
            size: default_block_size(),
            rotation: 0.0,
            tilt: [0.0; 2],
        };
        let valid_request = PublishGameRequest {
            title: "My Game".to_owned(),
            blocks: vec![valid.clone()],
            tools_enabled: false,
        };
        assert!(validate_published_game(&valid_request).is_ok());

        for title in ["  ".to_owned(), "ab".to_owned(), "x".repeat(41)] {
            let request = PublishGameRequest {
                title,
                blocks: vec![valid.clone()],
                tools_enabled: false,
            };
            assert!(validate_published_game(&request).is_err());
        }

        let invalid_part = PublishedBlock {
            size: [f32::NAN, 1.0, 1.0],
            ..valid
        };
        let request = PublishGameRequest {
            title: "My Game".to_owned(),
            blocks: vec![invalid_part],
            tools_enabled: false,
        };
        assert!(validate_published_game(&request).is_err());
    }
}
