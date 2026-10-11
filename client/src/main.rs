#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use std::{
    collections::HashMap,
    env, fs,
    io::{Cursor, Read},
    path::PathBuf,
    sync::mpsc::{self, Receiver},
    thread,
    time::Duration,
};

use futures_util::{SinkExt, StreamExt};
use macroquad::prelude::*;
use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc as tokio_mpsc;
use tokio_tungstenite::{
    connect_async,
    tungstenite::{
        Message,
        client::IntoClientRequest,
        http::{HeaderValue, header::AUTHORIZATION},
    },
};

mod studio;
use studio::*;

const BOX_DELIVERY_SLUG: &str = "box-delivery";

const EXPERIENCES: [Experience; 9] = [
    Experience {
        slug: "vortex-plaza",
        title: "Sunset Plaza",
        description: "Meet friends on the sunny block-built island.",
        accent: Color::new(0.29, 0.47, 0.98, 1.0),
    },
    Experience {
        slug: "block-bash",
        title: "Block Bash",
        description: "Jump into a colorful block-building playground.",
        accent: Color::new(0.11, 0.72, 0.55, 1.0),
    },
    Experience {
        slug: "sky-sprint",
        title: "Sky Sprint",
        description: "Race across floating platforms above the clouds.",
        accent: Color::new(0.92, 0.42, 0.26, 1.0),
    },
    Experience {
        slug: "my-first-game",
        title: "My First Game",
        description: "Your own world, built one block at a time.",
        accent: Color::new(0.65, 0.42, 0.95, 1.0),
    },
    Experience {
        slug: "npc-map",
        title: "NPC Park",
        description: "Explore the leafy NPC park and its winding paths.",
        accent: Color::new(0.16, 0.48, 0.24, 1.0),
    },
    Experience {
        slug: "classic-house",
        title: "Classic House",
        description: "Tour the imported 3D farmhouse and courtyard model.",
        accent: Color::new(0.80, 0.64, 0.34, 1.0),
    },
    Experience {
        slug: "coastal-drive",
        title: "Coastal Drive",
        description: "Cruise a giant seaside island in your own car.",
        accent: Color::new(0.05, 0.64, 0.72, 1.0),
    },
    Experience {
        slug: "obby-rift",
        title: "Obby Rift",
        description: "Hop across floating platforms in a long obstacle course.",
        accent: Color::new(0.98, 0.57, 0.09, 1.0),
    },
    Experience {
        slug: BOX_DELIVERY_SLUG,
        title: "Box Delivery",
        description: "Drive boxes across the city to customers' houses and earn cash.",
        accent: Color::new(0.85, 0.55, 0.20, 1.0),
    },
];

// Games shown in the hub list and search; the rest stay internal (Studio and community games use "my-first-game").
const HUB_EXPERIENCES: [Experience; 4] = [
    EXPERIENCES[0],
    EXPERIENCES[6],
    EXPERIENCES[7],
    EXPERIENCES[8],
];

const BODY_COLORS: [Color; 8] = [
    Color::new(0.27, 0.48, 0.96, 1.0),
    Color::new(0.96, 0.32, 0.35, 1.0),
    Color::new(0.15, 0.76, 0.49, 1.0),
    Color::new(0.98, 0.72, 0.20, 1.0),
    Color::new(0.69, 0.36, 0.92, 1.0),
    Color::new(0.12, 0.78, 0.84, 1.0),
    Color::new(0.96, 0.48, 0.75, 1.0),
    Color::new(0.93, 0.93, 0.97, 1.0),
];

const WORLD_CAMERA_FOVY: f32 = 55.0_f32.to_radians();
const AVATAR_PREVIEW_FOVY: f32 = 38.0_f32.to_radians();
const CAMERA_DISTANCE: f32 = 15.0;
const CAMERA_DRAG_SENSITIVITY: f32 = 2.5;
const CAMERA_MIN_PITCH: f32 = 12.0_f32.to_radians();
const CAMERA_MAX_PITCH: f32 = 75.0_f32.to_radians();
const AVATAR_MODEL_SCALE: f32 = 0.4;
const AVATAR_MODEL_GROUND_OFFSET: f32 = 1.2;
const NPC_MAP_SCALE: f32 = 2.0;
const DRIVE_ISLAND_HALF_WIDTH: f32 = 900.0;
const DRIVE_ISLAND_HALF_LENGTH: f32 = 740.0;
const DRIVE_ROAD_RADIUS_X: f32 = 650.0;
const DRIVE_ROAD_RADIUS_Z: f32 = 510.0;

#[derive(Clone, Copy)]
struct Experience {
    slug: &'static str,
    title: &'static str,
    description: &'static str,
    accent: Color,
}

#[derive(Deserialize, Serialize)]
struct AuthResponse {
    token: String,
    username: String,
}

#[derive(Deserialize)]
struct ApiError {
    error: String,
}

#[derive(Clone, Deserialize)]
struct Wallet {
    ebucks: i64,
    tix: i64,
    can_claim_daily: bool,
    #[serde(default)]
    owned: Vec<String>,
}

const TIX_PER_EBUCK: i64 = 10;

enum WalletAction {
    Back,
    ClaimDaily,
    Convert(i64),
    BuyItem(AccessoryId),
}

#[derive(Clone, Deserialize, Serialize)]
struct PeerUpdate {
    id: String,
    username: String,
    position: [f32; 3],
    body_color: [f32; 3],
    #[serde(default)]
    accessory: Option<String>,
    left: bool,
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ServerMessage {
    Peer { update: PeerUpdate },
    Chat { username: String, text: String },
}

#[derive(Serialize)]
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

enum NetworkEvent {
    Peer(PeerUpdate),
    Chat(String),
    Status(String),
}

struct NetworkSession {
    message_tx: tokio_mpsc::UnboundedSender<ClientMessage>,
    event_rx: Receiver<NetworkEvent>,
    peers: HashMap<String, PeerUpdate>,
    chat_messages: Vec<String>,
    status: String,
    offline: bool,
}

#[derive(Default)]
struct AuthForm {
    username: String,
    password: String,
    status: String,
    registering: bool,
    focused: usize,
}

#[derive(Default)]
struct HubSearch {
    open: bool,
    query: String,
    status: String,
    focused: bool,
    category: HubSearchCategory,
}

#[derive(Default, PartialEq, Eq)]
enum HubSearchCategory {
    Users,
    #[default]
    Games,
}

struct Player {
    position: Vec3,
    velocity: Vec3,
    grounded: bool,
    walk_phase: f32,
    walk_amount: f32,
    facing_yaw: f32,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum PlayerTool {
    #[default]
    Sword,
    JumpCoil,
}

impl PlayerTool {
    fn from_index(index: usize) -> Self {
        match index {
            0 => Self::Sword,
            _ => Self::JumpCoil,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Sword => "Sword",
            Self::JumpCoil => "Jump Coil",
        }
    }
}

fn jump_impulse(tool: PlayerTool) -> f32 {
    match tool {
        PlayerTool::Sword => 8.5,
        PlayerTool::JumpCoil => 14.0,
    }
}

struct CarState {
    speed: f32,
}

struct LoadingScreen {
    experience: Experience,
    title: String,
    elapsed: f32,
}

struct DrivingWorld {
    road: Vec<Mesh>,
    props: Vec<DriveProp>,
}

#[derive(Clone, Copy)]
enum DriveProp {
    Building {
        position: Vec3,
        size: Vec3,
        color: Color,
    },
    Tree {
        position: Vec3,
        height: f32,
    },
    Hill {
        position: Vec3,
        size: Vec3,
        color: Color,
    },
}

#[derive(Clone, Copy)]
enum AvatarPart {
    Torso,
    RightArm,
    LeftArm,
    LeftLeg,
    RightLeg,
    Head,
}

impl AvatarPart {
    fn index(self) -> usize {
        match self {
            Self::Torso => 0,
            Self::RightArm => 1,
            Self::LeftArm => 2,
            Self::LeftLeg => 3,
            Self::RightLeg => 4,
            Self::Head => 5,
        }
    }

    fn from_group(group: &str) -> Option<Self> {
        match group.strip_suffix("_Mesh").unwrap_or(group) {
            "Torso" => Some(Self::Torso),
            "RightArm" => Some(Self::RightArm),
            "LeftArm" => Some(Self::LeftArm),
            "LeftLeg" => Some(Self::LeftLeg),
            "RightLeg" => Some(Self::RightLeg),
            "Head" => Some(Self::Head),
            _ => None,
        }
    }
}

struct AvatarModel {
    triangles: [Vec<Vec3>; 6],
    texture_coordinates: [Vec<Vec2>; 6],
    textures: [Option<Texture2D>; 6],
}

struct GuestAvatarModel {
    mesh: Mesh,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum AccessoryId {
    RedHair,
    TrafficCone,
    Sword,
}

impl AccessoryId {
    const ALL: [Self; 3] = [Self::RedHair, Self::TrafficCone, Self::Sword];

    fn index(self) -> usize {
        match self {
            Self::RedHair => 0,
            Self::TrafficCone => 1,
            Self::Sword => 2,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::RedHair => "Red Hair",
            Self::TrafficCone => "Traffic Cone Hat",
            Self::Sword => "Back Sword",
        }
    }

    fn wire_id(self) -> &'static str {
        match self {
            Self::RedHair => "red_hair",
            Self::TrafficCone => "traffic_cone",
            Self::Sword => "sword",
        }
    }

    fn price(self) -> i64 {
        match self {
            Self::RedHair => 100,
            Self::TrafficCone => 150,
            Self::Sword => 300,
        }
    }

    fn from_wire_id(value: &str) -> Option<Self> {
        match value {
            "red_hair" => Some(Self::RedHair),
            "traffic_cone" => Some(Self::TrafficCone),
            "sword" => Some(Self::Sword),
            _ => None,
        }
    }
}

struct AvatarAccessories {
    triangles: [Vec<Vec3>; 3],
    texture_coordinates: [Vec<Vec2>; 3],
    hair_texture: Option<Texture2D>,
}

struct NpcMap {
    meshes: Vec<Mesh>,
}

struct ClassicHouseMap {
    mesh: Mesh,
}

impl ClassicHouseMap {
    fn from_source(source: &str) -> Result<Self, String> {
        let mut positions = Vec::new();
        let mut texture_coordinates = Vec::new();
        let mut vertices = Vec::new();
        let mut face_count = 0;

        for (line_index, line) in source.lines().enumerate() {
            let mut fields = line.split_whitespace();
            match fields.next() {
                Some("v") => {
                    let coordinates: Result<Vec<f32>, _> =
                        fields.take(3).map(str::parse::<f32>).collect();
                    let coordinates = coordinates.map_err(|error| {
                        format!("Invalid vertex on OBJ line {}: {error}", line_index + 1)
                    })?;
                    if coordinates.len() != 3 || coordinates.iter().any(|value| !value.is_finite()) {
                        return Err(format!("Invalid vertex on OBJ line {}.", line_index + 1));
                    }
                    positions.push(vec3(coordinates[0], coordinates[1], coordinates[2]));
                }
                Some("vt") => {
                    let coordinates: Result<Vec<f32>, _> =
                        fields.take(2).map(str::parse::<f32>).collect();
                    let coordinates = coordinates.map_err(|error| {
                        format!(
                            "Invalid texture coordinate on OBJ line {}: {error}",
                            line_index + 1
                        )
                    })?;
                    if coordinates.len() != 2 || coordinates.iter().any(|value| !value.is_finite()) {
                        return Err(format!(
                            "Invalid texture coordinate on OBJ line {}.",
                            line_index + 1
                        ));
                    }
                    texture_coordinates.push(vec2(coordinates[0], coordinates[1]));
                }
                Some("f") => {
                    let face_indices = fields
                        .map(|field| {
                            let mut parts = field.split('/');
                            let position_index = resolve_obj_index(
                                parts.next().unwrap_or_default(),
                                positions.len(),
                            )
                            .ok_or_else(|| {
                                format!(
                                    "Invalid face vertex index on OBJ line {}.",
                                    line_index + 1
                                )
                            })?;
                            let texture_index = parts
                                .next()
                                .filter(|value| !value.is_empty())
                                .and_then(|raw| resolve_obj_index(raw, texture_coordinates.len()));
                            Ok((position_index, texture_index))
                        })
                        .collect::<Result<Vec<_>, String>>()?;
                    if face_indices.len() < 3 {
                        return Err(format!(
                            "Classic House face has fewer than three vertices on OBJ line {}.",
                            line_index + 1
                        ));
                    }
                    for corner in 1..face_indices.len() - 1 {
                        for &(position_index, texture_index) in [
                            face_indices[0],
                            face_indices[corner],
                            face_indices[corner + 1],
                        ]
                        .iter()
                        {
                            let position = positions[position_index];
                            let uv = texture_index
                                .map(|index| texture_coordinates[index])
                                .unwrap_or(Vec2::ZERO);
                            let shade = ((position.x * 0.29 + position.y * 0.17 + position.z * 0.11)
                                .fract()
                                .abs())
                                * 0.18
                                + 0.82;
                            vertices.push(Vertex::new(
                                position.x,
                                position.y,
                                position.z,
                                uv.x,
                                uv.y,
                                Color::new(shade, shade * 0.93, shade * 0.82, 1.0),
                            ));
                        }
                        face_count += 1;
                    }
                }
                _ => {}
            }
        }

        if positions.is_empty() || face_count == 0 || vertices.is_empty() {
            return Err("The Classic House model contains no valid geometry.".to_owned());
        }

        let mut indices = Vec::with_capacity(vertices.len());
        for index in 0..vertices.len() {
            indices.push(index as u16);
        }

        Ok(Self {
            mesh: Mesh {
                vertices,
                indices,
                texture: None,
            },
        })
    }

    fn from_default_path() -> Result<Self, String> {
        let mut candidates = Vec::new();
        if let Ok(path) = env::var("PLAYVORTEX_CLASSIC_HOUSE_OBJ") {
            candidates.push(PathBuf::from(path));
        }
        if let Ok(profile) = env::var("USERPROFILE") {
            candidates.push(PathBuf::from(&profile).join(r"AppData\Local\Temp\classic-house.obj"));
        }
        if let Ok(local_appdata) = env::var("LOCALAPPDATA") {
            candidates.push(PathBuf::from(&local_appdata).join(r"Temp\classic-house.obj"));
        }
        candidates.push(PathBuf::from("classic-house.obj"));
        candidates.push(PathBuf::from(r"..\classic-house.obj"));

        let mut last_error = String::new();
        for path in candidates {
            if !path.is_file() {
                continue;
            }
            match fs::read_to_string(&path) {
                Ok(source) => return Self::from_source(&source),
                Err(error) => last_error = format!("Could not read {}: {error}", path.display()),
            }
        }

        if last_error.is_empty() {
            return Err("The Classic House OBJ file could not be found.".to_owned());
        }
        Err(last_error)
    }
}

impl NpcMap {
    fn from_embedded() -> Result<Self, String> {
        const TEXTURES: [&[u8]; 11] = [
            include_bytes!("../assets/npcmap/textures/5bobux62_diff.png"),
            include_bytes!("../assets/npcmap/textures/Groovydominoes521_diff.png"),
            include_bytes!("../assets/npcmap/textures/Lel2_diff.png"),
            include_bytes!("../assets/npcmap/textures/Part106_diff.png"),
            include_bytes!("../assets/npcmap/textures/Part163_diff.png"),
            include_bytes!("../assets/npcmap/textures/Part436_diff.png"),
            include_bytes!("../assets/npcmap/textures/Part443_diff.png"),
            include_bytes!("../assets/npcmap/textures/Ring1_diff.png"),
            include_bytes!("../assets/npcmap/textures/Sign1_diff.png"),
            include_bytes!("../assets/npcmap/textures/Swarm1_diff.png"),
            include_bytes!("../assets/npcmap/textures/Tangomangle1_diff.png"),
        ];
        let textures = std::iter::once(Texture2D::empty())
            .chain(
                TEXTURES
                    .iter()
                    .map(|bytes| Texture2D::from_file_with_format(bytes, Some(ImageFormat::Png))),
            )
            .collect::<Vec<_>>();
        let mut reader = Cursor::new(include_bytes!("../assets/npcmap/map.mesh").as_slice());
        let mut magic = [0; 4];
        reader
            .read_exact(&mut magic)
            .map_err(|error| format!("Could not read the NPC Park map: {error}"))?;
        if &magic != b"EBM1" {
            return Err("The NPC Park map has an unsupported format.".to_owned());
        }
        let group_count = read_map_u32(&mut reader)? as usize;
        if group_count > 256 {
            return Err("The NPC Park map contains too many mesh groups.".to_owned());
        }

        let mut meshes = Vec::new();
        for _ in 0..group_count {
            let texture_index = read_map_u16(&mut reader)? as usize;
            let vertex_count = read_map_u32(&mut reader)? as usize;
            if texture_index >= textures.len() || vertex_count % 3 != 0 {
                return Err("The NPC Park map contains an invalid mesh.".to_owned());
            }
            let mut remaining = vertex_count;
            while remaining > 0 {
                let chunk_size = remaining.min((u16::MAX as usize / 3) * 3);
                let mut vertices = Vec::with_capacity(chunk_size);
                let mut indices = Vec::with_capacity(chunk_size);
                for index in 0..chunk_size {
                    let mut data = [0; 24];
                    reader
                        .read_exact(&mut data)
                        .map_err(|error| format!("Could not read the NPC Park map: {error}"))?;
                    let coordinate =
                        |offset| f32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
                    let color = Color::from_rgba(data[20], data[21], data[22], data[23]);
                    vertices.push(Vertex::new(
                        coordinate(0) * NPC_MAP_SCALE,
                        coordinate(4) * NPC_MAP_SCALE,
                        coordinate(8) * NPC_MAP_SCALE,
                        coordinate(12),
                        coordinate(16),
                        color,
                    ));
                    indices.push(index as u16);
                }
                meshes.push(Mesh {
                    vertices,
                    indices,
                    texture: (texture_index != 0).then(|| textures[texture_index].clone()),
                });
                remaining -= chunk_size;
            }
        }
        if reader.position() != reader.get_ref().len() as u64 {
            return Err("The NPC Park map contains unexpected trailing data.".to_owned());
        }
        Ok(Self { meshes })
    }
}

fn read_map_u16(reader: &mut Cursor<&[u8]>) -> Result<u16, String> {
    let mut bytes = [0; 2];
    reader
        .read_exact(&mut bytes)
        .map_err(|error| format!("Could not read the NPC Park map: {error}"))?;
    Ok(u16::from_le_bytes(bytes))
}

fn read_map_u32(reader: &mut Cursor<&[u8]>) -> Result<u32, String> {
    let mut bytes = [0; 4];
    reader
        .read_exact(&mut bytes)
        .map_err(|error| format!("Could not read the NPC Park map: {error}"))?;
    Ok(u32::from_le_bytes(bytes))
}

impl DrivingWorld {
    fn new() -> Self {
        let mut road_vertices = Vec::new();
        let mut road_indices = Vec::new();
        let mut road_meshes = Vec::new();
        const SEGMENTS: usize = 720;
        const MAX_ROAD_MESH_VERTICES: usize = 6000;
        for index in 0..SEGMENTS {
            if road_vertices.len() + 24 > MAX_ROAD_MESH_VERTICES {
                road_meshes.push(Mesh {
                    vertices: std::mem::take(&mut road_vertices),
                    indices: std::mem::take(&mut road_indices),
                    texture: None,
                });
            }
            let start = std::f32::consts::TAU * index as f32 / SEGMENTS as f32;
            let end = std::f32::consts::TAU * (index + 1) as f32 / SEGMENTS as f32;
            let point = |angle: f32| {
                vec2(
                    DRIVE_ROAD_RADIUS_X * angle.cos(),
                    DRIVE_ROAD_RADIUS_Z * angle.sin(),
                )
            };
            let center = point(start);
            let next = point(end);
            let tangent = (next - center).normalize_or_zero();
            let side = vec2(tangent.y, -tangent.x);
            let quad = |half_width: f32, y: f32| {
                [
                    vec3(
                        center.x - side.x * half_width,
                        y,
                        center.y - side.y * half_width,
                    ),
                    vec3(
                        center.x + side.x * half_width,
                        y,
                        center.y + side.y * half_width,
                    ),
                    vec3(
                        next.x + side.x * half_width,
                        y,
                        next.y + side.y * half_width,
                    ),
                    vec3(
                        next.x - side.x * half_width,
                        y,
                        next.y - side.y * half_width,
                    ),
                ]
            };
            add_block_quad(
                &mut road_vertices,
                &mut road_indices,
                quad(10.5, -0.005),
                Color::from_rgba(107, 111, 117, 255),
            );
            add_block_quad(
                &mut road_vertices,
                &mut road_indices,
                quad(8.0, 0.008),
                Color::from_rgba(38, 43, 52, 255),
            );
            if index % 8 < 4 {
                let center_line = quad(0.16, 0.018);
                add_block_quad(
                    &mut road_vertices,
                    &mut road_indices,
                    center_line,
                    Color::from_rgba(255, 205, 55, 255),
                );
            }
            for edge in [-7.25_f32, 7.25] {
                let line_center = center + side * edge;
                let line_next = next + side * edge;
                let corners = [
                    vec3(
                        line_center.x - side.x * 0.14,
                        0.02,
                        line_center.y - side.y * 0.14,
                    ),
                    vec3(
                        line_center.x + side.x * 0.14,
                        0.02,
                        line_center.y + side.y * 0.14,
                    ),
                    vec3(
                        line_next.x + side.x * 0.14,
                        0.02,
                        line_next.y + side.y * 0.14,
                    ),
                    vec3(
                        line_next.x - side.x * 0.14,
                        0.02,
                        line_next.y - side.y * 0.14,
                    ),
                ];
                add_block_quad(
                    &mut road_vertices,
                    &mut road_indices,
                    corners,
                    Color::from_rgba(223, 228, 220, 255),
                );
            }
        }
        if !road_vertices.is_empty() {
            road_meshes.push(Mesh {
                vertices: road_vertices,
                indices: road_indices,
                texture: None,
            });
        }

        let mut props = Vec::new();
        for x in (-800_i32..=800).step_by(80) {
            for z in (-640_i32..=640).step_by(80) {
                let normalized = ((x as f32 / DRIVE_ROAD_RADIUS_X).powi(2)
                    + (z as f32 / DRIVE_ROAD_RADIUS_Z).powi(2))
                .sqrt();
                if x.abs() as f32 > DRIVE_ISLAND_HALF_WIDTH - 45.0
                    || z.abs() as f32 > DRIVE_ISLAND_HALF_LENGTH - 45.0
                    || (normalized - 1.0).abs() < 0.075
                {
                    continue;
                }
                let seed =
                    (x as u32).wrapping_mul(0x9E37_79B9) ^ (z as u32).wrapping_mul(0x85EB_CA6B);
                let jitter_x = ((seed & 0xff) as f32 / 255.0 - 0.5) * 34.0;
                let jitter_z = (((seed >> 8) & 0xff) as f32 / 255.0 - 0.5) * 34.0;
                let position = vec3(x as f32 + jitter_x, 0.0, z as f32 + jitter_z);
                if normalized < 0.86 {
                    if seed % 5 == 0 {
                        props.push(DriveProp::Tree {
                            position,
                            height: 7.0 + (seed % 9) as f32,
                        });
                    } else {
                        let height = 12.0 + ((seed >> 12) % 55) as f32;
                        let palette = [
                            Color::from_rgba(230, 112, 83, 255),
                            Color::from_rgba(83, 143, 181, 255),
                            Color::from_rgba(190, 159, 101, 255),
                            Color::from_rgba(132, 111, 174, 255),
                            Color::from_rgba(227, 185, 101, 255),
                            Color::from_rgba(82, 154, 133, 255),
                        ];
                        props.push(DriveProp::Building {
                            position,
                            size: vec3(
                                23.0 + (seed % 12) as f32,
                                height,
                                23.0 + ((seed >> 8) % 12) as f32,
                            ),
                            color: palette[((seed >> 16) as usize) % palette.len()],
                        });
                    }
                } else if normalized > 1.13 && seed % 3 != 0 {
                    props.push(DriveProp::Tree {
                        position,
                        height: 9.0 + (seed % 11) as f32,
                    });
                }
            }
        }

        for index in 0..36 {
            let angle = std::f32::consts::TAU * index as f32 / 36.0;
            let radius_x = DRIVE_ISLAND_HALF_WIDTH - 12.0;
            let radius_z = DRIVE_ISLAND_HALF_LENGTH - 12.0;
            let size = 25.0 + (index % 4) as f32 * 8.0;
            props.push(DriveProp::Hill {
                position: vec3(radius_x * angle.cos(), size * 0.35, radius_z * angle.sin()),
                size: vec3(size, size * 0.7, size),
                color: Color::from_rgba(63, 103, 81, 255),
            });
        }
        Self {
            road: road_meshes,
            props,
        }
    }
}

impl AvatarModel {
    fn from_obj(source: &str) -> Result<Self, String> {
        let mut model = Self {
            triangles: std::array::from_fn(|_| Vec::new()),
            texture_coordinates: std::array::from_fn(|_| Vec::new()),
            textures: std::array::from_fn(|_| None),
        };
        let mut positions = Vec::new();
        let mut texture_coordinates = Vec::new();
        let mut current_part = None;

        for (line_index, line) in source.lines().enumerate() {
            let mut fields = line.split_whitespace();
            match fields.next() {
                Some("v") => {
                    let coordinates: Result<Vec<f32>, _> =
                        fields.take(3).map(str::parse::<f32>).collect();
                    let coordinates = coordinates.map_err(|error| {
                        format!("Invalid vertex on OBJ line {}: {error}", line_index + 1)
                    })?;
                    if coordinates.len() != 3 || coordinates.iter().any(|value| !value.is_finite())
                    {
                        return Err(format!("Invalid vertex on OBJ line {}.", line_index + 1));
                    }
                    positions.push(vec3(coordinates[0], coordinates[1], coordinates[2]));
                }
                Some("vt") => {
                    let coordinates: Result<Vec<f32>, _> =
                        fields.take(2).map(str::parse::<f32>).collect();
                    let coordinates = coordinates.map_err(|error| {
                        format!(
                            "Invalid texture coordinate on OBJ line {}: {error}",
                            line_index + 1
                        )
                    })?;
                    if coordinates.len() != 2 || coordinates.iter().any(|value| !value.is_finite())
                    {
                        return Err(format!(
                            "Invalid texture coordinate on OBJ line {}.",
                            line_index + 1
                        ));
                    }
                    texture_coordinates.push(vec2(coordinates[0], coordinates[1]));
                }
                Some("g") | Some("o") => {
                    let group = fields.next().ok_or_else(|| {
                        format!("Missing avatar part name on OBJ line {}.", line_index + 1)
                    })?;
                    current_part = AvatarPart::from_group(group);
                }
                Some("f") => {
                    let part = current_part.ok_or_else(|| {
                        format!(
                            "Avatar face has no recognized part on OBJ line {}.",
                            line_index + 1
                        )
                    })?;
                    let face_indices: Result<Vec<(usize, Option<usize>)>, String> = fields
                        .map(|field| {
                            let mut indices = field.split('/');
                            let raw_index = indices
                                .next()
                                .unwrap_or_default()
                                .parse::<isize>()
                                .map_err(|error| {
                                    format!(
                                        "Invalid face index on OBJ line {}: {error}",
                                        line_index + 1
                                    )
                                })?;
                            let index = if raw_index > 0 {
                                raw_index - 1
                            } else {
                                positions.len() as isize + raw_index
                            };
                            let vertex_index = usize::try_from(index)
                                .ok()
                                .filter(|index| *index < positions.len())
                                .ok_or_else(|| {
                                    format!(
                                        "Face index is out of range on OBJ line {}.",
                                        line_index + 1
                                    )
                                })?;
                            let texture_index = indices
                                .next()
                                .filter(|index| !index.is_empty())
                                .map(|raw_index| {
                                    let raw_index = raw_index.parse::<isize>().map_err(|error| {
                                        format!(
                                            "Invalid texture coordinate index on OBJ line {}: {error}",
                                            line_index + 1
                                        )
                                    })?;
                                    let index = if raw_index > 0 {
                                        raw_index - 1
                                    } else {
                                        texture_coordinates.len() as isize + raw_index
                                    };
                                    usize::try_from(index)
                                        .ok()
                                        .filter(|index| *index < texture_coordinates.len())
                                        .ok_or_else(|| {
                                            format!(
                                                "Texture coordinate index is out of range on OBJ line {}.",
                                                line_index + 1
                                            )
                                        })
                                })
                                .transpose()?;
                            Ok((vertex_index, texture_index))
                        })
                        .collect();
                    let face_indices = face_indices?;
                    if face_indices.len() < 3 {
                        return Err(format!(
                            "Avatar face has fewer than three vertices on OBJ line {}.",
                            line_index + 1
                        ));
                    }
                    let triangles = &mut model.triangles[part.index()];
                    let coordinates = &mut model.texture_coordinates[part.index()];
                    for corner in 1..face_indices.len() - 1 {
                        for &(vertex_index, texture_index) in [
                            face_indices[0],
                            face_indices[corner],
                            face_indices[corner + 1],
                        ]
                        .iter()
                        {
                            triangles.push(positions[vertex_index]);
                            if let Some(texture_index) = texture_index {
                                coordinates.push(texture_coordinates[texture_index]);
                            } else {
                                coordinates.push(Vec2::ZERO);
                            }
                        }
                    }
                }
                _ => {}
            }
        }

        for (part, triangles) in model.triangles.iter().enumerate() {
            if triangles.len() % 3 != 0
                || triangles.is_empty()
                || (!triangles.is_empty()
                    && model.texture_coordinates[part].len() != triangles.len())
            {
                return Err(format!(
                    "Avatar model part {part} is missing valid triangles."
                ));
            }
        }
        Ok(model)
    }
}

impl GuestAvatarModel {
    fn from_avatar_model(model: &AvatarModel) -> Self {
        let mut vertices = Vec::new();
        let mut indices = Vec::new();
        let skin = Color::from_rgba(245, 195, 153, 255);
        let legs = Color::from_rgba(48, 91, 176, 255);
        for part in [
            AvatarPart::Torso,
            AvatarPart::RightArm,
            AvatarPart::LeftArm,
            AvatarPart::LeftLeg,
            AvatarPart::RightLeg,
            AvatarPart::Head,
        ] {
            let color = match part {
                AvatarPart::Head => skin,
                AvatarPart::LeftLeg | AvatarPart::RightLeg => legs,
                _ => BODY_COLORS[0],
            };
            for (position, uv) in model.triangles[part.index()]
                .iter()
                .zip(&model.texture_coordinates[part.index()])
            {
                indices.push(vertices.len() as u16);
                vertices.push(Vertex::new2(avatar_model_position(*position), *uv, color));
            }
        }
        Self {
            mesh: Mesh {
                vertices,
                indices,
                texture: None,
            },
        }
    }

    #[cfg(test)]
    fn from_gltf(gltf: &str, binary: &[u8], texture: Option<Texture2D>) -> Result<Self, String> {
        let document: serde_json::Value =
            serde_json::from_str(gltf).map_err(|error| format!("Invalid guest avatar: {error}"))?;
        let accessors = document["accessors"]
            .as_array()
            .ok_or_else(|| "Guest avatar has no accessors.".to_owned())?;
        let buffer_views = document["bufferViews"]
            .as_array()
            .ok_or_else(|| "Guest avatar has no buffer views.".to_owned())?;
        let read_view = |accessor_index: usize| -> Result<(usize, usize, usize), String> {
            let accessor = accessors
                .get(accessor_index)
                .ok_or_else(|| format!("Guest avatar is missing accessor {accessor_index}."))?;
            let view_index = accessor["bufferView"]
                .as_u64()
                .ok_or_else(|| format!("Guest avatar accessor {accessor_index} has no view."))?
                as usize;
            let view = buffer_views
                .get(view_index)
                .ok_or_else(|| format!("Guest avatar is missing buffer view {view_index}."))?;
            let offset = view["byteOffset"].as_u64().unwrap_or(0) as usize
                + accessor["byteOffset"].as_u64().unwrap_or(0) as usize;
            let count = accessor["count"]
                .as_u64()
                .ok_or_else(|| format!("Guest avatar accessor {accessor_index} has no count."))?
                as usize;
            let item_size = match accessor["type"].as_str() {
                Some("VEC3") => 12,
                Some("VEC2") => 8,
                Some("SCALAR") => match accessor["componentType"].as_u64() {
                    Some(5125) => 4,
                    Some(5123) => 2,
                    _ => {
                        return Err(format!(
                            "Guest avatar accessor {accessor_index} has an unsupported index type."
                        ));
                    }
                },
                _ => {
                    return Err(format!(
                        "Guest avatar accessor {accessor_index} has an unsupported type."
                    ));
                }
            };
            let stride = view["byteStride"]
                .as_u64()
                .map(|stride| stride as usize)
                .unwrap_or(item_size);
            if stride < item_size
                || offset.saturating_add(count.saturating_mul(stride)) > binary.len()
            {
                return Err(format!(
                    "Guest avatar accessor {accessor_index} is outside its geometry data."
                ));
            }
            Ok((offset, count, stride))
        };

        let (position_offset, vertex_count, position_stride) = read_view(0)?;
        let (uv_offset, uv_count, uv_stride) = read_view(2)?;
        let (index_offset, index_count, index_stride) = read_view(3)?;
        if vertex_count != uv_count || vertex_count > u16::MAX as usize {
            return Err("Guest avatar has incompatible or oversized vertex data.".to_owned());
        }
        let mut positions = Vec::with_capacity(vertex_count);
        let mut min_y = f32::INFINITY;
        let mut max_y = f32::NEG_INFINITY;
        let mut center_x = 0.0;
        let mut center_z = 0.0;
        for index in 0..vertex_count {
            let offset = position_offset + index * position_stride;
            let source = vec3(
                read_f32(binary, offset)?,
                read_f32(binary, offset + 4)?,
                read_f32(binary, offset + 8)?,
            );
            let oriented = vec3(source.x, source.z, -source.y);
            if !oriented.is_finite() {
                return Err("Guest avatar contains invalid vertex coordinates.".to_owned());
            }
            min_y = min_y.min(oriented.y);
            max_y = max_y.max(oriented.y);
            center_x += oriented.x;
            center_z += oriented.z;
            positions.push(oriented);
        }
        let height = max_y - min_y;
        if !height.is_finite() || height <= f32::EPSILON {
            return Err("Guest avatar has no usable height.".to_owned());
        }
        center_x /= vertex_count as f32;
        center_z /= vertex_count as f32;
        let scale = 2.0 / height;
        let mut vertices = Vec::with_capacity(vertex_count);
        for (index, source) in positions.into_iter().enumerate() {
            let uv_offset = uv_offset + index * uv_stride;
            vertices.push(Vertex::new2(
                vec3(
                    (source.x - center_x) * scale,
                    (source.y - min_y) * scale,
                    (source.z - center_z) * scale,
                ),
                vec2(
                    read_f32(binary, uv_offset)?,
                    read_f32(binary, uv_offset + 4)?,
                ),
                WHITE,
            ));
        }
        let mut indices = Vec::with_capacity(index_count);
        for index in 0..index_count {
            let offset = index_offset + index * index_stride;
            let value = if index_stride == 4 {
                u32::from_le_bytes(
                    binary[offset..offset + 4]
                        .try_into()
                        .map_err(|_| "Guest avatar has truncated triangle data.")?,
                ) as usize
            } else {
                u16::from_le_bytes(
                    binary[offset..offset + 2]
                        .try_into()
                        .map_err(|_| "Guest avatar has truncated triangle data.")?,
                ) as usize
            };
            if value >= vertex_count {
                return Err("Guest avatar contains an invalid triangle index.".to_owned());
            }
            indices.push(value as u16);
        }
        if indices.len() % 3 != 0 {
            return Err("Guest avatar triangle data is incomplete.".to_owned());
        }
        Ok(Self {
            mesh: Mesh {
                vertices,
                indices,
                texture,
            },
        })
    }
}

#[cfg(test)]
fn read_f32(bytes: &[u8], offset: usize) -> Result<f32, String> {
    let value = f32::from_le_bytes(
        bytes
            .get(offset..offset.saturating_add(4))
            .ok_or_else(|| "Guest avatar geometry is truncated.".to_owned())?
            .try_into()
            .map_err(|_| "Guest avatar geometry is truncated.".to_owned())?,
    );
    if !value.is_finite() {
        return Err("Guest avatar contains invalid geometry values.".to_owned());
    }
    Ok(value)
}

impl AvatarAccessories {
    fn from_obj(source: &str, hair_texture: Option<Texture2D>) -> Result<Self, String> {
        let mut model = Self {
            triangles: std::array::from_fn(|_| Vec::new()),
            texture_coordinates: std::array::from_fn(|_| Vec::new()),
            hair_texture,
        };
        let mut positions = Vec::new();
        let mut texture_coordinates = Vec::new();
        let mut current_accessory = None;

        for (line_index, line) in source.lines().enumerate() {
            let mut fields = line.split_whitespace();
            match fields.next() {
                Some("v") => {
                    let values = fields
                        .take(3)
                        .map(str::parse::<f32>)
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(|error| {
                            format!(
                                "Invalid accessory vertex on OBJ line {}: {error}",
                                line_index + 1
                            )
                        })?;
                    if values.len() != 3 || values.iter().any(|value| !value.is_finite()) {
                        return Err(format!(
                            "Invalid accessory vertex on OBJ line {}.",
                            line_index + 1
                        ));
                    }
                    positions.push(vec3(values[0], values[1], values[2]));
                }
                Some("vt") => {
                    let values = fields
                        .take(2)
                        .map(str::parse::<f32>)
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(|error| {
                            format!(
                                "Invalid accessory UV on OBJ line {}: {error}",
                                line_index + 1
                            )
                        })?;
                    if values.len() != 2 || values.iter().any(|value| !value.is_finite()) {
                        return Err(format!(
                            "Invalid accessory UV on OBJ line {}.",
                            line_index + 1
                        ));
                    }
                    texture_coordinates.push(vec2(values[0], values[1]));
                }
                Some("g") | Some("o") => {
                    let group = fields.next().ok_or_else(|| {
                        format!("Missing accessory name on OBJ line {}.", line_index + 1)
                    })?;
                    current_accessory = AccessoryId::from_wire_id(group);
                }
                Some("f") => {
                    let accessory = current_accessory.ok_or_else(|| {
                        format!(
                            "Accessory face has no recognized group on OBJ line {}.",
                            line_index + 1
                        )
                    })?;
                    let face = fields
                        .map(|field| {
                            let mut parts = field.split('/');
                            let position = resolve_obj_index(
                                parts.next().unwrap_or_default(),
                                positions.len(),
                            )
                            .ok_or_else(|| {
                                format!(
                                    "Invalid accessory vertex index on OBJ line {}.",
                                    line_index + 1
                                )
                            })?;
                            let uv = parts
                                .next()
                                .filter(|value| !value.is_empty())
                                .map(|value| {
                                    resolve_obj_index(value, texture_coordinates.len()).ok_or_else(
                                        || {
                                            format!(
                                                "Invalid accessory UV index on OBJ line {}.",
                                                line_index + 1
                                            )
                                        },
                                    )
                                })
                                .transpose()?;
                            Ok::<_, String>((position, uv))
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    if face.len() < 3 {
                        return Err(format!(
                            "Accessory face has fewer than three vertices on OBJ line {}.",
                            line_index + 1
                        ));
                    }
                    let triangles = &mut model.triangles[accessory.index()];
                    let coordinates = &mut model.texture_coordinates[accessory.index()];
                    for corner in 1..face.len() - 1 {
                        for &(position, uv) in [face[0], face[corner], face[corner + 1]].iter() {
                            triangles.push(positions[position]);
                            coordinates.push(uv.map_or(Vec2::ZERO, |uv| texture_coordinates[uv]));
                        }
                    }
                }
                _ => {}
            }
        }

        for accessory in AccessoryId::ALL {
            let index = accessory.index();
            if model.triangles[index].is_empty()
                || model.triangles[index].len() % 3 != 0
                || model.triangles[index].len() != model.texture_coordinates[index].len()
            {
                return Err(format!(
                    "Accessory {} is missing valid triangles.",
                    accessory.name()
                ));
            }
        }
        Ok(model)
    }
}

fn resolve_obj_index(raw_index: &str, count: usize) -> Option<usize> {
    let index = raw_index.parse::<isize>().ok()?;
    let index = if index > 0 {
        index - 1
    } else {
        isize::try_from(count).ok()?.checked_add(index)?
    };
    usize::try_from(index).ok().filter(|index| *index < count)
}

struct CameraOrbit {
    yaw: f32,
    pitch: f32,
}

#[derive(Clone, serde::Deserialize, serde::Serialize)]
struct StudioBlock {
    position: [f32; 3],
    color_index: usize,
    #[serde(default = "default_studio_block_size")]
    size: [f32; 3],
    #[serde(default)]
    rotation: f32,
    #[serde(default)]
    tilt: [f32; 2],
}

fn default_studio_block_size() -> [f32; 3] {
    [1.0; 3]
}

#[derive(Clone, Deserialize, Serialize)]
struct PublishedGame {
    id: String,
    title: String,
    owner: String,
    blocks: Vec<StudioBlock>,
    #[serde(default)]
    tools_enabled: bool,
}

#[derive(Clone, Copy, Default, PartialEq, Eq)]
enum StudioTool {
    #[default]
    Move,
    Scale,
    Rotate,
}

#[derive(Default)]
struct StudioUi {
    tool: StudioTool,
    selected_block: Option<usize>,
    publish_name: String,
    publish_focused: bool,
    publish_open: bool,
    step_index: usize,
    exit_requested: bool,
    tools_enabled: bool,
}

enum HubAction {
    Play(Experience),
    PlayPublished(PublishedGame),
    SearchUsername(String),
    SearchGames(String),
    RefreshCatalog,
    Customize,
    Studio,
    Wallet,
    LogOut,
}

#[derive(Clone, Copy)]
enum PauseAction {
    Leave,
    Restart,
    Resume,
}

fn window_conf() -> Conf {
    Conf {
        window_title: "E Blocks".to_owned(),
        window_width: 1280,
        window_height: 800,
        window_resizable: true,
        ..Default::default()
    }
}

#[macroquad::main(window_conf)]
async fn main() {
    let avatar_model = match AvatarModel::from_obj(include_str!("../assets/dynabloxx_avatar.obj")) {
        Ok(model) => model,
        Err(error) => loop {
            clear_background(Color::from_rgba(13, 17, 31, 255));
            draw_text(
                &format!("Could not load the supplied avatar model: {error}"),
                32.0,
                64.0,
                20.0,
                WHITE,
            );
            next_frame().await;
        },
    };
    let hair_texture = Texture2D::from_file_with_format(
        include_bytes!("../assets/accessories/red_hair.png"),
        Some(ImageFormat::Png),
    );
    hair_texture.set_filter(FilterMode::Linear);
    let accessory_model = match AvatarAccessories::from_obj(
        include_str!("../assets/accessories/catalog.obj"),
        Some(hair_texture),
    ) {
        Ok(model) => model,
        Err(error) => loop {
            clear_background(Color::from_rgba(13, 17, 31, 255));
            draw_text(
                &format!("Could not load avatar accessories: {error}"),
                32.0,
                64.0,
                20.0,
                WHITE,
            );
            next_frame().await;
        },
    };
    let guest_avatar_model = GuestAvatarModel::from_avatar_model(&avatar_model);
    let npc_map = match NpcMap::from_embedded() {
        Ok(map) => map,
        Err(error) => loop {
            clear_background(Color::from_rgba(13, 17, 31, 255));
            draw_text(
                &format!("Could not load NPC Park: {error}"),
                32.0,
                64.0,
                20.0,
                WHITE,
            );
            next_frame().await;
        },
    };
    let classic_house_map = match ClassicHouseMap::from_default_path() {
        Ok(map) => Some(map),
        Err(error) => {
            eprintln!("Classic House map not loaded: {error}");
            None
        }
    };
    let driving_world = DrivingWorld::new();
    let base_url = configured_server_url();
    let mut auth_form = AuthForm::default();
    let mut session: Option<AuthResponse> = load_saved_session(&base_url);
    let mut guest_mode = false;
    let mut selected_experience: Option<Experience> = None;
    let mut loading_screen: Option<LoadingScreen> = None;
    let mut active_published_game: Option<PublishedGame> = None;
    let mut avatar_page = false;
    let mut wallet_page = false;
    let mut wallet: Option<Wallet> = None;
    let mut wallet_status = String::new();
    let mut avatar_catalog_page = true;
    let mut studio_page = false;
    let mut network: Option<NetworkSession> = None;
    let (mut body_color, mut color_status) = match load_body_color() {
        Ok(color) => (color, String::new()),
        Err(error) => (BODY_COLORS[0], format!("Could not load avatar: {error}")),
    };
    let (mut equipped_accessory, mut accessory_status) = match load_avatar_accessory() {
        Ok(accessory) => (accessory, String::new()),
        Err(error) => (None, format!("Could not load avatar accessory: {error}")),
    };
    let mut studio_blocks: Vec<StudioBlock> = Vec::new();
    let mut studio_status = String::new();
    let mut studio_game_open = false;
    let mut studio_games = Vec::new();
    let mut studio_menu = StudioMenu::default();
    let mut studio_color_index = 0;
    let mut studio_ui = StudioUi::default();
    let mut published_games = Vec::new();
    let mut catalog_owner: Option<String> = None;
    let mut catalog_status = String::new();
    let mut selected_hub_game = 0;
    let mut hub_list_offset = 0;
    let mut hub_search = HubSearch::default();
    let mut chat_input = String::new();
    let mut chat_open = false;
    let mut avatar_menu_open = false;
    let mut pause_menu_open = false;
    let mut player = Player {
        position: vec3(0.0, 0.0, 0.0),
        velocity: Vec3::ZERO,
        grounded: true,
        walk_phase: 0.0,
        walk_amount: 0.0,
        facing_yaw: 0.0,
    };
    let mut car_state = CarState { speed: 0.0 };
    let mut selected_tool = PlayerTool::Sword;
    let mut sword_swing_timer = 0.0;
    let mut camera_orbit = CameraOrbit {
        yaw: 0.588,
        pitch: 0.56,
    };
    let mut send_timer = 0.0;

    loop {
        clear_background(Color::from_rgba(13, 17, 31, 255));

        if let Some(mut loading) = loading_screen.take() {
            loading.elapsed += get_frame_time().clamp(0.0, 0.1);
            draw_loading_screen(&loading);
            if loading.elapsed < 1.8 {
                loading_screen = Some(loading);
            }
            next_frame().await;
            continue;
        }

        if let Some(experience) = selected_experience {
            let leave_game = run_experience(
                experience,
                &session.as_ref().unwrap().username.clone(),
                &mut body_color,
                &mut equipped_accessory,
                &mut car_state,
                &mut player,
                network.as_mut().unwrap(),
                &mut send_timer,
                &mut chat_open,
                &mut chat_input,
                &mut studio_blocks,
                &mut studio_status,
                &mut studio_color_index,
                &mut studio_ui,
                &base_url,
                &session.as_ref().unwrap().token,
                active_published_game.as_ref(),
                &mut published_games,
                &mut avatar_menu_open,
                &mut pause_menu_open,
                &mut color_status,
                &mut accessory_status,
                false,
                &mut selected_tool,
                &mut sword_swing_timer,
                &mut camera_orbit,
                &avatar_model,
                &accessory_model,
                &driving_world,
                &npc_map,
                classic_house_map.as_ref(),
                guest_mode,
                Some(&guest_avatar_model),
                get_frame_time(),
            );
            if leave_game {
                selected_experience = None;
                active_published_game = None;
                network = None;
                pause_menu_open = false;
                car_state.speed = 0.0;
                chat_open = false;
                chat_input.clear();
                player = Player {
                    position: vec3(0.0, 0.0, 0.0),
                    velocity: Vec3::ZERO,
                    grounded: true,
                    walk_phase: 0.0,
                    walk_amount: 0.0,
                    facing_yaw: 0.0,
                };
            }
            next_frame().await;
            continue;
        }

        if let Some(account) = session.as_ref() {
            let username = account.username.clone();
            let token = account.token.clone();
            if !guest_mode && catalog_owner.as_deref() != Some(username.as_str()) {
                match fetch_published_games(&base_url) {
                    Ok(games) => {
                        published_games = games;
                        catalog_status.clear();
                    }
                    Err(error) => catalog_status = error,
                }
                catalog_owner = Some(username.clone());
            }
            if studio_page && !guest_mode && !studio_game_open {
                match draw_studio_menu(&mut studio_menu, &studio_games, &studio_status) {
                    Some(StudioMenuAction::Back) => {
                        studio_page = false;
                        network = None;
                    }
                    Some(StudioMenuAction::New) => {
                        let name = format!("New Game {}", studio_games.len() + 1);
                        studio_blocks.clear();
                        studio_status = begin_studio_game(None, &name);
                        studio_ui = StudioUi::default();
                        studio_ui.publish_name = name;
                        studio_game_open = true;
                        player.position = vec3(0.0, 6.0, 0.0);
                    }
                    Some(StudioMenuAction::Open(index)) => {
                        if let Some(info) = studio_games.get(index) {
                            match load_studio_game(&info.id) {
                                Ok((blocks, tools_enabled)) => {
                                    studio_blocks = blocks;
                                    studio_status = begin_studio_game(Some(&info.id), &info.name);
                                    studio_ui = StudioUi::default();
                                    studio_ui.publish_name = info.name.clone();
                                    studio_ui.tools_enabled = tools_enabled;
                                    STUDIO_TOOLS_ENABLED
                                        .store(tools_enabled, std::sync::atomic::Ordering::Relaxed);
                                    studio_game_open = true;
                                    player.position = vec3(0.0, 6.0, 0.0);
                                }
                                Err(error) => studio_status = error,
                            }
                        }
                    }
                    Some(StudioMenuAction::Delete(index)) => {
                        if let Some(info) = studio_games.get(index) {
                            studio_status = match delete_studio_game(&info.id) {
                                Ok(()) => format!("Deleted {}.", info.name),
                                Err(error) => error,
                            };
                        }
                        studio_games = list_studio_games();
                    }
                    None => {}
                }
            } else if studio_page && !guest_mode {
                let chat_was_open = chat_open;
                let publish_was_open = studio_ui.publish_open;
                run_experience(
                    EXPERIENCES[3],
                    &username,
                    &mut body_color,
                    &mut equipped_accessory,
                    &mut car_state,
                    &mut player,
                    network.get_or_insert_with(|| {
                        start_network(&base_url, &token, EXPERIENCES[3].slug)
                    }),
                    &mut send_timer,
                    &mut chat_open,
                    &mut chat_input,
                    &mut studio_blocks,
                    &mut studio_status,
                    &mut studio_color_index,
                    &mut studio_ui,
                    &base_url,
                    &token,
                    None,
                    &mut published_games,
                    &mut avatar_menu_open,
                    &mut pause_menu_open,
                    &mut color_status,
                    &mut accessory_status,
                    true,
                    &mut selected_tool,
                    &mut sword_swing_timer,
                    &mut camera_orbit,
                    &avatar_model,
                    &accessory_model,
                    &driving_world,
                    &npc_map,
                    classic_house_map.as_ref(),
                    false,
                    None,
                    get_frame_time(),
                );
                if studio_ui.exit_requested {
                    studio_ui.exit_requested = false;
                    studio_game_open = false;
                    studio_games = list_studio_games();
                    studio_menu = StudioMenu::default();
                    studio_status.clear();
                } else if is_key_pressed(KeyCode::Escape) && publish_was_open && studio_ui.publish_open {
                    studio_ui.publish_open = false;
                    studio_ui.publish_focused = false;
                } else if is_key_pressed(KeyCode::Escape)
                    && !chat_was_open
                    && !chat_open
                    && !studio_ui.publish_open
                {
                    studio_game_open = false;
                    studio_games = list_studio_games();
                    studio_menu = StudioMenu::default();
                    studio_status.clear();
                }
            } else if wallet_page && !guest_mode {
                if let Some(action) = draw_wallet_page(wallet.as_ref(), &wallet_status) {
                    let result = match action {
                        WalletAction::Back => {
                            wallet_page = false;
                            None
                        }
                        WalletAction::ClaimDaily => Some(wallet_request(
                            &base_url,
                            &token,
                            "wallet/daily",
                            None,
                            "You got 1 Tix!",
                        )),
                        WalletAction::Convert(ebucks) => Some(wallet_request(
                            &base_url,
                            &token,
                            "wallet/convert",
                            Some(serde_json::json!({ "ebucks": ebucks })),
                            "Tix converted to EBucks!",
                        )),
                        WalletAction::BuyItem(item) => Some(wallet_request(
                            &base_url,
                            &token,
                            "shop/buy",
                            Some(serde_json::json!({ "item": item.wire_id() })),
                            &format!("You bought {}!", item.name()),
                        )),
                    };
                    match result {
                        Some(Ok((updated, message))) => {
                            wallet = Some(updated);
                            wallet_status = message;
                        }
                        Some(Err(error)) => wallet_status = error,
                        None => {}
                    }
                }
            } else if avatar_page && !guest_mode {
                if draw_avatar_page(
                    &avatar_model,
                    &accessory_model,
                    &mut body_color,
                    &mut equipped_accessory,
                    &mut color_status,
                    &mut accessory_status,
                    &mut avatar_catalog_page,
                    wallet.as_ref().map_or(&[][..], |w| w.owned.as_slice()),
                ) {
                    avatar_page = false;
                }
            } else {
                match draw_hub(
                    &username,
                    if guest_mode { &[] } else { &published_games },
                    &catalog_status,
                    &mut selected_hub_game,
                    &mut hub_list_offset,
                    &mut hub_search,
                    &avatar_model,
                    &accessory_model,
                    body_color,
                    equipped_accessory,
                    guest_mode,
                    &guest_avatar_model,
                ) {
                    Some(HubAction::Play(experience)) => {
                        active_published_game = None;
                        selected_experience = Some(experience);
                        loading_screen = Some(LoadingScreen {
                            experience,
                            title: experience.title.to_owned(),
                            elapsed: 0.0,
                        });
                        player = Player {
                            position: if experience.slug == "coastal-drive" {
                                vec3(0.0, 0.0, DRIVE_ROAD_RADIUS_Z)
                            } else if experience.slug == "classic-house" {
                                vec3(8.0, 0.0, 35.0)
                            } else if experience.slug == OBBY_SLUG {
                                OBBY_SPAWN
                            } else {
                                Vec3::ZERO
                            },
                            velocity: Vec3::ZERO,
                            grounded: true,
                            walk_phase: 0.0,
                            walk_amount: 0.0,
                            facing_yaw: if experience.slug == "coastal-drive" {
                                -std::f32::consts::FRAC_PI_2
                            } else {
                                0.0
                            },
                        };
                        car_state.speed = 0.0;
                        delivery_reset();
                        camera_orbit.yaw = player.facing_yaw + std::f32::consts::PI;
                        network = Some(if guest_mode {
                            guest_network()
                        } else {
                            start_network(&base_url, &token, experience.slug)
                        });
                    }
                    Some(HubAction::PlayPublished(game)) => {
                        if guest_mode {
                            continue;
                        }
                        let slug = format!("game-{}", game.id);
                        selected_experience = Some(EXPERIENCES[3]);
                        network = Some(start_network(&base_url, &token, &slug));
                        loading_screen = Some(LoadingScreen {
                            experience: EXPERIENCES[3],
                            title: game.title.clone(),
                            elapsed: 0.0,
                        });
                        active_published_game = Some(game);
                    }
                    Some(HubAction::SearchUsername(query)) => {
                        hub_search.status = match search_username(&base_url, &query) {
                            Ok(Some(username)) => format!("Found username: {username}"),
                            Ok(None) => "Sorry, that is not a username.".to_owned(),
                            Err(error) => format!("Search failed: {error}"),
                        };
                    }
                    Some(HubAction::SearchGames(query)) => {
                        let searchable_games = if guest_mode {
                            &[][..]
                        } else {
                            published_games.as_slice()
                        };
                        let matches = search_game_titles(&query, searchable_games);
                        hub_search.status = if matches.is_empty() {
                            "Sorry, sir, there is no game with that name.".to_owned()
                        } else {
                            format!(
                                "Found {} matching game{}.",
                                matches.len(),
                                if matches.len() == 1 { "" } else { "s" }
                            )
                        };
                    }
                    Some(HubAction::RefreshCatalog) => match fetch_published_games(&base_url) {
                        Ok(games) => {
                            published_games = games;
                            catalog_status.clear();
                            selected_hub_game = 0;
                            hub_list_offset = 0;
                        }
                        Err(error) => catalog_status = error,
                    },
                    Some(HubAction::Customize) => {
                        if !guest_mode {
                            if let Ok(current) = fetch_wallet(&base_url, &token) {
                                wallet = Some(current);
                            }
                            avatar_catalog_page = true;
                            avatar_page = true;
                        }
                    }
                    Some(HubAction::LogOut) => {
                        clear_saved_session();
                        session = None;
                        guest_mode = false;
                        network = None;
                        wallet = None;
                        wallet_page = false;
                        avatar_page = false;
                        studio_page = false;
                        studio_game_open = false;
                        catalog_owner = None;
                        published_games.clear();
                        auth_form.password.clear();
                        auth_form.status.clear();
                        continue;
                    }
                    Some(HubAction::Wallet) => {
                        if !guest_mode {
                            wallet_status.clear();
                            match fetch_wallet(&base_url, &token) {
                                Ok(current) => wallet = Some(current),
                                Err(error) => {
                                    wallet = None;
                                    wallet_status = error;
                                }
                            }
                            wallet_page = true;
                        }
                    }
                    Some(HubAction::Studio) => {
                        if guest_mode {
                            continue;
                        }
                        studio_page = true;
                        studio_game_open = false;
                        studio_games = list_studio_games();
                        studio_menu = StudioMenu::default();
                        player = Player {
                            position: vec3(0.0, 0.0, 0.0),
                            velocity: Vec3::ZERO,
                            grounded: true,
                            walk_phase: 0.0,
                            walk_amount: 0.0,
                            facing_yaw: 0.0,
                        };
                        network = Some(start_network(&base_url, &token, EXPERIENCES[3].slug));
                    }
                    None => {}
                }
            }
        } else {
            draw_auth(
                &mut auth_form,
                &base_url,
                &mut session,
                &mut guest_mode,
                &guest_avatar_model,
            );
        }

        next_frame().await;
    }
}

fn draw_auth(
    form: &mut AuthForm,
    base_url: &str,
    session: &mut Option<AuthResponse>,
    guest_mode: &mut bool,
    guest_avatar: &GuestAvatarModel,
) {
    let screen_w = screen_width();
    let screen_h = screen_height();
    let center_x = screen_w * 0.5;
    clear_background(Color::from_rgba(4, 5, 11, 255));
    draw_moving_stars(160, 7.0);

    let field_width = (screen_w * 0.54).clamp(230.0, 410.0);
    let label_width = (screen_w * 0.18).clamp(82.0, 132.0);
    let group_width = (field_width + label_width + 14.0).min(screen_w - 24.0);
    let field_width = (group_width - label_width - 14.0).max(150.0);
    let field_x = center_x - group_width * 0.5;
    let label_x = field_x + field_width + 14.0;
    let banner_width = (field_width * 0.86).clamp(230.0, 400.0);
    let banner_height = banner_width * 0.56;
    let banner_x = center_x - banner_width * 0.5;
    let banner_y = (screen_h * 0.045).max(10.0);
    draw_auth_banner(Rect::new(banner_x, banner_y, banner_width, banner_height));

    let field_height = (screen_h * 0.115).clamp(48.0, 68.0);
    let field_gap = (screen_h * 0.035).clamp(14.0, 24.0);
    let first_y = banner_y + banner_height + 16.0;
    let username_rect = Rect::new(field_x, first_y, field_width, field_height);
    let password_rect = Rect::new(
        field_x,
        first_y + field_height + field_gap,
        field_width,
        field_height,
    );

    draw_auth_field(
        form,
        username_rect.x,
        username_rect.y,
        username_rect.w,
        username_rect.h,
        0,
    );
    draw_auth_field(
        form,
        password_rect.x,
        password_rect.y,
        password_rect.w,
        password_rect.h,
        1,
    );
    let label_size = (screen_h * 0.067).clamp(24.0, 38.0);
    draw_text(
        "NAME",
        label_x,
        username_rect.y + username_rect.h * 0.68,
        label_size,
        WHITE,
    );
    draw_text(
        "PASS",
        label_x,
        password_rect.y + password_rect.h * 0.46,
        label_size,
        WHITE,
    );
    draw_text(
        "WORD",
        label_x,
        password_rect.y + password_rect.h * 0.98,
        label_size,
        WHITE,
    );

    let submit_y = password_rect.y + password_rect.h + field_gap + 8.0;
    let submit_rect = Rect::new(
        field_x,
        submit_y,
        (field_width + label_width + 14.0).min(screen_w - 24.0),
        (screen_h * 0.09).clamp(46.0, 60.0),
    );
    draw_rectangle(
        submit_rect.x,
        submit_rect.y,
        submit_rect.w,
        submit_rect.h,
        Color::from_rgba(58, 205, 93, 255),
    );
    let submit_label = if form.registering {
        "CREATE ACCOUNT"
    } else {
        "LOG IN"
    };
    let submit_size = (submit_rect.h * 0.42).clamp(18.0, 24.0);
    let submit_text_width = measure_text(submit_label, None, submit_size as u16, 1.0).width;
    draw_text(
        submit_label,
        submit_rect.x + (submit_rect.w - submit_text_width) * 0.5,
        submit_rect.y + submit_rect.h * 0.67,
        submit_size,
        WHITE,
    );

    let switch_y = submit_rect.y + submit_rect.h + 30.0;
    let guest_rect = if screen_w >= 1050.0 {
        Rect::new(
            field_x + group_width + 24.0,
            first_y + field_height + field_gap * 0.5,
            208.0,
            92.0,
        )
    } else {
        Rect::new(
            (screen_w - 208.0) * 0.5,
            (screen_h - 104.0).max(12.0),
            208.0,
            92.0,
        )
    };
    draw_rectangle(
        guest_rect.x,
        guest_rect.y,
        guest_rect.w,
        guest_rect.h,
        Color::from_rgba(29, 37, 60, 245),
    );
    draw_rectangle_lines(
        guest_rect.x,
        guest_rect.y,
        guest_rect.w,
        guest_rect.h,
        2.0,
        Color::from_rgba(104, 153, 238, 255),
    );
    let guest_viewport = Rect::new(
        guest_rect.x + 7.0,
        guest_rect.y + 7.0,
        56.0,
        guest_rect.h - 14.0,
    );
    let guest_camera = Camera3D {
        position: vec3(0.0, 1.05, 4.5),
        target: vec3(0.0, 1.0, 0.0),
        up: vec3(0.0, 1.0, 0.0),
        fovy: 35.0_f32.to_radians(),
        viewport: Some((
            guest_viewport.x.round() as i32,
            guest_viewport.y.round() as i32,
            guest_viewport.w.round() as i32,
            guest_viewport.h.round() as i32,
        )),
        ..Default::default()
    };
    set_camera(&guest_camera);
    draw_guest_avatar(guest_avatar, Vec3::ZERO, 0.0);
    set_default_camera();
    draw_text(
        "PLAY AS",
        guest_rect.x + 72.0,
        guest_rect.y + 39.0,
        16.0,
        WHITE,
    );
    draw_text(
        "GUEST",
        guest_rect.x + 72.0,
        guest_rect.y + 63.0,
        20.0,
        Color::from_rgba(153, 198, 255, 255),
    );
    let switch_label = if form.registering {
        "Already have an account? Log in"
    } else {
        "New here? Create an account"
    };
    let switch_width = measure_text(switch_label, None, 16, 1.0).width;
    draw_text(
        switch_label,
        center_x - switch_width * 0.5,
        switch_y,
        16.0,
        Color::from_rgba(226, 230, 243, 255),
    );
    let status_size = 14_u16;
    let status_width = measure_text(&form.status, None, status_size, 1.0).width;
    draw_text(
        &form.status,
        center_x - status_width * 0.5,
        (switch_y + 28.0).min(screen_h - 12.0),
        status_size as f32,
        Color::from_rgba(255, 172, 150, 255),
    );

    let mouse = vec2(mouse_position().0, mouse_position().1);
    let switch_rect = Rect::new(
        center_x - switch_width * 0.5 - 8.0,
        switch_y - 21.0,
        switch_width + 16.0,
        28.0,
    );
    let submit_pressed = is_mouse_button_pressed(MouseButton::Left) && submit_rect.contains(mouse);
    if is_mouse_button_pressed(MouseButton::Left) {
        if username_rect.contains(mouse) {
            form.focused = 0;
        } else if password_rect.contains(mouse) {
            form.focused = 1;
        } else if switch_rect.contains(mouse) {
            form.registering = !form.registering;
            form.status.clear();
        }
    }

    while let Some(character) = get_char_pressed() {
        if !character.is_control() {
            if form.focused == 0 {
                if (character.is_ascii_alphanumeric() || character == '_')
                    && form.username.chars().count() < 24
                {
                    form.username.push(character);
                }
            } else if form.password.len() + character.len_utf8() <= 128 {
                form.password.push(character);
            }
        }
    }
    if is_key_pressed(KeyCode::Backspace) {
        let target = if form.focused == 0 {
            &mut form.username
        } else {
            &mut form.password
        };
        target.pop();
    }
    if is_key_pressed(KeyCode::Tab) {
        form.focused = 1 - form.focused;
    }
    let guest_pressed = is_mouse_button_pressed(MouseButton::Left) && guest_rect.contains(mouse);
    if guest_pressed {
        form.status.clear();
        *guest_mode = true;
        *session = Some(AuthResponse {
            token: String::new(),
            username: "Guest".to_owned(),
        });
    } else if submit_pressed || is_key_pressed(KeyCode::Enter) {
        match authenticate(base_url, form) {
            Ok(account) => {
                form.status.clear();
                save_session(&account);
                *guest_mode = false;
                *session = Some(account);
            }
            Err(message) => form.status = message,
        }
    }
}

fn draw_auth_banner(rect: Rect) {
    draw_rectangle(
        rect.x,
        rect.y,
        rect.w,
        rect.h,
        Color::from_rgba(250, 250, 250, 255),
    );
    draw_rectangle_lines(
        rect.x,
        rect.y,
        rect.w,
        rect.h,
        2.0,
        Color::from_rgba(230, 230, 230, 255),
    );
    let time = get_time() as f32;
    for index in 0..58 {
        let start_x = rect.x + ((index as f32 * 47.0 + time * 5.0) % rect.w);
        let start_y = rect.y + ((index as f32 * 31.0 + time * 2.0) % rect.h);
        let length = rect.h * (0.35 + (index % 5) as f32 * 0.09);
        draw_line(
            start_x,
            start_y,
            (start_x + length * 0.52).min(rect.x + rect.w - 1.0),
            (start_y - length).max(rect.y + 1.0),
            if index % 4 == 0 { 2.2 } else { 1.4 },
            Color::from_rgba(225, 28, 42, 210),
        );
    }

    draw_line(
        rect.x + rect.w * 0.12,
        rect.y + rect.h * 0.3,
        rect.x + rect.w * 0.22,
        rect.y + rect.h * 0.13,
        4.0,
        BLACK,
    );
    draw_line(
        rect.x + rect.w * 0.22,
        rect.y + rect.h * 0.13,
        rect.x + rect.w * 0.31,
        rect.y + rect.h * 0.3,
        4.0,
        BLACK,
    );
    for index in 0..5 {
        let dot_x = rect.x + rect.w * (0.39 + index as f32 * 0.055);
        draw_circle(
            dot_x,
            rect.y + rect.h * (0.17 + index as f32 % 2.0 * 0.035),
            3.0,
            BLACK,
        );
    }
    draw_text(
        "E",
        rect.x + rect.w * 0.29,
        rect.y + rect.h * 0.74,
        rect.h * 0.47,
        BLACK,
    );
    draw_text(
        "-",
        rect.x + rect.w * 0.47,
        rect.y + rect.h * 0.75,
        rect.h * 0.42,
        BLACK,
    );
    draw_text(
        "B",
        rect.x + rect.w * 0.58,
        rect.y + rect.h * 0.74,
        rect.h * 0.47,
        BLACK,
    );
    draw_text(
        "L",
        rect.x + rect.w * 0.71,
        rect.y + rect.h * 0.74,
        rect.h * 0.47,
        BLACK,
    );
    draw_text(
        "O",
        rect.x + rect.w * 0.79,
        rect.y + rect.h * 0.74,
        rect.h * 0.47,
        BLACK,
    );
    draw_text(
        "X",
        rect.x + rect.w * 0.88,
        rect.y + rect.h * 0.74,
        rect.h * 0.47,
        BLACK,
    );
}

fn draw_auth_field(form: &AuthForm, x: f32, y: f32, width: f32, height: f32, index: usize) {
    let active = form.focused == index;
    draw_rectangle(x, y, width, height, Color::from_rgba(250, 250, 250, 255));
    draw_rectangle_lines(
        x,
        y,
        width,
        height,
        if active { 3.0 } else { 2.0 },
        Color::from_rgba(15, 15, 18, 255),
    );
    draw_rectangle_lines(
        x + 7.0,
        y + 7.0,
        (width - 14.0).max(1.0),
        (height - 14.0).max(1.0),
        1.0,
        Color::from_rgba(55, 55, 55, 255),
    );
    let value = if index == 0 {
        form.username.clone()
    } else {
        "*".repeat(form.password.chars().count().min(28))
    };
    draw_text(
        if value.is_empty() { "" } else { &value },
        x + 15.0,
        y + height * 0.64,
        (height * 0.35).clamp(18.0, 25.0),
        Color::from_rgba(15, 15, 18, 255),
    );
}

fn authenticate(base_url: &str, form: &AuthForm) -> Result<AuthResponse, String> {
    let username = form.username.trim();
    if username.is_empty() || form.password.is_empty() {
        return Err("Enter a username and password.".to_owned());
    }

    let client = Client::builder()
        .timeout(Duration::from_secs(8))
        .build()
        .map_err(|error| format!("Could not create HTTP client: {error}"))?;
    let endpoint = if form.registering {
        "register"
    } else {
        "login"
    };
    let response = client
        .post(format!("{}/api/{endpoint}", base_url.trim_end_matches('/')))
        .json(&serde_json::json!({
            "username": username,
            "password": form.password,
        }))
        .send()
        .map_err(|error| format!("Could not reach the E Blocks server: {error}"))?;

    if response.status().is_success() {
        response
            .json::<AuthResponse>()
            .map_err(|error| format!("Invalid response from server: {error}"))
    } else {
        let status = response.status();
        let body = response.json::<ApiError>().ok();
        Err(body
            .map(|error| error.error)
            .unwrap_or_else(|| format!("Server returned {status}")))
    }
}

fn fetch_published_games(base_url: &str) -> Result<Vec<PublishedGame>, String> {
    let client = Client::builder()
        .timeout(Duration::from_secs(8))
        .build()
        .map_err(|error| format!("Could not create HTTP client: {error}"))?;
    let response = client
        .get(format!("{}/api/games", base_url.trim_end_matches('/')))
        .send()
        .map_err(|error| format!("Could not load published games: {error}"))?;
    if !response.status().is_success() {
        return Err(format!("Game catalog returned {}", response.status()));
    }
    response
        .json::<Vec<PublishedGame>>()
        .map_err(|error| format!("Invalid game catalog response: {error}"))
}

fn wallet_client() -> Result<Client, String> {
    Client::builder()
        .timeout(Duration::from_secs(8))
        .build()
        .map_err(|error| format!("Could not create HTTP client: {error}"))
}

fn wallet_response(response: reqwest::blocking::Response) -> Result<Wallet, String> {
    if response.status().is_success() {
        response
            .json::<Wallet>()
            .map_err(|error| format!("Invalid wallet response: {error}"))
    } else {
        let status = response.status();
        let body = response.json::<ApiError>().ok();
        Err(body
            .map(|error| error.error)
            .unwrap_or_else(|| format!("Wallet request failed: server returned {status}")))
    }
}

// Server address: PLAYVORTEX_SERVER_URL, else server_url.txt next to the game, else local.
fn configured_server_url() -> String {
    if let Ok(url) = env::var("PLAYVORTEX_SERVER_URL") {
        return url;
    }
    let from_file = env::current_exe()
        .ok()
        .and_then(|exe| fs::read_to_string(exe.with_file_name("server_url.txt")).ok())
        .map(|text| text.trim().to_owned())
        .filter(|text| text.starts_with("http"));
    from_file.unwrap_or_else(|| "http://127.0.0.1:3000".to_owned())
}

fn session_path() -> Result<PathBuf, String> {
    Ok(profile_directory()?.join("session.json"))
}

fn save_session(account: &AuthResponse) {
    if let Ok(path) = session_path() {
        if let Some(parent) = path.parent() {
            let _ = fs::create_dir_all(parent);
        }
        if let Ok(json) = serde_json::to_vec(account) {
            let _ = fs::write(path, json);
        }
    }
}

fn clear_saved_session() {
    if let Ok(path) = session_path() {
        let _ = fs::remove_file(path);
    }
}

// Returns the remembered account only if the server still accepts its token.
fn load_saved_session(base_url: &str) -> Option<AuthResponse> {
    let bytes = fs::read(session_path().ok()?).ok()?;
    let account: AuthResponse = serde_json::from_slice(&bytes).ok()?;
    if account.token.is_empty() {
        return None;
    }
    fetch_wallet(base_url, &account.token).ok().map(|_| account)
}

fn fetch_wallet(base_url: &str, token: &str) -> Result<Wallet, String> {
    let response = wallet_client()?
        .get(format!("{}/api/wallet", base_url.trim_end_matches('/')))
        .bearer_auth(token)
        .send()
        .map_err(|error| format!("Could not load your wallet: {error}"))?;
    wallet_response(response)
}

fn wallet_request(
    base_url: &str,
    token: &str,
    endpoint: &str,
    body: Option<serde_json::Value>,
    success_message: &str,
) -> Result<(Wallet, String), String> {
    let request = wallet_client()?
        .post(format!("{}/api/{endpoint}", base_url.trim_end_matches('/')))
        .bearer_auth(token);
    let request = match body {
        Some(body) => request.json(&body),
        None => request,
    };
    let response = request
        .send()
        .map_err(|error| format!("Could not reach the E Blocks server: {error}"))?;
    wallet_response(response).map(|wallet| (wallet, success_message.to_owned()))
}

fn draw_wallet_page(wallet: Option<&Wallet>, status: &str) -> Option<WalletAction> {
    let width = screen_width();
    let height = screen_height();
    let mouse = vec2(mouse_position().0, mouse_position().1);
    let clicked = is_mouse_button_pressed(MouseButton::Left);
    let mut action = None;

    draw_rectangle(0.0, 0.0, width, height, Color::from_rgba(10, 14, 31, 255));
    draw_moving_stars(120, 8.0);
    let card_w = (width - 40.0).min(560.0);
    let card = Rect::new((width - card_w) * 0.5, 30.0, card_w, (height - 60.0).max(460.0));
    draw_rectangle(card.x, card.y, card.w, card.h, Color::from_rgba(238, 241, 255, 255));
    let ink = Color::from_rgba(30, 34, 48, 255);
    draw_text("Wallet", card.x + 24.0, card.y + 44.0, 36.0, ink);

    let back = Rect::new(card.x + card.w - 104.0, card.y + 16.0, 84.0, 34.0);
    draw_studio_button(back, "BACK", Color::from_rgba(73, 77, 90, 255), 1.0);

    match wallet {
        Some(wallet) => {
            draw_text(
                &format!("EBucks: {}", wallet.ebucks),
                card.x + 24.0,
                card.y + 92.0,
                30.0,
                Color::from_rgba(34, 139, 70, 255),
            );
            draw_text(
                &format!("Tix: {}", wallet.tix),
                card.x + 24.0,
                card.y + 128.0,
                30.0,
                Color::from_rgba(200, 120, 10, 255),
            );

            let daily = Rect::new(card.x + 24.0, card.y + 158.0, card.w - 48.0, 40.0);
            let (label, color) = if wallet.can_claim_daily {
                ("CLAIM DAILY TIX  (+1)", Color::from_rgba(214, 150, 20, 255))
            } else {
                ("DAILY TIX CLAIMED - COME BACK TOMORROW", Color::from_rgba(130, 134, 146, 255))
            };
            draw_studio_button(daily, label, color, 1.2);
            if clicked && wallet.can_claim_daily && daily.contains(mouse) {
                action = Some(WalletAction::ClaimDaily);
            }

            draw_text(
                &format!("Convert Tix to EBucks ({TIX_PER_EBUCK} Tix = 1 EBuck)"),
                card.x + 24.0,
                card.y + 238.0,
                20.0,
                ink,
            );
            let convert = Rect::new(card.x + 24.0, card.y + 252.0, card.w - 48.0, 40.0);
            let can_convert = wallet.tix >= TIX_PER_EBUCK;
            draw_studio_button(
                convert,
                "CONVERT 10 TIX  ->  1 EBUCK",
                if can_convert {
                    Color::from_rgba(34, 139, 70, 255)
                } else {
                    Color::from_rgba(130, 134, 146, 255)
                },
                1.2,
            );
            if clicked && can_convert && convert.contains(mouse) {
                action = Some(WalletAction::Convert(1));
            }

            let gap = 12.0;
            draw_text("Avatar Shop (EBucks)", card.x + 24.0, card.y + 330.0, 20.0, ink);
            let item_w = (card.w - 48.0 - 2.0 * gap) / 3.0;
            for (index, item) in AccessoryId::ALL.iter().copied().enumerate() {
                let rect = Rect::new(
                    card.x + 24.0 + index as f32 * (item_w + gap),
                    card.y + 344.0,
                    item_w,
                    52.0,
                );
                let owned = wallet.owned.iter().any(|id| id == item.wire_id());
                let affordable = wallet.ebucks >= item.price();
                let (label, color) = if owned {
                    (format!("{} - OWNED", item.name()), Color::from_rgba(130, 134, 146, 255))
                } else if affordable {
                    (
                        format!("{} - {}", item.name(), item.price()),
                        Color::from_rgba(34, 139, 70, 255),
                    )
                } else {
                    (
                        format!("{} - {}", item.name(), item.price()),
                        Color::from_rgba(160, 90, 90, 255),
                    )
                };
                draw_studio_button(rect, &label, color, 1.0);
                if clicked && !owned && rect.contains(mouse) {
                    action = Some(WalletAction::BuyItem(item));
                }
            }
        }
        None => {
            draw_text("Wallet unavailable.", card.x + 24.0, card.y + 100.0, 24.0, ink);
        }
    }

    if !status.is_empty() {
        let shown = status.chars().take(60).collect::<String>();
        draw_text(&shown, card.x + 24.0, card.y + 430.0, 18.0, Color::from_rgba(150, 56, 56, 255));
    }
    if clicked && back.contains(mouse) || is_key_pressed(KeyCode::Escape) {
        action = Some(WalletAction::Back);
    }
    action
}

fn search_username(base_url: &str, username: &str) -> Result<Option<String>, String> {
    let client = Client::builder()
        .timeout(Duration::from_secs(8))
        .build()
        .map_err(|error| format!("Could not create HTTP client: {error}"))?;
    let response = client
        .get(format!("{}/api/users", base_url.trim_end_matches('/')))
        .query(&[("username", username.trim())])
        .send()
        .map_err(|error| format!("Could not search usernames: {error}"))?;
    if response.status() == reqwest::StatusCode::NOT_FOUND {
        return Ok(None);
    }
    if !response.status().is_success() {
        return Err(format!("Username search returned {}", response.status()));
    }
    response
        .json::<UsernameSearchResult>()
        .map(|result| Some(result.username))
        .map_err(|error| format!("Invalid username search response: {error}"))
}

#[derive(Deserialize)]
struct UsernameSearchResult {
    username: String,
}

fn search_game_titles(query: &str, published_games: &[PublishedGame]) -> Vec<String> {
    let query = query.trim().to_lowercase();
    if query.is_empty() {
        return Vec::new();
    }
    HUB_EXPERIENCES
        .iter()
        .map(|experience| experience.title.to_owned())
        .chain(published_games.iter().map(|game| game.title.clone()))
        .filter(|title| title.to_lowercase().contains(&query))
        .take(4)
        .collect()
}

fn publish_game_to_server(
    base_url: &str,
    token: &str,
    title: &str,
    blocks: &[StudioBlock],
    tools_enabled: bool,
) -> Result<PublishedGame, String> {
    let client = Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .map_err(|error| format!("Could not create HTTP client: {error}"))?;
    let response = client
        .post(format!("{}/api/games", base_url.trim_end_matches('/')))
        .bearer_auth(token)
        .json(&serde_json::json!({ "title": title, "blocks": blocks, "tools_enabled": tools_enabled }))
        .send()
        .map_err(|error| format!("Could not publish game: {error}"))?;
    if response.status().is_success() {
        response
            .json::<PublishedGame>()
            .map_err(|error| format!("Invalid publish response: {error}"))
    } else {
        let status = response.status();
        let body = response.json::<ApiError>().ok();
        Err(body
            .map(|error| error.error)
            .unwrap_or_else(|| format!("Publish failed: server returned {status}")))
    }
}

fn draw_loading_screen(loading: &LoadingScreen) {
    let width = screen_width();
    let height = screen_height();
    let time = get_time() as f32;
    draw_rectangle(0.0, 0.0, width, height, Color::from_rgba(10, 14, 31, 255));
    draw_circle(
        width * 0.5,
        height * 0.43,
        height * 0.48,
        Color::new(
            loading.experience.accent.r,
            loading.experience.accent.g,
            loading.experience.accent.b,
            0.08,
        ),
    );

    for index in 0..84 {
        let x = ((index * 197 + 43) as f32 % width.max(1.0)) + 2.0;
        let y = ((index * 113 + 29) as f32 % height.max(1.0)) + 2.0;
        let twinkle = ((time * 1.8 + index as f32 * 0.73).sin() * 0.5 + 0.5) * 0.65 + 0.25;
        let radius = if index % 9 == 0 { 2.0 } else { 1.1 };
        draw_circle(x, y, radius, Color::new(0.76, 0.84, 1.0, twinkle));
        if index % 9 == 0 {
            draw_line(
                x - 4.0,
                y,
                x + 4.0,
                y,
                1.0,
                Color::new(0.76, 0.84, 1.0, twinkle * 0.45),
            );
            draw_line(
                x,
                y - 4.0,
                x,
                y + 4.0,
                1.0,
                Color::new(0.76, 0.84, 1.0, twinkle * 0.45),
            );
        }
    }

    let panel_width = (width - 48.0).clamp(280.0, 720.0);
    let preview_width = panel_width - 20.0;
    let preview_height = (preview_width * 0.48).min((height * 0.4).max(145.0));
    let preview = Rect::new(
        (width - preview_width) * 0.5,
        (height - preview_height) * 0.5 - 62.0,
        preview_width,
        preview_height,
    );
    draw_text(
        "GETTING YOUR WORLD READY",
        width * 0.5 - 120.0,
        preview.y - 28.0,
        16.0,
        Color::from_rgba(179, 192, 225, 255),
    );
    draw_rectangle(
        preview.x - 3.0,
        preview.y - 3.0,
        preview.w + 6.0,
        preview.h + 6.0,
        Color::new(
            loading.experience.accent.r,
            loading.experience.accent.g,
            loading.experience.accent.b,
            0.7,
        ),
    );
    draw_loading_preview(loading.experience, preview);

    let title_width = measure_text(&loading.title, None, 36, 1.0).width.max(1.0);
    let title_size = (36.0 * ((panel_width - 24.0) / title_width).min(1.0)).max(18.0);
    let title_width = measure_text(&loading.title, None, title_size as u16, 1.0).width;
    let title_y = preview.y + preview.h + 55.0;
    draw_text(
        &loading.title,
        (width - title_width) * 0.5,
        title_y,
        title_size,
        WHITE,
    );

    let progress_width = 180.0;
    let progress_y = title_y + 34.0;
    draw_text(
        "LOADING",
        width * 0.5 - 42.0,
        progress_y,
        13.0,
        Color::from_rgba(162, 176, 211, 255),
    );
    for index in 0..3 {
        let phase = ((time * 4.0 - index as f32 * 0.55).sin() * 0.5 + 0.5) * 0.65 + 0.2;
        draw_circle(
            width * 0.5 + 42.0 + index as f32 * 12.0,
            progress_y - 4.0,
            2.4,
            Color::new(
                loading.experience.accent.r,
                loading.experience.accent.g,
                loading.experience.accent.b,
                phase,
            ),
        );
    }
    let bar_x = (width - progress_width) * 0.5;
    draw_rectangle(
        bar_x,
        progress_y + 12.0,
        progress_width,
        4.0,
        Color::from_rgba(45, 53, 77, 255),
    );
    draw_rectangle(
        bar_x,
        progress_y + 12.0,
        progress_width * (loading.elapsed / 1.8).clamp(0.08, 1.0),
        4.0,
        loading.experience.accent,
    );
}

fn draw_loading_preview(experience: Experience, rect: Rect) {
    let x = rect.x;
    let y = rect.y;
    let w = rect.w;
    let h = rect.h;
    draw_rectangle(x, y, w, h, Color::from_rgba(87, 156, 204, 255));
    draw_rectangle(
        x,
        y + h * 0.7,
        w,
        h * 0.3,
        Color::from_rgba(63, 126, 83, 255),
    );

    match experience.slug {
        "vortex-plaza" => {
            draw_circle(
                x + w * 0.78,
                y + h * 0.25,
                h * 0.12,
                Color::from_rgba(255, 210, 117, 255),
            );
            for (index, building) in [
                (0.12, 0.31, 0.13, 0.42),
                (0.3, 0.23, 0.12, 0.5),
                (0.47, 0.35, 0.16, 0.38),
                (0.67, 0.27, 0.13, 0.46),
                (0.84, 0.34, 0.1, 0.39),
            ]
            .into_iter()
            .enumerate()
            {
                let building_x = x + w * building.0;
                let building_y = y + h * (0.7 - building.3);
                let building_w = w * building.2;
                let building_h = h * building.3;
                draw_rectangle(
                    building_x,
                    building_y,
                    building_w,
                    building_h,
                    if index % 2 == 0 {
                        Color::from_rgba(85, 79, 126, 255)
                    } else {
                        Color::from_rgba(66, 91, 139, 255)
                    },
                );
                for row in 0..3 {
                    draw_rectangle(
                        building_x + building_w * 0.2,
                        building_y + building_h * (0.18 + row as f32 * 0.22),
                        building_w * 0.18,
                        building_h * 0.08,
                        Color::from_rgba(255, 211, 131, 255),
                    );
                    draw_rectangle(
                        building_x + building_w * 0.62,
                        building_y + building_h * (0.18 + row as f32 * 0.22),
                        building_w * 0.18,
                        building_h * 0.08,
                        Color::from_rgba(255, 211, 131, 255),
                    );
                }
            }
            draw_rectangle(
                x + w * 0.35,
                y + h * 0.76,
                w * 0.3,
                h * 0.09,
                Color::from_rgba(43, 49, 66, 255),
            );
            draw_block_player(x + w * 0.49, y + h * 0.68, h * 0.16);
        }
        "block-bash" => {
            draw_circle(
                x + w * 0.2,
                y + h * 0.2,
                h * 0.09,
                Color::from_rgba(255, 230, 143, 255),
            );
            for (px, py, pw, color) in [
                (0.12, 0.67, 0.22, Color::from_rgba(235, 88, 97, 255)),
                (0.39, 0.52, 0.22, Color::from_rgba(247, 185, 74, 255)),
                (0.67, 0.65, 0.22, Color::from_rgba(121, 95, 230, 255)),
            ] {
                draw_rectangle(x + w * px, y + h * py, w * pw, h * 0.1, color);
                draw_rectangle(
                    x + w * (px + 0.03),
                    y + h * (py + 0.1),
                    w * 0.04,
                    h * 0.08,
                    color,
                );
                draw_rectangle(
                    x + w * (px + pw - 0.07),
                    y + h * (py + 0.1),
                    w * 0.04,
                    h * 0.08,
                    color,
                );
            }
            draw_block_player(x + w * 0.49, y + h * 0.48, h * 0.16);
        }
        "sky-sprint" => {
            for (cx, cy) in [(0.2, 0.23), (0.76, 0.18), (0.55, 0.32)] {
                draw_circle(
                    x + w * cx,
                    y + h * cy,
                    h * 0.07,
                    Color::from_rgba(233, 245, 255, 220),
                );
                draw_circle(
                    x + w * (cx + 0.07),
                    y + h * (cy + 0.02),
                    h * 0.055,
                    Color::from_rgba(233, 245, 255, 220),
                );
            }
            for (px, py, pw) in [(0.08, 0.69, 0.24), (0.39, 0.53, 0.23), (0.7, 0.67, 0.22)] {
                draw_rectangle(
                    x + w * px,
                    y + h * py,
                    w * pw,
                    h * 0.075,
                    Color::from_rgba(246, 136, 84, 255),
                );
                draw_rectangle(
                    x + w * px,
                    y + h * (py + 0.075),
                    w * pw,
                    h * 0.035,
                    Color::from_rgba(250, 198, 106, 255),
                );
            }
            draw_block_player(x + w * 0.48, y + h * 0.45, h * 0.17);
        }
        "npc-map" => {
            draw_rectangle(
                x,
                y + h * 0.72,
                w,
                h * 0.28,
                Color::from_rgba(71, 143, 82, 255),
            );
            draw_rectangle(
                x + w * 0.41,
                y + h * 0.71,
                w * 0.18,
                h * 0.29,
                Color::from_rgba(196, 177, 134, 255),
            );
            for (tx, ty, size) in [
                (0.13, 0.47, 0.12),
                (0.3, 0.36, 0.15),
                (0.72, 0.39, 0.16),
                (0.88, 0.51, 0.12),
            ] {
                draw_rectangle(
                    x + w * tx,
                    y + h * ty,
                    w * 0.035,
                    h * 0.25,
                    Color::from_rgba(116, 79, 52, 255),
                );
                draw_circle(
                    x + w * tx,
                    y + h * ty,
                    h * size,
                    Color::from_rgba(43, 125, 72, 255),
                );
                draw_circle(
                    x + w * (tx - 0.035),
                    y + h * (ty + 0.04),
                    h * size * 0.63,
                    Color::from_rgba(57, 147, 81, 255),
                );
            }
            draw_block_player(x + w * 0.49, y + h * 0.62, h * 0.16);
        }
        "coastal-drive" => {
            draw_rectangle(
                x,
                y + h * 0.54,
                w,
                h * 0.46,
                Color::from_rgba(35, 151, 184, 255),
            );
            draw_rectangle(
                x,
                y + h * 0.62,
                w,
                h * 0.38,
                Color::from_rgba(104, 166, 96, 255),
            );
            let road = Color::from_rgba(58, 63, 76, 255);
            draw_line(
                x + w * 0.08,
                y + h * 0.81,
                x + w * 0.29,
                y + h * 0.67,
                h * 0.16,
                road,
            );
            draw_line(
                x + w * 0.29,
                y + h * 0.67,
                x + w * 0.52,
                y + h * 0.76,
                h * 0.16,
                road,
            );
            draw_line(
                x + w * 0.52,
                y + h * 0.76,
                x + w * 0.75,
                y + h * 0.63,
                h * 0.16,
                road,
            );
            draw_line(
                x + w * 0.75,
                y + h * 0.63,
                x + w * 0.94,
                y + h * 0.74,
                h * 0.16,
                road,
            );
            draw_line(
                x + w * 0.08,
                y + h * 0.81,
                x + w * 0.29,
                y + h * 0.67,
                2.0,
                Color::from_rgba(238, 220, 164, 255),
            );
            draw_line(
                x + w * 0.29,
                y + h * 0.67,
                x + w * 0.52,
                y + h * 0.76,
                2.0,
                Color::from_rgba(238, 220, 164, 255),
            );
            draw_line(
                x + w * 0.52,
                y + h * 0.76,
                x + w * 0.75,
                y + h * 0.63,
                2.0,
                Color::from_rgba(238, 220, 164, 255),
            );
            draw_line(
                x + w * 0.75,
                y + h * 0.63,
                x + w * 0.94,
                y + h * 0.74,
                2.0,
                Color::from_rgba(238, 220, 164, 255),
            );
            draw_rectangle(
                x + w * 0.46,
                y + h * 0.64,
                w * 0.11,
                h * 0.045,
                Color::from_rgba(234, 80, 65, 255),
            );
            draw_rectangle(
                x + w * 0.48,
                y + h * 0.61,
                w * 0.06,
                h * 0.035,
                Color::from_rgba(136, 205, 223, 255),
            );
            draw_circle(
                x + w * 0.48,
                y + h * 0.69,
                h * 0.018,
                Color::from_rgba(28, 32, 40, 255),
            );
            draw_circle(
                x + w * 0.55,
                y + h * 0.69,
                h * 0.018,
                Color::from_rgba(28, 32, 40, 255),
            );
        }
        OBBY_SLUG => {
            for (px, py, pw, color) in [
                (0.06, 0.74, 0.22, Color::from_rgba(40, 214, 111, 255)),
                (0.34, 0.6, 0.18, Color::from_rgba(255, 201, 40, 255)),
                (0.58, 0.46, 0.16, Color::from_rgba(59, 63, 74, 255)),
                (0.78, 0.3, 0.16, Color::from_rgba(255, 149, 5, 255)),
            ] {
                draw_rectangle(x + w * px, y + h * py, w * pw, h * 0.06, color);
            }
            draw_block_player(x + w * 0.15, y + h * 0.58, h * 0.14);
        }
        _ => {
            draw_rectangle(
                x,
                y + h * 0.73,
                w,
                h * 0.27,
                Color::from_rgba(80, 142, 91, 255),
            );
            draw_rectangle(
                x + w * 0.12,
                y + h * 0.6,
                w * 0.16,
                h * 0.13,
                Color::from_rgba(90, 117, 226, 255),
            );
            draw_rectangle(
                x + w * 0.3,
                y + h * 0.48,
                w * 0.18,
                h * 0.25,
                Color::from_rgba(231, 166, 64, 255),
            );
            draw_rectangle(
                x + w * 0.68,
                y + h * 0.56,
                w * 0.2,
                h * 0.17,
                Color::from_rgba(188, 83, 103, 255),
            );
            draw_block_player(x + w * 0.52, y + h * 0.61, h * 0.16);
        }
    }

    for index in 0..5 {
        let dot_x = x + 12.0 + index as f32 * 12.0;
        draw_circle(dot_x, y + 12.0, 2.0, Color::new(1.0, 1.0, 1.0, 0.35));
    }
}

fn draw_block_player(center_x: f32, feet_y: f32, height: f32) {
    let head = height * 0.36;
    let body = height * 0.38;
    let leg = height * 0.26;
    draw_rectangle(
        center_x - head * 0.38,
        feet_y - height,
        head * 0.76,
        head,
        Color::from_rgba(244, 194, 145, 255),
    );
    draw_rectangle(
        center_x - height * 0.31,
        feet_y - height + head,
        height * 0.62,
        body,
        Color::from_rgba(74, 124, 238, 255),
    );
    draw_rectangle(
        center_x - height * 0.27,
        feet_y - leg,
        height * 0.22,
        leg,
        Color::from_rgba(43, 54, 91, 255),
    );
    draw_rectangle(
        center_x + height * 0.05,
        feet_y - leg,
        height * 0.22,
        leg,
        Color::from_rgba(43, 54, 91, 255),
    );
}

fn draw_moving_stars(count: usize, drift_speed: f32) {
    let width = screen_width().max(1.0);
    let height = screen_height().max(1.0);
    let time = get_time() as f32;
    for index in 0..count {
        let seed = index as f32;
        let x = (seed * 197.0 + 43.0 + time * drift_speed * (0.35 + seed % 5.0 * 0.12))
            .rem_euclid(width);
        let y = (seed * 113.0 + 29.0 + time * drift_speed * (0.16 + seed % 7.0 * 0.035))
            .rem_euclid(height);
        let brightness = ((time * 1.4 + seed * 0.73).sin() * 0.5 + 0.5) * 0.62 + 0.2;
        let radius = if index % 13 == 0 { 1.8 } else { 1.0 };
        draw_circle(x, y, radius, Color::new(0.82, 0.88, 1.0, brightness));
    }
}

fn draw_pause_menu(player_count: usize) -> Option<PauseAction> {
    let width = screen_width();
    let height = screen_height();
    let time = get_time() as f32;
    draw_rectangle(0.0, 0.0, width, height, Color::new(0.03, 0.04, 0.1, 0.78));

    for index in 0..72 {
        let x = ((index * 181 + 67) as f32 % width.max(1.0)) + 2.0;
        let y = ((index * 127 + 31) as f32 % height.max(1.0)) + 2.0;
        let brightness = ((time * 1.5 + index as f32 * 0.91).sin() * 0.5 + 0.5) * 0.65 + 0.25;
        let radius = if index % 8 == 0 { 2.0 } else { 1.0 };
        draw_circle(x, y, radius, Color::new(0.8, 0.86, 1.0, brightness));
    }

    let player_text = format!(
        "{} PLAYER{} IN GAME",
        player_count,
        if player_count == 1 { "" } else { "S" }
    );
    let player_text_width = measure_text(&player_text, None, 17, 1.0).width;
    draw_text(
        &player_text,
        (width - player_text_width) * 0.5,
        52.0,
        17.0,
        Color::from_rgba(193, 207, 239, 255),
    );

    let title = "PAUSED";
    let title_width = measure_text(title, None, 48, 1.0).width;
    draw_text(
        title,
        (width - title_width) * 0.5,
        height * 0.32,
        48.0,
        WHITE,
    );

    let button_width = 300.0_f32.min(width - 40.0).max(200.0);
    let button_height = 52.0;
    let button_gap = 14.0;
    let buttons_top = height * 0.4;
    let mouse = vec2(mouse_position().0, mouse_position().1);
    let buttons = [
        (
            PauseAction::Leave,
            "LEAVE GAME",
            Color::from_rgba(169, 72, 90, 245),
        ),
        (
            PauseAction::Restart,
            "RESTART GAME",
            Color::from_rgba(68, 92, 159, 245),
        ),
        (
            PauseAction::Resume,
            "GO BACK TO GAME",
            Color::from_rgba(69, 143, 119, 245),
        ),
    ];
    let mut action = None;
    for (index, (button_action, label, base_color)) in buttons.into_iter().enumerate() {
        let rect = Rect::new(
            (width - button_width) * 0.5,
            buttons_top + index as f32 * (button_height + button_gap),
            button_width,
            button_height,
        );
        let hovered = rect.contains(mouse);
        draw_rectangle(
            rect.x,
            rect.y,
            rect.w,
            rect.h,
            if hovered {
                Color::new(
                    (base_color.r + 0.12).min(1.0),
                    (base_color.g + 0.12).min(1.0),
                    (base_color.b + 0.12).min(1.0),
                    base_color.a,
                )
            } else {
                base_color
            },
        );
        draw_rectangle_lines(
            rect.x,
            rect.y,
            rect.w,
            rect.h,
            1.0,
            Color::new(1.0, 1.0, 1.0, if hovered { 0.45 } else { 0.16 }),
        );
        let label_width = measure_text(label, None, 18, 1.0).width;
        draw_text(
            label,
            rect.x + (rect.w - label_width) * 0.5,
            rect.y + 33.0,
            18.0,
            WHITE,
        );
        if hovered && is_mouse_button_pressed(MouseButton::Left) {
            action = Some(button_action);
        }
    }
    action
}

fn draw_hub(
    username: &str,
    published_games: &[PublishedGame],
    catalog_status: &str,
    selected_game: &mut usize,
    list_offset: &mut usize,
    hub_search: &mut HubSearch,
    avatar_model: &AvatarModel,
    accessory_model: &AvatarAccessories,
    body_color: Color,
    equipped_accessory: Option<AccessoryId>,
    is_guest: bool,
    guest_avatar: &GuestAvatarModel,
) -> Option<HubAction> {
    let width = screen_width();
    let height = screen_height();
    let panel_x = width * 0.43;
    let panel_w = width - panel_x;
    let pale = Color::from_rgba(238, 241, 255, 255);
    let mouse = vec2(mouse_position().0, mouse_position().1);

    draw_rectangle(0.0, 0.0, width, height, Color::from_rgba(4, 5, 10, 255));
    draw_moving_stars(150, 10.0);
    draw_rectangle(panel_x, 0.0, panel_w, height, pale);

    let header_h = (height * 0.18).clamp(90.0, 126.0);
    let tab_y = 14.0;
    let tab_h = (header_h - 28.0).max(58.0);
    let games_tab = Rect::new(panel_x + 22.0, tab_y, 132.0, tab_h);
    let avatar_tab = Rect::new(panel_x + 164.0, tab_y, 132.0, tab_h);
    draw_rectangle(
        games_tab.x,
        games_tab.y,
        games_tab.w,
        games_tab.h,
        Color::from_rgba(17, 18, 22, 255),
    );
    draw_text(
        "Games",
        games_tab.x + 15.0,
        games_tab.y + tab_h * 0.62,
        27.0,
        WHITE,
    );
    if is_guest {
        draw_rectangle(
            avatar_tab.x,
            avatar_tab.y,
            avatar_tab.w,
            avatar_tab.h,
            Color::from_rgba(75, 82, 101, 255),
        );
        draw_text(
            "GUEST",
            avatar_tab.x + 15.0,
            avatar_tab.y + tab_h * 0.58,
            24.0,
            Color::from_rgba(216, 226, 246, 255),
        );
    } else {
        draw_rectangle(
            avatar_tab.x,
            avatar_tab.y,
            avatar_tab.w,
            avatar_tab.h,
            Color::from_rgba(145, 147, 153, 255),
        );
        draw_text(
            "avatar",
            avatar_tab.x + 8.0,
            avatar_tab.y + tab_h * 0.58,
            24.0,
            Color::from_rgba(248, 248, 255, 255),
        );
        draw_rectangle(
            avatar_tab.x + 12.0,
            avatar_tab.y + tab_h - 26.0,
            28.0,
            20.0,
            Color::from_rgba(109, 111, 118, 255),
        );
        draw_rectangle(
            avatar_tab.x + 88.0,
            avatar_tab.y + tab_h - 26.0,
            28.0,
            20.0,
            Color::from_rgba(109, 111, 118, 255),
        );
    }

    let profile_center = vec2(width - 58.0, 48.0);
    let profile_rect = Rect::new(profile_center.x - 43.0, 5.0, 86.0, 86.0);
    draw_circle(
        profile_center.x,
        profile_center.y,
        43.0,
        Color::from_rgba(9, 14, 35, 255),
    );
    let avatar_viewport = Rect::new(
        profile_rect.x + 5.0,
        profile_rect.y + 5.0,
        profile_rect.w - 10.0,
        profile_rect.h - 10.0,
    );
    let avatar_camera = Camera3D {
        position: vec3(0.1, 2.1, 4.0),
        target: vec3(0.0, 1.05, 0.0),
        up: vec3(0.0, 1.0, 0.0),
        fovy: 34.0_f32.to_radians(),
        viewport: Some((
            avatar_viewport.x.round() as i32,
            avatar_viewport.y.round() as i32,
            avatar_viewport.w.round() as i32,
            avatar_viewport.h.round() as i32,
        )),
        ..Default::default()
    };
    set_camera(&avatar_camera);
    if is_guest {
        draw_guest_avatar(guest_avatar, Vec3::ZERO, 0.0);
    } else {
        draw_avatar(
            avatar_model,
            Vec3::ZERO,
            body_color,
            equipped_accessory,
            accessory_model,
            0.0,
            0.0,
            0.0,
        );
    }
    set_default_camera();
    draw_circle_lines(
        profile_center.x,
        profile_center.y,
        43.0,
        2.0,
        Color::from_rgba(5, 6, 10, 255),
    );
    let welcome = username.chars().take(16).collect::<String>();
    let welcome_width = measure_text(&welcome, None, 13, 1.0).width;
    draw_text(
        &welcome,
        (profile_rect.x - welcome_width - 12.0).max(panel_x + 8.0),
        profile_center.y + 5.0,
        13.0,
        Color::from_rgba(62, 66, 80, 255),
    );
    if is_guest {
        draw_text(
            "guest account - local play",
            panel_x + 18.0,
            profile_center.y + 26.0,
            11.0,
            Color::from_rgba(91, 95, 108, 255),
        );
    }

    let list_x = panel_x + 10.0;
    let list_y = header_h + 15.0;
    let list_w = (panel_w * 0.4).clamp(152.0, 300.0).min(panel_w - 20.0);
    let row_h = 31.0;
    let list_h = (height * 0.49)
        .min(height - list_y - 135.0)
        .max(row_h * 3.0);
    draw_rectangle(
        list_x - 5.0,
        list_y - 5.0,
        list_w + 10.0,
        list_h + 10.0,
        Color::from_rgba(25, 27, 32, 255),
    );
    draw_rectangle(
        list_x,
        list_y,
        list_w,
        list_h,
        Color::from_rgba(0, 0, 0, 255),
    );

    let total_games = HUB_EXPERIENCES.len() + published_games.len();
    let visible_rows = ((list_h - 12.0) / row_h).floor().max(1.0) as usize;
    *selected_game = (*selected_game).min(total_games.saturating_sub(1));
    *list_offset = (*list_offset).min(total_games.saturating_sub(visible_rows));
    if Rect::new(list_x, list_y, list_w, list_h).contains(mouse) {
        if mouse_wheel().1 > 0.0 {
            *list_offset = list_offset.saturating_sub(1);
        } else if mouse_wheel().1 < 0.0 {
            *list_offset = (*list_offset + 1).min(total_games.saturating_sub(visible_rows));
        }
    }
    if *selected_game < *list_offset {
        *list_offset = *selected_game;
    } else if *selected_game >= *list_offset + visible_rows {
        *list_offset =
            (*selected_game + 1 - visible_rows).min(total_games.saturating_sub(visible_rows));
    }

    for visible_index in 0..visible_rows {
        let game_index = *list_offset + visible_index;
        if game_index >= total_games {
            break;
        }
        let row = Rect::new(
            list_x + 7.0,
            list_y + 7.0 + visible_index as f32 * row_h,
            list_w - 14.0,
            row_h,
        );
        let title = if let Some(experience) = HUB_EXPERIENCES.get(game_index) {
            experience.title.to_owned()
        } else {
            published_games[game_index - HUB_EXPERIENCES.len()]
                .title
                .clone()
        };
        if game_index == *selected_game {
            draw_rectangle(
                row.x,
                row.y,
                row.w,
                row.h,
                Color::from_rgba(45, 49, 61, 255),
            );
        }
        let title_size = 18_u16;
        let mut shown_title = title;
        let mut title_clipped = false;
        while measure_text(&shown_title, None, title_size, 1.0).width > row.w - 10.0
            && shown_title.chars().count() > 3
        {
            shown_title.pop();
            title_clipped = true;
        }
        if title_clipped {
            shown_title.push_str("...");
        }
        draw_text(
            &shown_title,
            row.x + 7.0,
            row.y + 21.0,
            title_size as f32,
            Color::from_rgba(244, 245, 250, 255),
        );
        if row.contains(mouse) {
            if let Some(experience) = HUB_EXPERIENCES.get(game_index) {
                let tooltip = experience.description;
                let tooltip_width = measure_text(tooltip, None, 14, 1.0).width;
                let tooltip_x = (mouse.x + 14.0).min(width - tooltip_width - 16.0);
                let tooltip_y = (mouse.y + 22.0).min(height - 18.0);
                draw_text(
                    tooltip,
                    tooltip_x.max(8.0),
                    tooltip_y.max(18.0),
                    14.0,
                    Color::from_rgba(52, 57, 74, 255),
                );
            }
        }
        if is_mouse_button_pressed(MouseButton::Left) && row.contains(mouse) {
            *selected_game = game_index;
        }
    }
    if total_games > visible_rows {
        draw_text(
            "Scroll for more games",
            list_x,
            list_y + list_h + 24.0,
            13.0,
            Color::from_rgba(66, 70, 84, 255),
        );
    }

    let refresh_rect = Rect::new(list_x, list_y + list_h + 30.0, 92.0, 30.0);
    if !is_guest {
        draw_studio_button(
            refresh_rect,
            "REFRESH",
            Color::from_rgba(73, 77, 90, 255),
            1.0,
        );
        if !catalog_status.is_empty() {
            draw_text(
                &catalog_status.chars().take(30).collect::<String>(),
                refresh_rect.x,
                refresh_rect.y + 48.0,
                12.0,
                Color::from_rgba(150, 56, 56, 255),
            );
        }
    }
    let search_rect = Rect::new(panel_x + 310.0, tab_y, 96.0, tab_h);
    let wallet_rect = Rect::new(panel_x + 414.0, tab_y, 96.0, tab_h);
    if !is_guest {
        draw_studio_button(
            wallet_rect,
            "WALLET",
            Color::from_rgba(214, 150, 20, 255),
            1.0,
        );
    }
    draw_studio_button(
        search_rect,
        "SEARCH",
        Color::from_rgba(57, 95, 180, 255),
        1.0,
    );

    let footer_y = height * 0.79;
    draw_rectangle(
        0.0,
        footer_y,
        width * 0.88,
        height - footer_y,
        Color::from_rgba(5, 6, 10, 255),
    );
    let join_rect = Rect::new(
        width * 0.045,
        footer_y + 10.0,
        (width * 0.37).min(360.0),
        (height - footer_y - 20.0).max(48.0),
    );
    draw_rectangle(
        join_rect.x,
        join_rect.y,
        join_rect.w,
        join_rect.h,
        Color::from_rgba(58, 205, 93, 255),
    );
    draw_text(
        "▶",
        join_rect.x + join_rect.w * 0.27,
        join_rect.y + join_rect.h * 0.65,
        28.0,
        Color::from_rgba(225, 255, 232, 255),
    );
    draw_text(
        "Join",
        join_rect.x + join_rect.w * 0.44,
        join_rect.y + join_rect.h * 0.63,
        28.0,
        Color::from_rgba(225, 255, 232, 255),
    );

    let studio_rect = Rect::new(panel_x + 20.0, height - 54.0, 160.0, 36.0);
    if !is_guest {
        draw_studio_button(
            studio_rect,
            "CREATE GAME  +",
            Color::from_rgba(83, 74, 140, 255),
            1.0,
        );
    }

    let logout_rect = Rect::new(panel_x + 190.0, height - 54.0, 110.0, 36.0);
    draw_studio_button(logout_rect, "LOG OUT", Color::from_rgba(150, 56, 56, 255), 1.0);

    if is_mouse_button_pressed(MouseButton::Left) {
        if logout_rect.contains(mouse) {
            return Some(HubAction::LogOut);
        } else if search_rect.contains(mouse) {
            hub_search.open = true;
            hub_search.focused = false;
            hub_search.status.clear();
        } else if !is_guest && wallet_rect.contains(mouse) {
            return Some(HubAction::Wallet);
        } else if !is_guest && (avatar_tab.contains(mouse) || profile_rect.contains(mouse)) {
            return Some(HubAction::Customize);
        } else if !is_guest && refresh_rect.contains(mouse) {
            return Some(HubAction::RefreshCatalog);
        } else if !is_guest && studio_rect.contains(mouse) {
            return Some(HubAction::Studio);
        } else if join_rect.contains(mouse) {
            if let Some(experience) = HUB_EXPERIENCES.get(*selected_game) {
                return Some(HubAction::Play(*experience));
            }
            if let Some(game) = published_games.get(*selected_game - HUB_EXPERIENCES.len()) {
                return Some(HubAction::PlayPublished(game.clone()));
            }
        }
    }
    if hub_search.open {
        return draw_hub_search(hub_search, published_games);
    }
    None
}

fn draw_hub_search(search: &mut HubSearch, published_games: &[PublishedGame]) -> Option<HubAction> {
    let width = screen_width();
    let height = screen_height();
    draw_rectangle(0.0, 0.0, width, height, Color::new(0.015, 0.02, 0.04, 0.82));
    let card_w = width.min(460.0);
    let card_h = 344.0;
    let card = Rect::new(
        (width - card_w) * 0.5,
        (height - card_h) * 0.5,
        card_w,
        card_h,
    );
    draw_rectangle(
        card.x,
        card.y,
        card.w,
        card.h,
        Color::from_rgba(21, 25, 40, 255),
    );
    draw_rectangle_lines(
        card.x,
        card.y,
        card.w,
        card.h,
        2.0,
        Color::from_rgba(99, 133, 225, 255),
    );
    draw_text("Search", card.x + 24.0, card.y + 42.0, 30.0, WHITE);
    let close_rect = Rect::new(card.x + card.w - 44.0, card.y + 12.0, 32.0, 32.0);
    draw_rectangle(
        close_rect.x,
        close_rect.y,
        close_rect.w,
        close_rect.h,
        Color::from_rgba(62, 68, 88, 255),
    );
    draw_text("X", close_rect.x + 10.0, close_rect.y + 22.0, 16.0, WHITE);

    let mouse = vec2(mouse_position().0, mouse_position().1);
    let category_y = card.y + 60.0;
    let category_w = (card.w - 60.0) * 0.5;
    let users_rect = Rect::new(card.x + 20.0, category_y, category_w, 36.0);
    let games_rect = Rect::new(card.x + 40.0 + category_w, category_y, category_w, 36.0);
    for (rect, label, selected) in [
        (
            users_rect,
            "USERS",
            search.category == HubSearchCategory::Users,
        ),
        (
            games_rect,
            "GAMES",
            search.category == HubSearchCategory::Games,
        ),
    ] {
        draw_rectangle(
            rect.x,
            rect.y,
            rect.w,
            rect.h,
            if selected {
                Color::from_rgba(67, 101, 189, 255)
            } else {
                Color::from_rgba(45, 50, 68, 255)
            },
        );
        draw_text(label, rect.x + 14.0, rect.y + 24.0, 16.0, WHITE);
        if is_mouse_button_pressed(MouseButton::Left) && rect.contains(mouse) {
            search.category = if label == "USERS" {
                HubSearchCategory::Users
            } else {
                HubSearchCategory::Games
            };
            search.status.clear();
        }
    }

    let input_rect = Rect::new(card.x + 20.0, card.y + 112.0, card.w - 40.0, 42.0);
    draw_rectangle(
        input_rect.x,
        input_rect.y,
        input_rect.w,
        input_rect.h,
        Color::from_rgba(8, 10, 17, 255),
    );
    draw_rectangle_lines(
        input_rect.x,
        input_rect.y,
        input_rect.w,
        input_rect.h,
        1.0,
        if search.focused {
            Color::from_rgba(107, 147, 255, 255)
        } else {
            Color::from_rgba(88, 94, 116, 255)
        },
    );
    if is_mouse_button_pressed(MouseButton::Left) {
        search.focused = input_rect.contains(mouse);
    }
    if search.focused {
        while let Some(character) = get_char_pressed() {
            if !character.is_control() && search.query.chars().count() < 40 {
                search.query.push(character);
                search.status.clear();
            }
        }
        if is_key_pressed(KeyCode::Backspace) {
            search.query.pop();
            search.status.clear();
        }
    }
    let placeholder = match search.category {
        HubSearchCategory::Users => "Type a username",
        HubSearchCategory::Games => "Type a game name",
    };
    let shown_query = if search.query.is_empty() {
        placeholder
    } else {
        &search.query
    };
    draw_text(
        shown_query,
        input_rect.x + 12.0,
        input_rect.y + 27.0,
        17.0,
        if search.query.is_empty() {
            Color::from_rgba(133, 139, 158, 255)
        } else {
            WHITE
        },
    );

    let submit_rect = Rect::new(card.x + 20.0, card.y + 174.0, 126.0, 38.0);
    draw_rectangle(
        submit_rect.x,
        submit_rect.y,
        submit_rect.w,
        submit_rect.h,
        Color::from_rgba(43, 165, 90, 255),
    );
    draw_text(
        "SEARCH",
        submit_rect.x + 25.0,
        submit_rect.y + 25.0,
        16.0,
        WHITE,
    );
    let close_clicked = is_mouse_button_pressed(MouseButton::Left) && close_rect.contains(mouse);
    if close_clicked {
        search.open = false;
        search.focused = false;
        return None;
    }
    let submitted = is_mouse_button_pressed(MouseButton::Left) && submit_rect.contains(mouse)
        || search.focused && is_key_pressed(KeyCode::Enter);
    if submitted {
        let query = search.query.trim().to_owned();
        search.focused = false;
        if query.is_empty() {
            search.status = "Type a name to search.".to_owned();
        } else {
            search.status.clear();
            return Some(match search.category {
                HubSearchCategory::Users => HubAction::SearchUsername(query),
                HubSearchCategory::Games => HubAction::SearchGames(query),
            });
        }
    }
    if !search.status.is_empty() {
        let status_size = 16.0;
        let mut shown_status = search.status.clone();
        while measure_text(&shown_status, None, status_size as u16, 1.0).width > card.w - 40.0
            && shown_status.chars().count() > 6
        {
            shown_status.pop();
        }
        draw_text(
            &shown_status,
            card.x + 20.0,
            card.y + 236.0,
            status_size,
            if search.status.starts_with("Search failed:") {
                Color::from_rgba(248, 154, 128, 255)
            } else if search.status.starts_with("Sorry") {
                Color::from_rgba(255, 183, 157, 255)
            } else {
                Color::from_rgba(168, 233, 181, 255)
            },
        );
    }
    if search.category == HubSearchCategory::Games && !search.query.trim().is_empty() {
        let matches = search_game_titles(&search.query, published_games);
        if matches.is_empty() && search.status.is_empty() {
            draw_text(
                "No matching games in this catalog.",
                card.x + 20.0,
                card.y + 264.0,
                15.0,
                Color::from_rgba(192, 197, 211, 255),
            );
        } else {
            for (index, title) in matches.iter().enumerate() {
                let mut shown_title = title.clone();
                while measure_text(&shown_title, None, 15, 1.0).width > card.w - 40.0
                    && shown_title.chars().count() > 3
                {
                    shown_title.pop();
                }
                draw_text(
                    &shown_title,
                    card.x + 20.0,
                    card.y + 260.0 + index as f32 * 18.0,
                    15.0,
                    Color::from_rgba(192, 219, 255, 255),
                );
            }
        }
    }
    None
}

fn draw_avatar_page(
    avatar_model: &AvatarModel,
    accessory_model: &AvatarAccessories,
    body_color: &mut Color,
    equipped_accessory: &mut Option<AccessoryId>,
    color_status: &mut String,
    accessory_status: &mut String,
    catalog_page: &mut bool,
    owned: &[String],
) -> bool {
    let width = screen_width();
    let height = screen_height();
    let card_w = (width - 32.0).min(520.0);
    let card_h = (height - 32.0).min(700.0);
    let card_x = (width - card_w) * 0.5;
    let card_y = (height - card_h) * 0.5;
    clear_background(Color::from_rgba(4, 5, 12, 255));
    draw_moving_stars(125, 8.0);
    draw_rectangle(
        card_x,
        card_y,
        card_w,
        card_h,
        Color::from_rgba(14, 54, 176, 255),
    );
    draw_rectangle(
        card_x,
        card_y,
        card_w,
        card_h * 0.56,
        Color::new(0.08, 0.22, 0.82, 0.28),
    );
    draw_rectangle_lines(
        card_x,
        card_y,
        card_w,
        card_h,
        1.0,
        Color::from_rgba(63, 112, 255, 255),
    );
    let title = "Avatar";
    let title_width = measure_text(title, None, 38, 1.0).width;
    draw_text(
        title,
        card_x + (card_w - title_width) * 0.5,
        card_y + 64.0,
        38.0,
        Color::from_rgba(248, 249, 255, 255),
    );
    let equipped_name = equipped_accessory.map_or("No gear equipped", |item| item.name());
    let subtitle = format!("Currently wearing: {equipped_name}");
    let subtitle_width = measure_text(&subtitle, None, 14, 1.0).width;
    draw_text(
        &subtitle,
        card_x + (card_w - subtitle_width) * 0.5,
        card_y + 88.0,
        14.0,
        Color::from_rgba(202, 216, 255, 255),
    );

    let camera = Camera3D {
        position: vec3(3.4, 3.2, 5.8),
        target: vec3(0.0, 1.0, 0.0),
        up: vec3(0.0, 1.0, 0.0),
        fovy: AVATAR_PREVIEW_FOVY,
        ..Default::default()
    };
    set_camera(&camera);
    draw_avatar(
        avatar_model,
        vec3(0.0, 0.0, 0.0),
        *body_color,
        *equipped_accessory,
        accessory_model,
        0.0,
        0.0,
        0.0,
    );
    set_default_camera();

    let panel_x = card_x + 22.0;
    let mouse = vec2(mouse_position().0, mouse_position().1);
    let controls_y = card_y + card_h - 204.0;
    let color_tab = Rect::new(card_x + card_w * 0.5 - 126.0, controls_y, 116.0, 36.0);
    let catalog_tab = Rect::new(card_x + card_w * 0.5 + 10.0, controls_y, 132.0, 36.0);
    draw_rectangle(
        color_tab.x,
        color_tab.y,
        color_tab.w,
        color_tab.h,
        if !*catalog_page {
            Color::from_rgba(72, 102, 226, 255)
        } else {
            Color::from_rgba(43, 53, 77, 255)
        },
    );
    draw_text(
        "COLORS",
        color_tab.x + 29.0,
        color_tab.y + 24.0,
        15.0,
        WHITE,
    );
    draw_rectangle(
        catalog_tab.x,
        catalog_tab.y,
        catalog_tab.w,
        catalog_tab.h,
        if *catalog_page {
            Color::from_rgba(72, 102, 226, 255)
        } else {
            Color::from_rgba(43, 53, 77, 255)
        },
    );
    draw_text(
        "CATALOG",
        catalog_tab.x + 29.0,
        catalog_tab.y + 24.0,
        15.0,
        WHITE,
    );
    if is_mouse_button_pressed(MouseButton::Left) {
        if color_tab.contains(mouse) {
            *catalog_page = false;
        } else if catalog_tab.contains(mouse) {
            *catalog_page = true;
        }
    }

    if *catalog_page {
        draw_text("ACCESSORY CATALOG", panel_x, controls_y + 57.0, 16.0, WHITE);
        for (index, accessory) in AccessoryId::ALL.iter().copied().enumerate() {
            let row = Rect::new(
                panel_x,
                controls_y + 66.0 + index as f32 * 40.0,
                card_w - 44.0,
                36.0,
            );
            draw_rectangle(
                row.x,
                row.y,
                row.w,
                row.h,
                Color::from_rgba(32, 40, 61, 255),
            );
            draw_text(accessory.name(), row.x + 12.0, row.y + 23.0, 15.0, WHITE);
            draw_text(
                match accessory {
                    AccessoryId::RedHair => "Hair",
                    AccessoryId::TrafficCone => "Hat",
                    AccessoryId::Sword => "Back gear",
                },
                row.x + row.w * 0.48,
                row.y + 23.0,
                12.0,
                Color::from_rgba(183, 195, 225, 255),
            );
            let selected = *equipped_accessory == Some(accessory);
            let unlocked = owned.iter().any(|id| id == accessory.wire_id());
            let action = Rect::new(row.x + row.w - 88.0, row.y + 4.0, 78.0, 28.0);
            draw_rectangle(
                action.x,
                action.y,
                action.w,
                action.h,
                if !unlocked {
                    Color::from_rgba(90, 94, 104, 255)
                } else if selected {
                    Color::from_rgba(89, 67, 105, 255)
                } else {
                    Color::from_rgba(56, 98, 143, 255)
                },
            );
            draw_text(
                if !unlocked {
                    "LOCKED"
                } else if selected {
                    "UNEQUIP"
                } else {
                    "EQUIP"
                },
                action.x + 7.0,
                action.y + 19.0,
                12.0,
                WHITE,
            );
            if is_mouse_button_pressed(MouseButton::Left) && action.contains(mouse) && !unlocked {
                *accessory_status =
                    format!("Buy {} in the Wallet shop first.", accessory.name());
            } else if is_mouse_button_pressed(MouseButton::Left) && action.contains(mouse) {
                *equipped_accessory = if selected { None } else { Some(accessory) };
                match save_avatar_accessory(*equipped_accessory) {
                    Ok(()) => {
                        *accessory_status = match *equipped_accessory {
                            Some(equipped) => format!("{} equipped and saved.", equipped.name()),
                            None => "Accessory removed and saved.".to_owned(),
                        };
                    }
                    Err(error) => {
                        *accessory_status = format!("Could not save accessory: {error}");
                    }
                }
            }
        }
        draw_text(
            &accessory_status.chars().take(52).collect::<String>(),
            panel_x,
            card_y + card_h - 18.0,
            12.0,
            Color::from_rgba(222, 230, 250, 255),
        );
    } else {
        draw_text("BODY COLORS", panel_x, controls_y + 57.0, 16.0, WHITE);
        let mut changed = false;
        for (index, color) in BODY_COLORS.iter().enumerate() {
            let rect = Rect::new(
                card_x + card_w * 0.5 - 116.0 + (index % 4) as f32 * 58.0,
                controls_y + 72.0 + (index / 4) as f32 * 48.0,
                40.0,
                40.0,
            );
            draw_rectangle(rect.x, rect.y, rect.w, rect.h, *color);
            if colors_equal(*color, *body_color) {
                draw_rectangle_lines(
                    rect.x - 3.0,
                    rect.y - 3.0,
                    rect.w + 6.0,
                    rect.h + 6.0,
                    3.0,
                    WHITE,
                );
            }
            if is_mouse_button_pressed(MouseButton::Left) && rect.contains(mouse) {
                *body_color = *color;
                changed = true;
            }
        }
        if changed {
            match save_body_color(*body_color) {
                Ok(()) => *color_status = "Avatar color saved.".to_owned(),
                Err(error) => *color_status = format!("Could not save avatar: {error}"),
            }
        }
        draw_text(
            &color_status.chars().take(52).collect::<String>(),
            panel_x,
            controls_y + 184.0,
            12.0,
            Color::from_rgba(222, 230, 250, 255),
        );
    }

    let back_rect = Rect::new(card_x + 16.0, card_y + 15.0, 86.0, 30.0);
    draw_rectangle(
        back_rect.x,
        back_rect.y,
        back_rect.w,
        back_rect.h,
        Color::from_rgba(20, 39, 115, 255),
    );
    draw_text(
        "← BACK",
        back_rect.x + 13.0,
        back_rect.y + 21.0,
        13.0,
        WHITE,
    );
    is_key_pressed(KeyCode::Escape)
        || (is_mouse_button_pressed(MouseButton::Left) && back_rect.contains(mouse))
}

fn start_network(base_url: &str, token: &str, slug: &str) -> NetworkSession {
    let ws_base = base_url
        .replacen("https://", "wss://", 1)
        .replacen("http://", "ws://", 1)
        .trim_end_matches('/')
        .to_owned();
    let url = format!("{ws_base}/ws/{slug}");
    let token = token.to_owned();
    let (message_tx, mut message_rx) = tokio_mpsc::unbounded_channel::<ClientMessage>();
    let (event_tx, event_rx) = mpsc::channel();

    thread::spawn(move || {
        let runtime = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(runtime) => runtime,
            Err(error) => {
                let _ = event_tx.send(NetworkEvent::Status(format!("Network error: {error}")));
                return;
            }
        };
        runtime.block_on(async move {
            let mut request = match url.into_client_request() {
                Ok(request) => request,
                Err(error) => {
                    let _ = event_tx.send(NetworkEvent::Status(format!("Invalid server URL: {error}")));
                    return;
                }
            };
            let authorization = match HeaderValue::from_str(&format!("Bearer {token}")) {
                Ok(header) => header,
                Err(error) => {
                    let _ = event_tx.send(NetworkEvent::Status(format!("Invalid sign-in token: {error}")));
                    return;
                }
            };
            request.headers_mut().insert(AUTHORIZATION, authorization);
            match connect_async(request).await {
                Ok((socket, _)) => {
                    let _ = event_tx.send(NetworkEvent::Status("Online".to_owned()));
                    let (mut writer, mut reader) = socket.split();
                    loop {
                        tokio::select! {
                            message = message_rx.recv() => {
                                let Some(message) = message else { break };
                                let Ok(text) = serde_json::to_string(&message) else {
                                    let _ = event_tx.send(NetworkEvent::Status("Could not encode game message.".to_owned()));
                                    continue;
                                };
                                if writer.send(Message::Text(text.into())).await.is_err() {
                                    let _ = event_tx.send(NetworkEvent::Status("Connection lost.".to_owned()));
                                    break;
                                }
                            }
                            incoming = reader.next() => {
                                match incoming {
                                    Some(Ok(Message::Text(text))) => {
                                        if let Ok(message) = serde_json::from_str::<ServerMessage>(text.as_str()) {
                                            match message {
                                                ServerMessage::Peer { update } => {
                                                    let _ = event_tx.send(NetworkEvent::Peer(update));
                                                }
                                                ServerMessage::Chat { username, text } => {
                                                    let _ = event_tx.send(NetworkEvent::Chat(format!("{username}: {text}")));
                                                }
                                            }
                                        }
                                    }
                                    Some(Ok(Message::Close(_))) | None => {
                                        let _ = event_tx.send(NetworkEvent::Status("Disconnected.".to_owned()));
                                        break;
                                    }
                                    Some(Err(error)) => {
                                        let _ = event_tx.send(NetworkEvent::Status(format!("Connection error: {error}")));
                                        break;
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                }
                Err(error) => {
                    let _ = event_tx.send(NetworkEvent::Status(format!("Offline: {error}")));
                }
            }
        });
    });

    NetworkSession {
        message_tx,
        event_rx,
        peers: HashMap::new(),
        chat_messages: Vec::new(),
        status: "Connecting...".to_owned(),
        offline: false,
    }
}

fn guest_network() -> NetworkSession {
    let (message_tx, _message_rx) = tokio_mpsc::unbounded_channel();
    let (_event_tx, event_rx) = mpsc::channel();
    NetworkSession {
        message_tx,
        event_rx,
        peers: HashMap::new(),
        chat_messages: Vec::new(),
        status: "Offline guest game".to_owned(),
        offline: true,
    }
}

fn run_experience(
    experience: Experience,
    username: &str,
    body_color: &mut Color,
    equipped_accessory: &mut Option<AccessoryId>,
    car_state: &mut CarState,
    player: &mut Player,
    network: &mut NetworkSession,
    send_timer: &mut f32,
    chat_open: &mut bool,
    chat_input: &mut String,
    studio_blocks: &mut Vec<StudioBlock>,
    studio_status: &mut String,
    studio_color_index: &mut usize,
    studio_ui: &mut StudioUi,
    base_url: &str,
    token: &str,
    published_game: Option<&PublishedGame>,
    published_games: &mut Vec<PublishedGame>,
    avatar_menu_open: &mut bool,
    pause_menu_open: &mut bool,
    color_status: &mut String,
    accessory_status: &mut String,
    is_studio: bool,
    selected_tool: &mut PlayerTool,
    sword_swing_timer: &mut f32,
    camera_orbit: &mut CameraOrbit,
    avatar_model: &AvatarModel,
    accessory_model: &AvatarAccessories,
    driving_world: &DrivingWorld,
    npc_map: &NpcMap,
    classic_house_map: Option<&ClassicHouseMap>,
    is_guest: bool,
    guest_avatar: Option<&GuestAvatarModel>,
    delta: f32,
) -> bool {
    let delta = delta.clamp(0.0, 0.1);
    // Community games only have the Sword and Jump Coil when their maker allowed it.
    let tools_allowed =
        experience.slug != "my-first-game" || published_game.is_some_and(|game| game.tools_enabled);
    if !tools_allowed {
        *selected_tool = PlayerTool::Sword;
        *sword_swing_timer = 0.0;
    }
    if !is_studio && tools_allowed {
        if !*chat_open && !*avatar_menu_open && !*pause_menu_open && is_key_pressed(KeyCode::Key1) {
            *selected_tool = PlayerTool::Sword;
        } else if !*chat_open
            && !*avatar_menu_open
            && !*pause_menu_open
            && is_key_pressed(KeyCode::Key2)
        {
            *selected_tool = PlayerTool::JumpCoil;
        }
        *sword_swing_timer = (*sword_swing_timer - delta).max(0.0);
        if *selected_tool == PlayerTool::Sword
            && !*chat_open
            && !*avatar_menu_open
            && !*pause_menu_open
            && is_key_pressed(KeyCode::F)
        {
            *sword_swing_timer = 0.28;
        }
    }
    let pause_was_open = *pause_menu_open;
    if !is_studio && is_key_pressed(KeyCode::Escape) && !*chat_open && !*avatar_menu_open {
        *pause_menu_open = !*pause_menu_open;
    }
    if is_studio && studio_ui.publish_open && studio_ui.publish_focused {
        if is_key_pressed(KeyCode::Escape) || is_key_pressed(KeyCode::Enter) {
            studio_ui.publish_focused = false;
        } else if is_key_pressed(KeyCode::Backspace) {
            studio_ui.publish_name.pop();
        } else {
            while let Some(character) = get_char_pressed() {
                if !character.is_control() && studio_ui.publish_name.chars().count() < 40 {
                    studio_ui.publish_name.push(character);
                }
            }
        }
    }
    if !is_studio
        && !*chat_open
        && (is_key_pressed(KeyCode::Slash) || is_key_pressed(KeyCode::Enter))
    {
        *chat_open = true;
        chat_input.clear();
    }
    if *chat_open {
        if is_key_pressed(KeyCode::Escape) {
            *chat_open = false;
            chat_input.clear();
        } else if is_key_pressed(KeyCode::Enter) {
            send_chat(network, username, chat_input);
        } else if is_key_pressed(KeyCode::Backspace) {
            chat_input.pop();
        } else {
            while let Some(character) = get_char_pressed() {
                if !character.is_control() && chat_input.chars().count() < 120 {
                    chat_input.push(character);
                }
            }
        }
    }
    if *avatar_menu_open && is_key_pressed(KeyCode::Escape) {
        *avatar_menu_open = false;
    }

    if pause_was_open || *pause_menu_open {
        poll_network_events(network);
        draw_world(
            experience,
            avatar_model,
            *body_color,
            *equipped_accessory,
            accessory_model,
            driving_world,
            player,
            player.walk_phase,
            player.walk_amount,
            player.facing_yaw,
            camera_orbit,
            network,
            npc_map,
            classic_house_map,
            published_game.map_or(studio_blocks.as_slice(), |game| game.blocks.as_slice()),
            None,
            experience.slug == "my-first-game",
            true,
            tools_allowed && *selected_tool == PlayerTool::Sword,
            *sword_swing_timer,
            is_guest,
            guest_avatar,
        );
        if *pause_menu_open {
            match draw_pause_menu(network.peers.len() + 1) {
                Some(PauseAction::Leave) => return true,
                Some(PauseAction::Restart) => {
                    let is_driving = is_driving_experience(experience.slug);
                    delivery_reset();
                    player.position = if experience.slug == BOX_DELIVERY_SLUG {
                        Vec3::ZERO
                    } else if is_driving {
                        vec3(0.0, 0.0, DRIVE_ROAD_RADIUS_Z)
                    } else if experience.slug == "classic-house" {
                        vec3(8.0, 0.0, 35.0)
                    } else {
                        Vec3::ZERO
                    };
                    player.velocity = Vec3::ZERO;
                    player.grounded = true;
                    player.walk_phase = 0.0;
                    player.walk_amount = 0.0;
                    player.facing_yaw = if experience.slug == BOX_DELIVERY_SLUG {
                        0.0
                    } else if is_driving {
                        -std::f32::consts::FRAC_PI_2
                    } else {
                        0.0
                    };
                    car_state.speed = 0.0;
                    camera_orbit.yaw = player.facing_yaw + std::f32::consts::PI;
                    *send_timer = 0.0;
                    *pause_menu_open = false;
                }
                Some(PauseAction::Resume) => *pause_menu_open = false,
                None => {}
            }
        }
        return false;
    }

    if is_mouse_button_down(MouseButton::Right) && !*chat_open && !*avatar_menu_open {
        let mouse_delta = mouse_delta_position();
        camera_orbit.yaw += mouse_delta.x * CAMERA_DRAG_SENSITIVITY;
        camera_orbit.pitch = (camera_orbit.pitch + mouse_delta.y * CAMERA_DRAG_SENSITIVITY)
            .clamp(CAMERA_MIN_PITCH, CAMERA_MAX_PITCH);
    }

    let previous_position = player.position;
    let is_driving = is_driving_experience(experience.slug);
    let input_direction = Vec2::new(
        if *chat_open
            || (is_studio && studio_ui.publish_focused)
        {
            0.0
        } else {
            (if is_key_down(KeyCode::D) { 1.0 } else { 0.0 })
                - (if is_key_down(KeyCode::A) { 1.0 } else { 0.0 })
        },
        if *chat_open
            || (is_studio && studio_ui.publish_focused)
        {
            0.0
        } else {
            (if is_key_down(KeyCode::W) { 1.0 } else { 0.0 })
                - (if is_key_down(KeyCode::S) { 1.0 } else { 0.0 })
        },
    )
    .normalize_or_zero();
    if is_driving {
        update_car_drive(
            player,
            car_state,
            delta,
            !*chat_open,
            experience.slug == BOX_DELIVERY_SLUG,
        );
        if experience.slug == BOX_DELIVERY_SLUG {
            delivery_update(player.position, delta);
        }
    } else if is_studio {
        fly_studio_camera(player, camera_orbit, delta, !*chat_open && !studio_ui.publish_focused);
    } else {
        let direction = camera_relative_direction(input_direction, camera_orbit.yaw);
        update_player_facing(player, direction, delta);
        let move_speed = 6.0;
        player.position.x += direction.x * move_speed * delta;
        player.position.z += direction.y * move_speed * delta;
        let limit = if experience.slug == OBBY_SLUG { 150.0 } else { 24.0 };
        player.position.x = player.position.x.clamp(-limit, limit);
        player.position.z = player.position.z.clamp(-limit, limit);
        let is_walking = direction.length_squared() > 0.0 && player.grounded;
        update_walk_animation(player, is_walking, delta);
    }

    if !is_driving && !is_studio && !*chat_open
        && player.grounded
        && is_key_pressed(KeyCode::Space)
    {
        player.velocity.y = if tools_allowed { jump_impulse(*selected_tool) } else { jump_impulse(PlayerTool::Sword) };
        player.grounded = false;
    }
    if !is_driving && !is_studio {
        update_player_vertical(experience.slug, player, delta);
        if experience.slug == "my-first-game" {
            let solid = published_game.map_or(studio_blocks.as_slice(), |game| game.blocks.as_slice());
            apply_block_collisions(solid, previous_position, player);
        }
    }

    if is_studio {
        studio_controls(
            studio_blocks,
            studio_status,
            studio_color_index,
            player,
            camera_orbit,
            studio_ui,
            base_url,
            token,
            published_games,
        );
    }

    poll_network_events(network);
    *send_timer += delta;
    if *send_timer >= 0.1 {
        *send_timer = 0.0;
        if !network.offline {
            let _ = network.message_tx.send(ClientMessage::Position {
                position: player.position.to_array(),
                body_color: [body_color.r, body_color.g, body_color.b],
                accessory: equipped_accessory.map(|accessory| accessory.wire_id().to_owned()),
            });
        }
    }
    draw_world(
        experience,
        avatar_model,
        *body_color,
        *equipped_accessory,
        accessory_model,
        driving_world,
        player,
        player.walk_phase,
        player.walk_amount,
        player.facing_yaw,
        camera_orbit,
        network,
        npc_map,
        classic_house_map,
        published_game.map_or(studio_blocks.as_slice(), |game| game.blocks.as_slice()),
        if is_studio {
            studio_ui.selected_block
        } else {
            None
        },
        is_studio || experience.slug == "my-first-game",
        !is_studio,
        !is_studio && tools_allowed && *selected_tool == PlayerTool::Sword,
        *sword_swing_timer,
        is_guest,
        guest_avatar,
    );
    if is_studio {
        draw_studio_overlay(
            studio_blocks,
            studio_status,
            *studio_color_index,
            player,
            camera_orbit,
            studio_ui,
        );
    } else {
        draw_game_overlay(
            experience,
            username,
            network,
            body_color,
            equipped_accessory,
            avatar_menu_open,
            color_status,
            accessory_status,
            chat_open,
            chat_input,
            published_game.map_or(experience.title, |game| game.title.as_str()),
            selected_tool,
            tools_allowed,
            100.0,
            is_guest,
        );
        if experience.slug == BOX_DELIVERY_SLUG {
            draw_delivery_hud(player.position, player.facing_yaw, car_state.speed);
        } else if is_driving {
            draw_drive_hud(car_state.speed);
        }
    }
    false
}

fn poll_network_events(network: &mut NetworkSession) {
    loop {
        match network.event_rx.try_recv() {
            Ok(NetworkEvent::Peer(update)) => {
                if update.left {
                    network.peers.remove(&update.id);
                } else {
                    network.peers.insert(update.id.clone(), update);
                }
            }
            Ok(NetworkEvent::Chat(message)) => push_chat(&mut network.chat_messages, message),
            Ok(NetworkEvent::Status(status)) => network.status = status,
            Err(_) => break,
        }
    }
}

fn update_car_drive(
    player: &mut Player,
    car: &mut CarState,
    delta: f32,
    controls_enabled: bool,
    city: bool,
) {
    let previous_position = player.position;
    let throttle =
        (if controls_enabled && (is_key_down(KeyCode::W) || is_key_down(KeyCode::Up)) {
            1.0
        } else {
            0.0
        }) - (if controls_enabled && (is_key_down(KeyCode::S) || is_key_down(KeyCode::Down)) {
            1.0
        } else {
            0.0
        });
    let steering =
        (if controls_enabled && (is_key_down(KeyCode::D) || is_key_down(KeyCode::Right)) {
            1.0
        } else {
            0.0
        }) - (if controls_enabled && (is_key_down(KeyCode::A) || is_key_down(KeyCode::Left)) {
            1.0
        } else {
            0.0
        });

    if throttle != 0.0 {
        car.speed += throttle * 23.0 * delta;
    } else {
        car.speed *= (1.0 - delta * 1.7).max(0.0);
    }
    car.speed = car.speed.clamp(-18.0, 58.0);
    if car.speed.abs() > 0.1 {
        let steering_power = (car.speed.abs() / 12.0).clamp(0.2, 1.0);
        player.facing_yaw = (player.facing_yaw
            + steering * car.speed.signum() * steering_power * 1.1 * delta)
            .rem_euclid(std::f32::consts::TAU);
    }
    let direction = vec2(player.facing_yaw.sin(), player.facing_yaw.cos());
    player.position.x += direction.x * car.speed * delta;
    player.position.z += direction.y * car.speed * delta;
    if city {
        player.position.x = player.position.x.clamp(-CITY_HALF, CITY_HALF);
        player.position.z = player.position.z.clamp(-CITY_HALF, CITY_HALF);
        if city_collides(player.position.x, player.position.z) {
            player.position = previous_position;
            car.speed *= -0.3;
        }
        player.position.y = 0.0;
        player.velocity = Vec3::ZERO;
        return;
    }
    player.position.x = player.position.x.clamp(
        -DRIVE_ISLAND_HALF_WIDTH + 20.0,
        DRIVE_ISLAND_HALF_WIDTH - 20.0,
    );
    player.position.z = player.position.z.clamp(
        -DRIVE_ISLAND_HALF_LENGTH + 20.0,
        DRIVE_ISLAND_HALF_LENGTH - 20.0,
    );
    player.position.y = 0.0;
    player.velocity = Vec3::ZERO;
}

fn draw_drive_hud(speed: f32) {
    let panel = Rect::new(screen_width() - 186.0, screen_height() - 112.0, 170.0, 96.0);
    draw_rectangle(
        panel.x,
        panel.y,
        panel.w,
        panel.h,
        Color::new(0.05, 0.09, 0.14, 0.88),
    );
    draw_text("COASTAL DRIVE", panel.x + 13.0, panel.y + 23.0, 16.0, WHITE);
    draw_text(
        &format!("{:03} km/h", (speed.abs() * 2.3) as u32),
        panel.x + 13.0,
        panel.y + 49.0,
        21.0,
        Color::from_rgba(116, 231, 232, 255),
    );
    draw_text(
        "WASD / Arrows  •  Esc exit",
        panel.x + 12.0,
        panel.y + 76.0,
        11.0,
        LIGHTGRAY,
    );
}

fn camera_relative_direction(input: Vec2, yaw: f32) -> Vec2 {
    let (sin_yaw, cos_yaw) = yaw.sin_cos();
    Vec2::new(
        input.x * cos_yaw - input.y * sin_yaw,
        -input.x * sin_yaw - input.y * cos_yaw,
    )
    .normalize_or_zero()
}

fn update_walk_animation(player: &mut Player, is_walking: bool, delta: f32) {
    if is_walking {
        player.walk_phase =
            (player.walk_phase + delta * 6.0 * 1.8).rem_euclid(std::f32::consts::TAU);
    }
    let gait_target = if is_walking { 1.0 } else { 0.0 };
    player.walk_amount += (gait_target - player.walk_amount) * (delta * 8.0).min(1.0);
}

fn update_player_facing(player: &mut Player, direction: Vec2, delta: f32) {
    if direction.length_squared() == 0.0 {
        return;
    }

    let target_yaw = direction.x.atan2(direction.y);
    let angle_difference = (target_yaw - player.facing_yaw + std::f32::consts::PI)
        .rem_euclid(std::f32::consts::TAU)
        - std::f32::consts::PI;
    player.facing_yaw = (player.facing_yaw + angle_difference * (delta * 12.0).min(1.0))
        .rem_euclid(std::f32::consts::TAU);
}

fn rotate_avatar_offset(offset: Vec3, yaw: f32) -> Vec3 {
    let (sin_yaw, cos_yaw) = yaw.sin_cos();
    vec3(
        offset.x * cos_yaw + offset.z * sin_yaw,
        offset.y,
        -offset.x * sin_yaw + offset.z * cos_yaw,
    )
}

fn update_player_vertical(experience_slug: &str, player: &mut Player, delta: f32) {
    let previous_y = player.position.y;
    player.velocity.y -= 20.0 * delta;
    player.position.y += player.velocity.y * delta;

    if experience_slug == "sky-sprint" {
        let landing_height = sky_platforms().into_iter().find_map(|(x, z, top)| {
            ((player.position.x - x).abs() < 2.0
                && (player.position.z - z).abs() < 2.0
                && player.velocity.y <= 0.0
                && previous_y >= top
                && player.position.y <= top)
                .then_some(top)
        });

        if let Some(height) = landing_height {
            player.position.y = height;
            player.velocity.y = 0.0;
            player.grounded = true;
        } else if player.position.y < -10.0 {
            player.position = Vec3::ZERO;
            player.velocity = Vec3::ZERO;
            player.grounded = true;
        } else {
            player.grounded = false;
        }
    } else if experience_slug == OBBY_SLUG {
        if let Some(height) = obby_landing_height(player.position, previous_y)
            .filter(|_| player.velocity.y <= 0.0)
        {
            player.position.y = height;
            player.velocity.y = 0.0;
            player.grounded = true;
        } else if player.position.y < OBBY_FALL_LIMIT {
            player.position = OBBY_SPAWN;
            player.velocity = Vec3::ZERO;
            player.grounded = true;
        } else {
            player.grounded = false;
        }
    } else if player.position.y <= 0.0 {
        player.position.y = 0.0;
        player.velocity.y = 0.0;
        player.grounded = true;
    }
}

fn send_chat(network: &mut NetworkSession, username: &str, input: &mut String) {
    let message = input.trim();
    if message.is_empty() {
        *input = String::new();
        return;
    }
    if network.offline {
        push_chat(&mut network.chat_messages, format!("{username}: {message}"));
    } else {
        match network.message_tx.send(ClientMessage::Chat {
            text: message.to_owned(),
        }) {
            Ok(()) => push_chat(&mut network.chat_messages, format!("{username}: {message}")),
            Err(error) => network.status = format!("Chat unavailable: {error}"),
        }
    }
    *input = String::new();
}

fn push_chat(messages: &mut Vec<String>, message: String) {
    messages.push(message);
    if messages.len() > 8 {
        messages.remove(0);
    }
}

fn report_studio_save(status: &mut String, blocks: &[StudioBlock]) {
    *status = match save_studio_blocks(blocks) {
        Ok(()) => format!("Saved {} blocks", blocks.len()),
        Err(error) => format!("Could not save studio: {error}"),
    };
}

fn sky_platforms() -> [(f32, f32, f32); 6] {
    [
        (0.0, 0.0, 0.0),
        (3.0, -3.0, 1.4),
        (6.0, -6.0, 2.8),
        (9.0, -9.0, 4.2),
        (12.0, -12.0, 5.6),
        (15.0, -15.0, 7.0),
    ]
}

const OBBY_SLUG: &str = "obby-rift";
const OBBY_SPAWN: Vec3 = Vec3::new(0.0, 1.4, 0.0);
const OBBY_FALL_LIMIT: f32 = -15.0;
const OBBY_EDGE_GRACE: f32 = 0.3;

#[derive(Clone, Copy)]
struct ObbySolid {
    min: Vec3,
    max: Vec3,
    color: Color,
    round: bool,
}

impl ObbySolid {
    fn contains_footprint(&self, x: f32, z: f32) -> bool {
        if self.round {
            let center_x = (self.min.x + self.max.x) * 0.5;
            let center_z = (self.min.z + self.max.z) * 0.5;
            let radius = (self.max.x - self.min.x) * 0.5 + OBBY_EDGE_GRACE;
            (x - center_x).hypot(z - center_z) <= radius
        } else {
            x >= self.min.x - OBBY_EDGE_GRACE
                && x <= self.max.x + OBBY_EDGE_GRACE
                && z >= self.min.z - OBBY_EDGE_GRACE
                && z <= self.max.z + OBBY_EDGE_GRACE
        }
    }
}

fn parse_obby_solids(source: &str) -> Vec<ObbySolid> {
    source
        .lines()
        .filter_map(|line| {
            let mut parts = line.split_whitespace();
            let round = match parts.next()? {
                "b" => false,
                "c" => true,
                _ => return None,
            };
            let values = parts
                .map(|part| part.parse::<f32>().ok())
                .collect::<Option<Vec<_>>>()?;
            if values.len() != 9 {
                return None;
            }
            Some(ObbySolid {
                min: vec3(values[0], values[1], values[2]),
                max: vec3(values[3], values[4], values[5]),
                color: Color::from_rgba(values[6] as u8, values[7] as u8, values[8] as u8, 255),
                round,
            })
        })
        .collect()
}

fn obby_solids() -> &'static [ObbySolid] {
    static SOLIDS: std::sync::OnceLock<Vec<ObbySolid>> = std::sync::OnceLock::new();
    SOLIDS.get_or_init(|| parse_obby_solids(include_str!("../assets/obby_map.txt")))
}

fn obby_landing_height(position: Vec3, previous_y: f32) -> Option<f32> {
    obby_solids()
        .iter()
        .filter(|solid| {
            solid.contains_footprint(position.x, position.z)
                && previous_y >= solid.max.y - 0.05
                && position.y <= solid.max.y
        })
        .map(|solid| solid.max.y)
        .max_by(f32::total_cmp)
}

fn push_obby_quad(vertices: &mut Vec<Vertex>, corners: [Vec3; 4], color: Color) {
    for index in [0, 1, 2, 0, 2, 3] {
        let corner = corners[index];
        vertices.push(Vertex::new(
            corner.x,
            corner.y,
            corner.z,
            0.0,
            0.0,
            color,
        ));
    }
}

fn shade(color: Color, factor: f32) -> Color {
    Color::new(color.r * factor, color.g * factor, color.b * factor, 1.0)
}

fn build_obby_vertices(solid: &ObbySolid, vertices: &mut Vec<Vertex>) {
    let (min, max) = (solid.min, solid.max);
    if solid.round {
        const SIDES: usize = 16;
        let center = vec3((min.x + max.x) * 0.5, 0.0, (min.z + max.z) * 0.5);
        let radius = (max.x - min.x) * 0.5;
        let ring = |index: usize, y: f32| {
            let angle = index as f32 / SIDES as f32 * std::f32::consts::TAU;
            vec3(
                center.x + angle.cos() * radius,
                y,
                center.z + angle.sin() * radius,
            )
        };
        for index in 0..SIDES {
            let side = shade(solid.color, 0.78 + 0.12 * (index as f32 * 0.8).sin());
            push_obby_quad(
                vertices,
                [
                    ring(index, min.y),
                    ring(index + 1, min.y),
                    ring(index + 1, max.y),
                    ring(index, max.y),
                ],
                side,
            );
            for (y, factor) in [(max.y, 1.0), (min.y, 0.55)] {
                let top_center = vec3(center.x, y, center.z);
                for corner in [top_center, ring(index, y), ring(index + 1, y)] {
                    vertices.push(Vertex::new(
                        corner.x,
                        corner.y,
                        corner.z,
                        0.0,
                        0.0,
                        shade(solid.color, factor),
                    ));
                }
            }
        }
        return;
    }
    let corner = |x: bool, y: bool, z: bool| {
        vec3(
            if x { max.x } else { min.x },
            if y { max.y } else { min.y },
            if z { max.z } else { min.z },
        )
    };
    let faces = [
        ([(false, true, false), (true, true, false), (true, true, true), (false, true, true)], 1.0),
        ([(false, false, false), (true, false, false), (true, false, true), (false, false, true)], 0.55),
        ([(false, false, false), (true, false, false), (true, true, false), (false, true, false)], 0.8),
        ([(false, false, true), (true, false, true), (true, true, true), (false, true, true)], 0.8),
        ([(false, false, false), (false, false, true), (false, true, true), (false, true, false)], 0.68),
        ([(true, false, false), (true, false, true), (true, true, true), (true, true, false)], 0.68),
    ];
    for (points, factor) in faces {
        push_obby_quad(
            vertices,
            points.map(|(x, y, z)| corner(x, y, z)),
            shade(solid.color, factor),
        );
    }
}

fn build_obby_meshes() -> Vec<Mesh> {
    let mut meshes = Vec::new();
    let mut vertices = Vec::new();
    let mut flush = |vertices: &mut Vec<Vertex>| {
        if !vertices.is_empty() {
            let indices = (0..vertices.len() as u16).collect();
            meshes.push(Mesh {
                vertices: std::mem::take(vertices),
                indices,
                texture: None,
            });
        }
    };
    for solid in obby_solids() {
        let mut solid_vertices = Vec::new();
        build_obby_vertices(solid, &mut solid_vertices);
        if vertices.len() + solid_vertices.len() > 4800 {
            flush(&mut vertices);
        }
        vertices.extend(solid_vertices);
    }
    flush(&mut vertices);
    meshes
}

fn draw_obby_map() {
    thread_local! {
        static OBBY_MESHES: Vec<Mesh> = build_obby_meshes();
    }
    draw_plane(
        vec3(20.0, OBBY_FALL_LIMIT, -20.0),
        vec2(400.0, 400.0),
        None,
        Color::from_rgba(52, 78, 128, 255),
    );
    OBBY_MESHES.with(|meshes| {
        for mesh in meshes {
            draw_mesh(mesh);
        }
    });
}

fn draw_world(
    experience: Experience,
    avatar_model: &AvatarModel,
    body_color: Color,
    equipped_accessory: Option<AccessoryId>,
    accessory_model: &AvatarAccessories,
    driving_world: &DrivingWorld,
    player: &Player,
    walk_phase: f32,
    walk_amount: f32,
    facing_yaw: f32,
    camera_orbit: &CameraOrbit,
    network: &NetworkSession,
    npc_map: &NpcMap,
    classic_house_map: Option<&ClassicHouseMap>,
    studio_blocks: &[StudioBlock],
    selected_block: Option<usize>,
    is_studio: bool,
    show_avatars: bool,
    show_held_sword: bool,
    sword_swing_timer: f32,
    is_guest: bool,
    guest_avatar: Option<&GuestAvatarModel>,
) {
    clear_background(Color::from_rgba(124, 169, 225, 255));
    let horizontal_distance = CAMERA_DISTANCE * camera_orbit.pitch.cos();
    let camera = Camera3D {
        position: player.position
            + vec3(
                camera_orbit.yaw.sin() * horizontal_distance,
                CAMERA_DISTANCE * camera_orbit.pitch.sin(),
                camera_orbit.yaw.cos() * horizontal_distance,
            ),
        target: player.position + vec3(0.0, 1.0, 0.0),
        up: vec3(0.0, 1.0, 0.0),
        fovy: WORLD_CAMERA_FOVY,
        ..Default::default()
    };
    set_camera(&camera);
    if is_studio {
        draw_studio_baseplate();
        draw_studio_blocks(studio_blocks, selected_block);
    } else {
        match experience.slug {
            BOX_DELIVERY_SLUG => draw_city_world(player.position),
            "coastal-drive" => {
                draw_driving_world(driving_world, player.position);
            }
            "block-bash" => {
                draw_plane(
                    vec3(0.0, -0.05, 0.0),
                    vec2(48.0, 48.0),
                    None,
                    Color::from_rgba(36, 75, 78, 255),
                );
                for x in -2_i32..=2 {
                    for z in -2_i32..=2 {
                        let height = 1.0 + ((x * 13 + z * 7).abs() % 4) as f32 * 0.65;
                        draw_beveled_block(
                            vec3(x as f32 * 3.0, height * 0.5, z as f32 * 3.0),
                            vec3(2.8, height, 2.8),
                            block_color(x, z),
                        );
                    }
                }
            }
            "sky-sprint" => {
                for (index, (x, z, top)) in sky_platforms().iter().enumerate() {
                    draw_beveled_block(
                        vec3(*x, *top - 0.2, *z),
                        vec3(3.0, 0.4, 3.0),
                        EXPERIENCES[index % EXPERIENCES.len()].accent,
                    );
                }
            }
            "npc-map" => draw_npc_map(npc_map),
            "classic-house" => draw_classic_house_map(classic_house_map),
            OBBY_SLUG => draw_obby_map(),
            _ => {
                draw_sunset_plaza();
            }
        }
    }

    if show_avatars {
        if is_driving_experience(experience.slug) {
            for peer in network.peers.values() {
                draw_car(
                    Vec3::from_array(peer.position),
                    0.0,
                    Color::new(
                        peer.body_color[0],
                        peer.body_color[1],
                        peer.body_color[2],
                        1.0,
                    ),
                );
            }
            draw_car(player.position, player.facing_yaw, body_color);
            if experience.slug == BOX_DELIVERY_SLUG && delivery_lock().carrying {
                draw_beveled_block(
                    player.position
                        + rotate_avatar_offset(vec3(0.0, 2.1, -0.6), player.facing_yaw),
                    vec3(1.7, 1.2, 1.7),
                    Color::from_rgba(196, 146, 84, 255),
                );
            }
        } else {
            for peer in network.peers.values() {
                let position = Vec3::from_array(peer.position);
                draw_avatar(
                    avatar_model,
                    position,
                    Color::new(
                        peer.body_color[0],
                        peer.body_color[1],
                        peer.body_color[2],
                        1.0,
                    ),
                    peer.accessory
                        .as_deref()
                        .and_then(AccessoryId::from_wire_id),
                    accessory_model,
                    0.0,
                    0.0,
                    0.0,
                );
            }
            if is_guest {
                if let Some(guest_avatar) = guest_avatar {
                    draw_guest_avatar(guest_avatar, player.position, facing_yaw);
                }
            } else {
                draw_avatar(
                    avatar_model,
                    player.position,
                    body_color,
                    equipped_accessory,
                    accessory_model,
                    walk_phase,
                    walk_amount,
                    facing_yaw,
                );
            }
            if show_held_sword {
                draw_held_sword(player.position, facing_yaw, sword_swing_timer);
            }
        }
    }
    set_default_camera();
}

fn draw_npc_map(npc_map: &NpcMap) {
    for mesh in &npc_map.meshes {
        draw_mesh(mesh);
    }
}

fn draw_classic_house_map(classic_house_map: Option<&ClassicHouseMap>) {
    draw_plane(
        vec3(0.0, -1.2, 0.0),
        vec2(200.0, 200.0),
        None,
        Color::from_rgba(58, 78, 65, 255),
    );
    if let Some(map) = classic_house_map {
        draw_mesh(&map.mesh);
    } else {
        draw_cube(
            vec3(0.0, 4.0, 0.0),
            vec3(12.0, 10.0, 12.0),
            None,
            Color::from_rgba(188, 170, 132, 255),
        );
        draw_cube(
            vec3(0.0, 10.0, 0.0),
            vec3(18.0, 2.0, 18.0),
            None,
            Color::from_rgba(138, 86, 61, 255),
        );
    }
}

fn draw_driving_world(world: &DrivingWorld, player_position: Vec3) {
    draw_plane(
        vec3(0.0, -1.2, 0.0),
        vec2(2600.0, 2400.0),
        None,
        Color::from_rgba(35, 139, 170, 255),
    );
    draw_plane(
        vec3(0.0, -0.65, 0.0),
        vec2(
            DRIVE_ISLAND_HALF_WIDTH * 2.0,
            DRIVE_ISLAND_HALF_LENGTH * 2.0,
        ),
        None,
        Color::from_rgba(75, 153, 91, 255),
    );
    for mesh in &world.road {
        draw_mesh(mesh);
    }

    for prop in &world.props {
        let position = match prop {
            DriveProp::Building { position, .. }
            | DriveProp::Tree { position, .. }
            | DriveProp::Hill { position, .. } => *position,
        };
        if (position - player_position).length_squared() > 310.0 * 310.0 {
            continue;
        }
        match prop {
            DriveProp::Building {
                position,
                size,
                color,
            } => {
                draw_cube(
                    *position + vec3(0.0, size.y * 0.5, 0.0),
                    *size,
                    None,
                    *color,
                );
                let window_color = Color::from_rgba(255, 220, 132, 230);
                draw_cube(
                    *position + vec3(0.0, size.y * 0.56, size.z * 0.5 + 0.012),
                    vec3(size.x * 0.72, size.y * 0.12, 0.025),
                    None,
                    window_color,
                );
                draw_cube(
                    *position + vec3(size.x * 0.5 + 0.012, size.y * 0.56, 0.0),
                    vec3(0.025, size.y * 0.12, size.z * 0.72),
                    None,
                    window_color,
                );
            }
            DriveProp::Tree { position, height } => {
                draw_cube(
                    *position + vec3(0.0, *height * 0.28, 0.0),
                    vec3(1.7, *height * 0.56, 1.7),
                    None,
                    Color::from_rgba(111, 77, 53, 255),
                );
                draw_beveled_block(
                    *position + vec3(0.0, *height * 0.76, 0.0),
                    vec3(*height * 0.54, *height * 0.62, *height * 0.54),
                    Color::from_rgba(36, 118, 71, 255),
                );
            }
            DriveProp::Hill {
                position,
                size,
                color,
            } => draw_beveled_block(*position, *size, *color),
        }
    }
}

fn draw_car(position: Vec3, yaw: f32, body_color: Color) {
    let parts = [
        (vec3(0.0, 0.68, 0.0), vec3(1.95, 0.48, 3.7), body_color),
        (
            vec3(0.0, 0.98, 1.08),
            vec3(1.82, 0.2, 1.25),
            shade_block_color(body_color, 1.12),
        ),
        (
            vec3(0.0, 1.13, -0.28),
            vec3(1.48, 0.62, 1.55),
            Color::from_rgba(88, 190, 217, 230),
        ),
        (
            vec3(0.0, 1.48, -0.34),
            vec3(1.24, 0.12, 1.22),
            shade_block_color(body_color, 0.8),
        ),
        (
            vec3(-1.0, 0.38, -1.15),
            vec3(0.32, 0.68, 0.72),
            Color::from_rgba(23, 27, 33, 255),
        ),
        (
            vec3(1.0, 0.38, -1.15),
            vec3(0.32, 0.68, 0.72),
            Color::from_rgba(23, 27, 33, 255),
        ),
        (
            vec3(-1.0, 0.38, 1.15),
            vec3(0.32, 0.68, 0.72),
            Color::from_rgba(23, 27, 33, 255),
        ),
        (
            vec3(1.0, 0.38, 1.15),
            vec3(0.32, 0.68, 0.72),
            Color::from_rgba(23, 27, 33, 255),
        ),
        (
            vec3(-0.61, 0.78, 1.88),
            vec3(0.38, 0.18, 0.08),
            Color::from_rgba(255, 240, 175, 255),
        ),
        (
            vec3(0.61, 0.78, 1.88),
            vec3(0.38, 0.18, 0.08),
            Color::from_rgba(255, 240, 175, 255),
        ),
    ];
    for (local_center, size, color) in parts {
        let center = position + rotate_avatar_offset(local_center, yaw);
        let mut mesh = beveled_block_mesh(center, size, color);
        for vertex in &mut mesh.vertices {
            let relative = vertex.position - center;
            vertex.position = center + rotate_avatar_offset(relative, yaw);
        }
        draw_mesh(&mesh);
    }
}

fn draw_avatar(
    model: &AvatarModel,
    position: Vec3,
    body_color: Color,
    equipped_accessory: Option<AccessoryId>,
    accessory_model: &AvatarAccessories,
    walk_phase: f32,
    walk_amount: f32,
    facing_yaw: f32,
) {
    let skin = Color::from_rgba(245, 195, 153, 255);
    let arm_swing = walk_phase.sin() * walk_amount * 0.55;
    let leg_swing = walk_phase.sin() * walk_amount * 0.4;
    let body_bob = walk_phase.sin().abs() * walk_amount * 0.035;

    for part in [
        AvatarPart::Torso,
        AvatarPart::RightArm,
        AvatarPart::LeftArm,
        AvatarPart::LeftLeg,
        AvatarPart::RightLeg,
        AvatarPart::Head,
    ] {
        let mut vertices = Vec::with_capacity(model.triangles[part.index()].len());
        let texture = &model.textures[part.index()];
        for (vertex_index, vertex) in model.triangles[part.index()].iter().enumerate() {
            let mut local = avatar_model_position(*vertex);
            match part {
                AvatarPart::RightArm => {
                    local = rotate_avatar_limb(local, vec3(0.5, 1.46, 0.0), arm_swing);
                }
                AvatarPart::LeftArm => {
                    local = rotate_avatar_limb(local, vec3(-0.5, 1.46, 0.0), -arm_swing);
                }
                AvatarPart::LeftLeg => {
                    local = rotate_avatar_limb(local, vec3(-0.2, 0.88, 0.0), -leg_swing);
                }
                AvatarPart::RightLeg => {
                    local = rotate_avatar_limb(local, vec3(0.2, 0.88, 0.0), leg_swing);
                }
                AvatarPart::Torso | AvatarPart::Head => {
                    local.y += body_bob;
                }
            }

            let world = position + rotate_avatar_offset(local, facing_yaw);
            let color = if texture.is_some() {
                WHITE
            } else {
                match part {
                    AvatarPart::Head => skin,
                    AvatarPart::LeftLeg | AvatarPart::RightLeg => {
                        Color::from_rgba(48, 91, 176, 255)
                    }
                    _ => body_color,
                }
            };
            vertices.push(Vertex::new2(
                world,
                model.texture_coordinates[part.index()][vertex_index],
                color,
            ));
        }
        let indices = (0..vertices.len()).map(|index| index as u16).collect();
        draw_mesh(&Mesh {
            vertices,
            indices,
            texture: texture.clone(),
        });
    }
    if let Some(accessory) = equipped_accessory {
        let index = accessory.index();
        let texture = if accessory == AccessoryId::RedHair {
            accessory_model.hair_texture.clone()
        } else {
            None
        };
        let color = match accessory {
            AccessoryId::RedHair if texture.is_some() => WHITE,
            AccessoryId::RedHair => Color::from_rgba(218, 44, 57, 255),
            AccessoryId::TrafficCone => Color::from_rgba(242, 85, 46, 255),
            AccessoryId::Sword => Color::from_rgba(186, 201, 220, 255),
        };
        let vertices = accessory_model.triangles[index]
            .iter()
            .enumerate()
            .map(|(vertex_index, vertex)| {
                let mut local = *vertex;
                local.y += body_bob;
                Vertex::new2(
                    position + rotate_avatar_offset(local, facing_yaw),
                    accessory_model.texture_coordinates[index][vertex_index],
                    color,
                )
            })
            .collect::<Vec<_>>();
        let indices = (0..vertices.len()).map(|index| index as u16).collect();
        draw_mesh(&Mesh {
            vertices,
            indices,
            texture,
        });
    }
}

fn draw_guest_avatar(model: &GuestAvatarModel, position: Vec3, facing_yaw: f32) {
    let vertices = model
        .mesh
        .vertices
        .iter()
        .map(|vertex| {
            Vertex::new2(
                position + rotate_avatar_offset(vertex.position, facing_yaw),
                vertex.uv,
                WHITE,
            )
        })
        .collect::<Vec<_>>();
    draw_mesh(&Mesh {
        vertices,
        indices: model.mesh.indices.clone(),
        texture: model.mesh.texture.clone(),
    });
    draw_guest_back_label(position, facing_yaw);
}

fn draw_guest_back_label(position: Vec3, facing_yaw: f32) {
    const PIXEL: f32 = 0.023;
    const GLYPHS: [[u8; 7]; 5] = [
        [
            0b01110, 0b10001, 0b10000, 0b10111, 0b10001, 0b10001, 0b01110,
        ],
        [
            0b10001, 0b10001, 0b10001, 0b10001, 0b10001, 0b10001, 0b01110,
        ],
        [
            0b11111, 0b10000, 0b10000, 0b11110, 0b10000, 0b10000, 0b11111,
        ],
        [
            0b01111, 0b10000, 0b10000, 0b01110, 0b00001, 0b00001, 0b11110,
        ],
        [
            0b11111, 0b00100, 0b00100, 0b00100, 0b00100, 0b00100, 0b00100,
        ],
    ];
    let mut vertices = Vec::new();
    let mut indices = Vec::new();
    append_guest_label_quad(
        &mut vertices,
        &mut indices,
        position,
        facing_yaw,
        -0.43,
        1.28,
        -0.53,
        0.43,
        1.57,
        Color::from_rgba(20, 29, 48, 255),
    );
    let text_start = -(29.0 * PIXEL) * 0.5;
    for (glyph_index, glyph) in GLYPHS.iter().enumerate() {
        for (row, bits) in glyph.iter().enumerate() {
            for column in 0..5 {
                if bits & (1 << (4 - column)) == 0 {
                    continue;
                }
                let left = text_start + (glyph_index * 6 + column) as f32 * PIXEL;
                let top = 1.52 - row as f32 * PIXEL;
                append_guest_label_quad(
                    &mut vertices,
                    &mut indices,
                    position,
                    facing_yaw,
                    left,
                    top - PIXEL,
                    -0.54,
                    left + PIXEL * 0.82,
                    top,
                    WHITE,
                );
            }
        }
    }
    draw_mesh(&Mesh {
        vertices,
        indices,
        texture: None,
    });
}

fn append_guest_label_quad(
    vertices: &mut Vec<Vertex>,
    indices: &mut Vec<u16>,
    position: Vec3,
    facing_yaw: f32,
    left: f32,
    bottom: f32,
    z: f32,
    right: f32,
    top: f32,
    color: Color,
) {
    let start = vertices.len() as u16;
    for (x, y) in [(left, bottom), (right, bottom), (right, top), (left, top)] {
        vertices.push(Vertex::new2(
            position + rotate_avatar_offset(vec3(x, y, z), facing_yaw),
            Vec2::ZERO,
            color,
        ));
    }
    indices.extend_from_slice(&[start, start + 1, start + 2, start, start + 2, start + 3]);
}

fn draw_held_sword(position: Vec3, facing_yaw: f32, swing_timer: f32) {
    let swing = if swing_timer > 0.0 {
        ((0.28 - swing_timer) / 0.28 * std::f32::consts::PI).sin() * 0.8
    } else {
        0.0
    };
    let sword_yaw = facing_yaw + swing;
    let parts = [
        (
            vec3(0.7, 1.05, 0.05),
            vec3(0.13, 0.5, 0.13),
            Color::from_rgba(87, 51, 32, 255),
        ),
        (
            vec3(0.7, 1.31, 0.05),
            vec3(0.48, 0.1, 0.15),
            Color::from_rgba(222, 175, 89, 255),
        ),
        (
            vec3(0.7, 1.76, 0.05),
            vec3(0.24, 0.86, 0.12),
            Color::from_rgba(205, 222, 239, 255),
        ),
        (
            vec3(0.7, 2.22, 0.05),
            vec3(0.02, 0.08, 0.02),
            Color::from_rgba(247, 250, 255, 255),
        ),
    ];
    for (local_center, size, color) in parts {
        let center = position + rotate_avatar_offset(local_center, sword_yaw);
        let mut mesh = beveled_block_mesh(center, size, color);
        for vertex in &mut mesh.vertices {
            let relative = vertex.position - center;
            vertex.position = center + rotate_avatar_offset(relative, sword_yaw);
        }
        draw_mesh(&mesh);
    }
}

fn avatar_model_position(source: Vec3) -> Vec3 {
    vec3(
        source.x * AVATAR_MODEL_SCALE,
        source.y * AVATAR_MODEL_SCALE + AVATAR_MODEL_GROUND_OFFSET,
        -source.z * AVATAR_MODEL_SCALE,
    )
}

fn draw_beveled_block(center: Vec3, size: Vec3, color: Color) {
    draw_mesh(&beveled_block_mesh(center, size, color));
}

fn beveled_block_mesh(center: Vec3, size: Vec3, color: Color) -> Mesh {
    let mut vertices = Vec::with_capacity(36);
    let mut indices = Vec::with_capacity(54);
    let half_x = size.x * 0.5;
    let half_z = size.z * 0.5;
    let bevel = half_x.min(half_z).min(size.y * 0.4) * 0.32;
    let low_y = center.y - size.y * 0.5;
    let shoulder_y = center.y + size.y * 0.5 - bevel;
    let top_y = center.y + size.y * 0.5;
    let outer = [
        vec2(-half_x, -half_z),
        vec2(half_x, -half_z),
        vec2(half_x, half_z),
        vec2(-half_x, half_z),
    ];
    let inset = [
        vec2(-half_x + bevel, -half_z + bevel),
        vec2(half_x - bevel, -half_z + bevel),
        vec2(half_x - bevel, half_z - bevel),
        vec2(-half_x + bevel, half_z - bevel),
    ];

    for edge in 0..4 {
        let next = (edge + 1) % 4;
        let outer_a = center + vec3(outer[edge].x, 0.0, outer[edge].y);
        let outer_b = center + vec3(outer[next].x, 0.0, outer[next].y);
        let inner_a = center + vec3(inset[edge].x, 0.0, inset[edge].y);
        let inner_b = center + vec3(inset[next].x, 0.0, inset[next].y);
        let side_color = shade_block_color(color, [0.58, 0.78, 0.68, 1.0][edge]);
        let bevel_color = shade_block_color(color, [0.78, 1.05, 0.88, 1.12][edge]);
        add_block_quad(
            &mut vertices,
            &mut indices,
            [
                vec3(outer_a.x, low_y, outer_a.z),
                vec3(outer_a.x, shoulder_y, outer_a.z),
                vec3(outer_b.x, shoulder_y, outer_b.z),
                vec3(outer_b.x, low_y, outer_b.z),
            ],
            side_color,
        );
        add_block_quad(
            &mut vertices,
            &mut indices,
            [
                vec3(outer_a.x, shoulder_y, outer_a.z),
                vec3(inner_a.x, top_y, inner_a.z),
                vec3(inner_b.x, top_y, inner_b.z),
                vec3(outer_b.x, shoulder_y, outer_b.z),
            ],
            bevel_color,
        );
    }

    add_block_quad(
        &mut vertices,
        &mut indices,
        [
            vec3(inset[0].x, top_y, inset[0].y),
            vec3(inset[3].x, top_y, inset[3].y),
            vec3(inset[2].x, top_y, inset[2].y),
            vec3(inset[1].x, top_y, inset[1].y),
        ],
        shade_block_color(color, 1.2),
    );
    Mesh {
        vertices,
        indices,
        texture: None,
    }
}

fn add_block_quad(
    vertices: &mut Vec<Vertex>,
    indices: &mut Vec<u16>,
    corners: [Vec3; 4],
    color: Color,
) {
    let first_index = vertices.len() as u16;
    vertices.extend(
        corners
            .into_iter()
            .map(|corner| Vertex::new2(corner, Vec2::ZERO, color)),
    );
    indices.extend([
        first_index,
        first_index + 1,
        first_index + 2,
        first_index,
        first_index + 2,
        first_index + 3,
    ]);
}

fn shade_block_color(color: Color, amount: f32) -> Color {
    Color::new(
        (color.r * amount).min(1.0),
        (color.g * amount).min(1.0),
        (color.b * amount).min(1.0),
        color.a,
    )
}

fn rotate_avatar_limb(position: Vec3, pivot: Vec3, angle: f32) -> Vec3 {
    let relative = position - pivot;
    let (sin_angle, cos_angle) = angle.sin_cos();
    pivot
        + vec3(
            relative.x,
            relative.y * cos_angle - relative.z * sin_angle,
            relative.y * sin_angle + relative.z * cos_angle,
        )
}

fn colors_equal(first: Color, second: Color) -> bool {
    (first.r - second.r).abs() < 0.01
        && (first.g - second.g).abs() < 0.01
        && (first.b - second.b).abs() < 0.01
}

fn is_driving_experience(slug: &str) -> bool {
    slug == "coastal-drive" || slug == BOX_DELIVERY_SLUG
}

const CITY_HALF: f32 = 255.0;
const CITY_PITCH: f32 = 60.0;
const CITY_ORIGIN: f32 = -210.0;
const DEPOT_X: f32 = -30.0;
const DEPOT_Z: f32 = -30.0;
const PICKUP_RADIUS: f32 = 9.0;
const DELIVERY_RADIUS: f32 = 14.0;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum BuildingKind {
    House,
    Tower,
    Shed,
}

#[derive(Clone, Copy)]
struct CityBuilding {
    x: f32,
    z: f32,
    half_w: f32,
    half_d: f32,
    height: f32,
    color: Color,
    kind: BuildingKind,
}

fn city_hash(a: i32, b: i32, c: i32) -> u32 {
    let mut h = (a as u32).wrapping_mul(73_856_093)
        ^ (b as u32).wrapping_mul(19_349_663)
        ^ (c as u32).wrapping_mul(83_492_791);
    h ^= h >> 13;
    h = h.wrapping_mul(0x5bd1_e995);
    h ^ (h >> 15)
}

fn city_buildings() -> &'static [CityBuilding] {
    static CITY: std::sync::OnceLock<Vec<CityBuilding>> = std::sync::OnceLock::new();
    CITY.get_or_init(|| {
        const HOUSE_COLORS: [(u8, u8, u8); 6] = [
            (232, 196, 160),
            (176, 207, 226),
            (226, 160, 150),
            (190, 220, 170),
            (240, 228, 170),
            (214, 190, 226),
        ];
        const TOWER_COLORS: [(u8, u8, u8); 4] =
            [(120, 132, 150), (96, 110, 134), (150, 156, 168), (84, 98, 120)];
        let mut buildings = Vec::new();
        for bi in 0..8 {
            for bj in 0..8 {
                let cx = CITY_ORIGIN + CITY_PITCH * bi as f32;
                let cz = CITY_ORIGIN + CITY_PITCH * bj as f32;
                if cx == DEPOT_X && cz == DEPOT_Z {
                    buildings.push(CityBuilding {
                        x: cx + 17.0,
                        z: cz + 17.0,
                        half_w: 4.0,
                        half_d: 4.0,
                        height: 5.0,
                        color: Color::from_rgba(170, 120, 70, 255),
                        kind: BuildingKind::Shed,
                    });
                    continue;
                }
                let downtown = (2..=5).contains(&bi) && (2..=5).contains(&bj);
                for (k, (sx, sz)) in [(-1.0, -1.0), (1.0, -1.0), (-1.0, 1.0), (1.0, 1.0)]
                    .into_iter()
                    .enumerate()
                {
                    let h = city_hash(bi, bj, k as i32);
                    let half_w = 6.0 + (h % 4) as f32 * 0.5;
                    let half_d = 6.0 + ((h >> 3) % 4) as f32 * 0.5;
                    let (height, rgb, kind) = if downtown {
                        (
                            26.0 + ((h >> 6) % 40) as f32,
                            TOWER_COLORS[(h >> 12) as usize % TOWER_COLORS.len()],
                            BuildingKind::Tower,
                        )
                    } else {
                        (
                            5.0 + ((h >> 6) % 3) as f32,
                            HOUSE_COLORS[(h >> 12) as usize % HOUSE_COLORS.len()],
                            BuildingKind::House,
                        )
                    };
                    buildings.push(CityBuilding {
                        x: cx + sx * 13.0,
                        z: cz + sz * 13.0,
                        half_w,
                        half_d,
                        height,
                        color: Color::from_rgba(rgb.0, rgb.1, rgb.2, 255),
                        kind,
                    });
                }
            }
        }
        buildings
    })
}

fn city_house_indices() -> &'static [usize] {
    static HOUSES: std::sync::OnceLock<Vec<usize>> = std::sync::OnceLock::new();
    HOUSES.get_or_init(|| {
        city_buildings()
            .iter()
            .enumerate()
            .filter(|(_, building)| building.kind == BuildingKind::House)
            .map(|(index, _)| index)
            .collect()
    })
}

fn city_collides(x: f32, z: f32) -> bool {
    city_buildings().iter().any(|building| {
        (x - building.x).abs() < building.half_w + 2.5
            && (z - building.z).abs() < building.half_d + 2.5
    })
}

struct DeliveryState {
    money: u32,
    carrying: bool,
    target: usize,
    time_left: f32,
    deliveries: u32,
    message: String,
    message_timer: f32,
    seed: u32,
}

impl DeliveryState {
    const fn new() -> Self {
        Self {
            money: 0,
            carrying: false,
            target: 0,
            time_left: 0.0,
            deliveries: 0,
            message: String::new(),
            message_timer: 0.0,
            seed: 12_345,
        }
    }

    fn next_random(&mut self) -> u32 {
        self.seed = self.seed.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
        self.seed >> 8
    }

    fn pick_target(&mut self) {
        let houses = city_house_indices();
        for _ in 0..50 {
            let candidate = houses[self.next_random() as usize % houses.len()];
            let building = &city_buildings()[candidate];
            let distance = (building.x - DEPOT_X).hypot(building.z - DEPOT_Z);
            if candidate != self.target && distance > 70.0 {
                self.target = candidate;
                return;
            }
        }
        self.target = houses[0];
    }

    fn update(&mut self, position: Vec3, delta: f32) {
        self.message_timer = (self.message_timer - delta).max(0.0);
        if self.carrying {
            self.time_left = (self.time_left - delta).max(0.0);
            let house = &city_buildings()[self.target];
            let distance = (position.x - house.x).hypot(position.z - house.z);
            if distance < DELIVERY_RADIUS {
                let trip = (house.x - DEPOT_X).hypot(house.z - DEPOT_Z);
                let base = 10 + (trip * 0.4) as u32;
                let bonus = if self.time_left > 0.0 { base / 2 } else { 0 };
                self.money += base + bonus;
                self.deliveries += 1;
                self.carrying = false;
                self.message = if bonus > 0 {
                    format!("Delivered! +${} (+${} fast bonus)", base + bonus, bonus)
                } else {
                    format!("Delivered! +${}", base)
                };
                self.message_timer = 4.0;
            }
        } else if (position.x - DEPOT_X).hypot(position.z - DEPOT_Z) < PICKUP_RADIUS {
            self.pick_target();
            let house = &city_buildings()[self.target];
            let trip = (house.x - DEPOT_X).hypot(house.z - DEPOT_Z);
            self.carrying = true;
            self.time_left = 20.0 + trip / 30.0;
            self.message = "Box loaded! Deliver it to the green beacon".to_owned();
            self.message_timer = 4.0;
        }
    }
}

static DELIVERY: std::sync::Mutex<DeliveryState> = std::sync::Mutex::new(DeliveryState::new());

fn delivery_lock() -> std::sync::MutexGuard<'static, DeliveryState> {
    DELIVERY.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn delivery_reset() {
    let mut state = delivery_lock();
    *state = DeliveryState::new();
    state.seed = (get_time() * 1000.0) as u32 ^ 0x9e37_79b9;
}

fn delivery_update(position: Vec3, delta: f32) {
    delivery_lock().update(position, delta);
}

fn draw_city_world(player_position: Vec3) {
    let (px, pz) = (player_position.x, player_position.z);
    draw_plane(
        vec3(0.0, -0.05, 0.0),
        vec2(900.0, 900.0),
        None,
        Color::from_rgba(96, 148, 84, 255),
    );
    let road_color = Color::from_rgba(58, 62, 70, 255);
    let line_count = 9;
    for k in 0..line_count {
        let line = -240.0 + CITY_PITCH * k as f32;
        draw_cube(vec3(line, 0.03, 0.0), vec3(14.0, 0.10, 520.0), None, road_color);
        draw_cube(vec3(0.0, 0.02, line), vec3(520.0, 0.08, 14.0), None, road_color);
    }
    let pad_color = Color::from_rgba(178, 182, 188, 255);
    for bi in 0..8 {
        for bj in 0..8 {
            let cx = CITY_ORIGIN + CITY_PITCH * bi as f32;
            let cz = CITY_ORIGIN + CITY_PITCH * bj as f32;
            if (cx - px).abs() < 220.0 && (cz - pz).abs() < 220.0 {
                draw_cube(vec3(cx, 0.02, cz), vec3(46.0, 0.08, 46.0), None, pad_color);
            }
        }
    }

    let near_intersection = |t: f32| {
        let m = ((t + 240.0) / CITY_PITCH).round().clamp(0.0, 8.0);
        (t - (-240.0 + CITY_PITCH * m)).abs() < 10.0
    };
    let dash_color = Color::from_rgba(244, 214, 90, 255);
    for k in 0..line_count {
        let line = -240.0 + CITY_PITCH * k as f32;
        if (line - px).abs() < 130.0 {
            let mut t = ((pz - 130.0) / 8.0).floor() * 8.0;
            while t < pz + 130.0 {
                if t.abs() < CITY_HALF && !near_intersection(t) {
                    draw_cube(vec3(line, 0.09, t), vec3(0.4, 0.04, 3.5), None, dash_color);
                }
                t += 8.0;
            }
        }
        if (line - pz).abs() < 130.0 {
            let mut t = ((px - 130.0) / 8.0).floor() * 8.0;
            while t < px + 130.0 {
                if t.abs() < CITY_HALF && !near_intersection(t) {
                    draw_cube(vec3(t, 0.08, line), vec3(3.5, 0.04, 0.4), None, dash_color);
                }
                t += 8.0;
            }
        }
    }

    draw_cube(
        vec3(DEPOT_X, 0.1, DEPOT_Z),
        vec3(14.0, 0.1, 14.0),
        None,
        Color::from_rgba(240, 176, 40, 255),
    );

    for building in city_buildings() {
        if (building.x - px).abs() > 220.0 || (building.z - pz).abs() > 220.0 {
            continue;
        }
        let size = vec3(building.half_w * 2.0, building.height, building.half_d * 2.0);
        draw_cube(
            vec3(building.x, building.height * 0.5, building.z),
            size,
            None,
            building.color,
        );
        match building.kind {
            BuildingKind::House => {
                draw_cube(
                    vec3(building.x, building.height + 0.6, building.z),
                    vec3(size.x + 1.4, 1.2, size.z + 1.4),
                    None,
                    Color::from_rgba(150, 70, 62, 255),
                );
                draw_cube(
                    vec3(building.x, 1.3, building.z + building.half_d),
                    vec3(1.8, 2.6, 0.3),
                    None,
                    Color::from_rgba(92, 62, 40, 255),
                );
            }
            BuildingKind::Tower => {
                draw_cube(
                    vec3(building.x, building.height + 0.4, building.z),
                    vec3(size.x + 0.6, 0.8, size.z + 0.6),
                    None,
                    Color::from_rgba(70, 78, 92, 255),
                );
                let mut y = 4.0;
                while y < building.height - 2.0 {
                    draw_cube(
                        vec3(building.x, y, building.z),
                        vec3(size.x + 0.15, 1.2, size.z + 0.15),
                        None,
                        Color::from_rgba(172, 216, 240, 255),
                    );
                    y += 5.0;
                }
            }
            BuildingKind::Shed => {}
        }
    }

    let state = delivery_lock();
    let (beam_x, beam_z, beam_color) = if state.carrying {
        let house = &city_buildings()[state.target];
        (house.x, house.z, Color::new(0.2, 1.0, 0.4, 0.45))
    } else {
        (DEPOT_X, DEPOT_Z, Color::new(1.0, 0.85, 0.1, 0.45))
    };
    draw_cube(
        vec3(beam_x, 45.0, beam_z),
        vec3(1.6, 90.0, 1.6),
        None,
        beam_color,
    );
}

fn draw_delivery_hud(position: Vec3, yaw: f32, speed: f32) {
    let state = delivery_lock();
    let panel = Rect::new(screen_width() - 226.0, screen_height() - 132.0, 210.0, 116.0);
    draw_rectangle(
        panel.x,
        panel.y,
        panel.w,
        panel.h,
        Color::new(0.05, 0.09, 0.14, 0.88),
    );
    draw_text("BOX DELIVERY", panel.x + 13.0, panel.y + 23.0, 16.0, WHITE);
    draw_text(
        &format!("${}", state.money),
        panel.x + 13.0,
        panel.y + 52.0,
        30.0,
        Color::from_rgba(120, 235, 140, 255),
    );
    draw_text(
        &format!(
            "{} deliveries  {:03} km/h",
            state.deliveries,
            (speed.abs() * 2.3) as u32
        ),
        panel.x + 13.0,
        panel.y + 76.0,
        14.0,
        LIGHTGRAY,
    );
    draw_text(
        "WASD / Arrows  •  Esc exit",
        panel.x + 12.0,
        panel.y + 100.0,
        11.0,
        LIGHTGRAY,
    );

    let (target_x, target_z) = if state.carrying {
        let house = &city_buildings()[state.target];
        (house.x, house.z)
    } else {
        (DEPOT_X, DEPOT_Z)
    };
    let (dx, dz) = (target_x - position.x, target_z - position.z);
    let distance = dx.hypot(dz);
    let status = if state.carrying {
        format!("Deliver the box  {:.0} m", distance)
    } else {
        format!("Go to the yellow depot  {:.0} m", distance)
    };
    let center_x = screen_width() * 0.5;
    draw_rectangle(
        center_x - 150.0,
        14.0,
        300.0,
        if state.carrying { 86.0 } else { 58.0 },
        Color::new(0.05, 0.09, 0.14, 0.85),
    );
    let width = measure_text(&status, None, 18, 1.0).width;
    draw_text(&status, center_x - width * 0.5, 38.0, 18.0, WHITE);
    if state.carrying {
        let timer = format!("Time left: {:.0}s", state.time_left);
        let width = measure_text(&timer, None, 16, 1.0).width;
        let timer_color = if state.time_left > 0.0 {
            Color::from_rgba(120, 235, 140, 255)
        } else {
            Color::from_rgba(240, 120, 110, 255)
        };
        draw_text(&timer, center_x - width * 0.5, 62.0, 16.0, timer_color);
    }

    // +x is on the car's left, so the arrow mirrors the horizontal axis.
    let relative = dx.atan2(dz) - yaw;
    let arrow_center = vec2(center_x, if state.carrying { 148.0 } else { 118.0 });
    let tip = arrow_center + vec2(-relative.sin(), -relative.cos()) * 22.0;
    let tail = arrow_center - vec2(-relative.sin(), -relative.cos()) * 14.0;
    let side = vec2(relative.cos(), -relative.sin()) * 12.0;
    draw_triangle(tip, tail + side, tail - side, Color::from_rgba(120, 235, 140, 255));

    if state.message_timer > 0.0 {
        let width = measure_text(&state.message, None, 24, 1.0).width;
        draw_text(
            &state.message,
            center_x - width * 0.5,
            screen_height() * 0.3,
            24.0,
            Color::from_rgba(255, 236, 120, 255),
        );
    }

    let map = Rect::new(16.0, 16.0, 150.0, 150.0);
    draw_rectangle(map.x, map.y, map.w, map.h, Color::new(0.05, 0.09, 0.14, 0.85));
    let to_map = |x: f32, z: f32| {
        vec2(
            map.x + (x + CITY_HALF) / (CITY_HALF * 2.0) * map.w,
            map.y + (CITY_HALF - z) / (CITY_HALF * 2.0) * map.h,
        )
    };
    for building in city_buildings() {
        let point = to_map(building.x, building.z);
        let color = if building.kind == BuildingKind::Tower {
            Color::from_rgba(100, 112, 134, 255)
        } else {
            Color::from_rgba(70, 84, 100, 255)
        };
        draw_rectangle(point.x - 2.0, point.y - 2.0, 4.0, 4.0, color);
    }
    let depot = to_map(DEPOT_X, DEPOT_Z);
    draw_circle(depot.x, depot.y, 4.0, Color::from_rgba(240, 176, 40, 255));
    if state.carrying {
        let house = &city_buildings()[state.target];
        let point = to_map(house.x, house.z);
        draw_circle(point.x, point.y, 4.0, Color::from_rgba(60, 230, 110, 255));
    }
    let me = to_map(position.x, position.z);
    draw_circle(me.x, me.y, 3.0, WHITE);
}

fn draw_sunset_plaza() {
    draw_plane(
        vec3(0.0, -0.08, 2.0),
        vec2(48.0, 30.0),
        None,
        Color::from_rgba(56, 154, 77, 255),
    );
    draw_plane(
        vec3(0.0, -0.03, -13.0),
        vec2(48.0, 9.0),
        None,
        Color::from_rgba(36, 183, 205, 255),
    );
    draw_plane(
        vec3(0.0, -0.01, -8.5),
        vec2(48.0, 4.0),
        None,
        Color::from_rgba(232, 197, 135, 255),
    );
    draw_plane(
        vec3(0.0, 0.0, 4.0),
        vec2(48.0, 7.0),
        None,
        Color::from_rgba(34, 38, 46, 255),
    );
    draw_plane(
        vec3(0.0, 0.02, 2.5),
        vec2(48.0, 0.08),
        None,
        Color::from_rgba(252, 220, 36, 255),
    );
    draw_plane(
        vec3(0.0, 0.02, 5.2),
        vec2(48.0, 0.08),
        None,
        Color::from_rgba(252, 220, 36, 255),
    );
    draw_cube(
        vec3(-10.0, 1.0, -2.0),
        vec3(4.0, 2.0, 3.0),
        None,
        Color::from_rgba(211, 101, 79, 255),
    );
    draw_cube(
        vec3(-10.0, 2.3, -2.0),
        vec3(4.4, 0.55, 3.3),
        None,
        Color::from_rgba(139, 55, 67, 255),
    );
    for window_x in [-11.0, -9.0] {
        draw_cube(
            vec3(window_x, 1.15, -0.44),
            vec3(0.8, 0.55, 0.08),
            None,
            Color::from_rgba(107, 210, 235, 255),
        );
    }
    for tree_x in [-6.0, -2.0, 4.0, 9.0] {
        draw_cube(
            vec3(tree_x, 1.2, -3.2),
            vec3(0.6, 2.4, 0.6),
            None,
            Color::from_rgba(117, 73, 42, 255),
        );
        draw_cube(
            vec3(tree_x, 2.8, -3.2),
            vec3(2.3, 1.9, 2.0),
            None,
            Color::from_rgba(18, 113, 54, 255),
        );
        draw_cube(
            vec3(tree_x, 3.8, -3.2),
            vec3(1.5, 1.3, 1.4),
            None,
            Color::from_rgba(23, 139, 62, 255),
        );
    }
    draw_cube(
        vec3(5.0, 0.8, -8.5),
        vec3(1.0, 1.6, 1.0),
        None,
        Color::from_rgba(187, 100, 50, 255),
    );
    draw_cube(
        vec3(5.0, 1.7, -8.5),
        vec3(1.2, 0.25, 1.2),
        None,
        Color::from_rgba(72, 39, 42, 255),
    );
    draw_cube(
        vec3(6.4, 0.75, -8.5),
        vec3(0.16, 1.5, 0.16),
        None,
        Color::from_rgba(43, 43, 49, 255),
    );
    draw_cube(
        vec3(6.7, 1.55, -8.5),
        vec3(2.0, 0.18, 0.75),
        None,
        Color::from_rgba(59, 108, 182, 255),
    );
    for step in 0..4 {
        draw_beveled_block(
            vec3(-5.0 + step as f32 * 0.5, 0.25 + step as f32 * 0.28, -6.0),
            vec3(1.1, 0.5 + step as f32 * 0.54, 0.8),
            Color::from_rgba(67, 178, 237, 255),
        );
    }
}

fn draw_game_overlay(
    _experience: Experience,
    username: &str,
    network: &mut NetworkSession,
    body_color: &mut Color,
    equipped_accessory: &mut Option<AccessoryId>,
    avatar_menu_open: &mut bool,
    color_status: &mut String,
    accessory_status: &mut String,
    chat_open: &mut bool,
    chat_input: &mut String,
    experience_title: &str,
    selected_tool: &mut PlayerTool,
    show_tools: bool,
    health: f32,
    is_guest: bool,
) {
    let chat_rect = Rect::new(12.0, 44.0, 300.0, 236.0);
    if !is_guest {
        draw_rectangle(2.0, 2.0, 44.0, 37.0, Color::from_rgba(27, 34, 52, 235));
        draw_text("⚙", 14.0, 28.0, 22.0, WHITE);
    }
    draw_rectangle(50.0, 2.0, 44.0, 37.0, Color::from_rgba(27, 34, 52, 235));
    draw_text("●", 65.0, 27.0, 18.0, Color::from_rgba(187, 133, 255, 255));
    let mouse = vec2(mouse_position().0, mouse_position().1);
    if !is_guest
        && is_mouse_button_pressed(MouseButton::Left)
        && Rect::new(2.0, 2.0, 44.0, 37.0).contains(mouse)
    {
        *avatar_menu_open = !*avatar_menu_open;
    }
    if is_mouse_button_pressed(MouseButton::Left)
        && Rect::new(50.0, 2.0, 44.0, 37.0).contains(mouse)
    {
        *chat_open = true;
    }
    draw_rectangle(
        chat_rect.x,
        chat_rect.y,
        chat_rect.w,
        chat_rect.h,
        Color::new(0.08, 0.11, 0.17, 0.92),
    );
    draw_text(
        "WORLD CHAT",
        14.0,
        64.0,
        13.0,
        Color::from_rgba(174, 190, 224, 255),
    );
    let visible_messages = network.chat_messages.len().min(8);
    let first = network.chat_messages.len().saturating_sub(visible_messages);
    for (index, message) in network.chat_messages[first..].iter().enumerate() {
        draw_text(
            message,
            14.0,
            89.0 + index as f32 * 20.0,
            15.0,
            Color::from_rgba(232, 235, 244, 255),
        );
    }
    let input_rect = Rect::new(10.0, 244.0, 230.0, 28.0);
    let send_rect = Rect::new(247.0, 244.0, 32.0, 28.0);
    draw_rectangle(
        input_rect.x,
        input_rect.y,
        input_rect.w,
        input_rect.h,
        Color::from_rgba(21, 22, 33, 255),
    );
    draw_text(
        if *chat_open {
            chat_input.as_str()
        } else {
            "Click or press / to chat..."
        },
        input_rect.x + 8.0,
        input_rect.y + 19.0,
        14.0,
        Color::from_rgba(224, 227, 237, 255),
    );
    draw_rectangle(
        send_rect.x,
        send_rect.y,
        send_rect.w,
        send_rect.h,
        Color::from_rgba(47, 49, 69, 255),
    );
    draw_text(">", send_rect.x + 11.0, send_rect.y + 20.0, 17.0, WHITE);

    if *avatar_menu_open && !is_guest {
        draw_rectangle(
            100.0,
            42.0,
            198.0,
            194.0,
            Color::new(0.08, 0.11, 0.17, 0.96),
        );
        draw_text("AVATAR BODY COLOR", 109.0, 62.0, 13.0, WHITE);
        for (index, color) in BODY_COLORS.iter().enumerate() {
            let rect = Rect::new(
                110.0 + (index % 4) as f32 * 43.0,
                75.0 + (index / 4) as f32 * 34.0,
                29.0,
                25.0,
            );
            draw_rectangle(rect.x, rect.y, rect.w, rect.h, *color);
            if colors_equal(*color, *body_color) {
                draw_rectangle_lines(
                    rect.x - 2.0,
                    rect.y - 2.0,
                    rect.w + 4.0,
                    rect.h + 4.0,
                    2.0,
                    WHITE,
                );
            }
            if is_mouse_button_pressed(MouseButton::Left) && rect.contains(mouse) {
                *body_color = *color;
                match save_body_color(*body_color) {
                    Ok(()) => *color_status = "Avatar saved.".to_owned(),
                    Err(error) => *color_status = format!("Could not save avatar: {error}"),
                }
            }
        }
        draw_text(color_status, 108.0, 154.0, 12.0, LIGHTGRAY);
        draw_text("GEAR", 109.0, 174.0, 12.0, WHITE);
        for (index, accessory) in AccessoryId::ALL.iter().copied().enumerate() {
            let rect = Rect::new(108.0 + index as f32 * 61.0, 180.0, 57.0, 27.0);
            draw_rectangle(
                rect.x,
                rect.y,
                rect.w,
                rect.h,
                if *equipped_accessory == Some(accessory) {
                    Color::from_rgba(89, 67, 105, 255)
                } else {
                    Color::from_rgba(56, 98, 143, 255)
                },
            );
            draw_text(
                match accessory {
                    AccessoryId::RedHair => "HAIR",
                    AccessoryId::TrafficCone => "HAT",
                    AccessoryId::Sword => "SWORD",
                },
                rect.x + 5.0,
                rect.y + 18.0,
                10.0,
                WHITE,
            );
            if is_mouse_button_pressed(MouseButton::Left) && rect.contains(mouse) {
                *equipped_accessory = if *equipped_accessory == Some(accessory) {
                    None
                } else {
                    Some(accessory)
                };
                match save_avatar_accessory(*equipped_accessory) {
                    Ok(()) => *accessory_status = "Avatar gear saved.".to_owned(),
                    Err(error) => *accessory_status = format!("Could not save accessory: {error}"),
                }
            }
        }
        draw_text(accessory_status, 108.0, 230.0, 10.0, LIGHTGRAY);
    }

    let player_panel = Rect::new(screen_width() - 178.0, 14.0, 164.0, 126.0);
    draw_rectangle(
        player_panel.x,
        player_panel.y,
        player_panel.w,
        player_panel.h,
        Color::new(0.08, 0.11, 0.17, 0.92),
    );
    draw_text(
        &format!("PLAYERS  ({})", network.peers.len() + 1),
        player_panel.x + 10.0,
        player_panel.y + 23.0,
        14.0,
        WHITE,
    );
    draw_text(
        username,
        player_panel.x + 12.0,
        player_panel.y + 49.0,
        15.0,
        Color::from_rgba(245, 205, 102, 255),
    );
    draw_health_tool_hud(selected_tool, show_tools, health);
    for (index, peer) in network.peers.values().take(3).enumerate() {
        draw_text(
            &peer.username,
            player_panel.x + 12.0,
            player_panel.y + 74.0 + index as f32 * 19.0,
            14.0,
            LIGHTGRAY,
        );
    }

    draw_text(
        &format!("{}  •  {}", experience_title, network.status),
        18.0,
        screen_height() - 20.0,
        14.0,
        if network.status.starts_with("Online") {
            LIME
        } else {
            YELLOW
        },
    );
    draw_text(
        "WASD move   1/2 tools   Space jump   F swing   Esc menu",
        screen_width() - 278.0,
        screen_height() - 20.0,
        12.0,
        WHITE,
    );

    if is_mouse_button_pressed(MouseButton::Left) && input_rect.contains(mouse) {
        *chat_open = true;
    }
    if is_mouse_button_pressed(MouseButton::Left) && send_rect.contains(mouse) {
        send_chat(network, username, chat_input);
    }
}

fn draw_health_tool_hud(selected_tool: &mut PlayerTool, show_tools: bool, health: f32) {
    let width = screen_width();
    let height = screen_height();
    let health_panel = Rect::new(14.0, height - 62.0, 246.0, 42.0);
    draw_rectangle(
        health_panel.x,
        health_panel.y,
        health_panel.w,
        health_panel.h,
        Color::new(0.04, 0.06, 0.1, 0.88),
    );
    draw_text(
        "HEALTH",
        health_panel.x + 10.0,
        health_panel.y + 16.0,
        11.0,
        Color::from_rgba(220, 228, 244, 255),
    );
    draw_text(
        &format!("{:.0}", health.clamp(0.0, 100.0)),
        health_panel.x + health_panel.w - 31.0,
        health_panel.y + 16.0,
        11.0,
        WHITE,
    );
    let bar = Rect::new(
        health_panel.x + 10.0,
        health_panel.y + 22.0,
        health_panel.w - 20.0,
        12.0,
    );
    draw_rectangle(
        bar.x,
        bar.y,
        bar.w,
        bar.h,
        Color::from_rgba(48, 53, 66, 255),
    );
    draw_rectangle(
        bar.x,
        bar.y,
        bar.w * (health.clamp(0.0, 100.0) / 100.0),
        bar.h,
        Color::from_rgba(70, 220, 104, 255),
    );

    if !show_tools {
        return;
    }
    let mouse = vec2(mouse_position().0, mouse_position().1);
    let slot_size = 68.0;
    let slot_x = width - slot_size - 14.0;
    let first_y = (height * 0.35)
        .min(height - slot_size * 2.0 - 58.0)
        .max(120.0);
    for index in 0..2 {
        let tool = PlayerTool::from_index(index);
        let rect = Rect::new(
            slot_x,
            first_y + index as f32 * (slot_size + 16.0),
            slot_size,
            slot_size,
        );
        let selected = *selected_tool == tool;
        draw_rectangle(
            rect.x,
            rect.y,
            rect.w,
            rect.h,
            if selected {
                Color::from_rgba(54, 78, 127, 245)
            } else {
                Color::from_rgba(23, 30, 46, 235)
            },
        );
        draw_rectangle_lines(
            rect.x,
            rect.y,
            rect.w,
            rect.h,
            if selected { 3.0 } else { 1.0 },
            if selected {
                Color::from_rgba(116, 174, 255, 255)
            } else {
                Color::from_rgba(111, 126, 153, 255)
            },
        );
        match tool {
            PlayerTool::Sword => {
                draw_line(
                    rect.x + 22.0,
                    rect.y + 49.0,
                    rect.x + 47.0,
                    rect.y + 17.0,
                    5.0,
                    Color::from_rgba(220, 231, 244, 255),
                );
                draw_line(
                    rect.x + 18.0,
                    rect.y + 43.0,
                    rect.x + 32.0,
                    rect.y + 54.0,
                    4.0,
                    Color::from_rgba(218, 173, 91, 255),
                );
                draw_line(
                    rect.x + 38.0,
                    rect.y + 13.0,
                    rect.x + 51.0,
                    rect.y + 23.0,
                    3.0,
                    Color::from_rgba(218, 173, 91, 255),
                );
            }
            PlayerTool::JumpCoil => {
                draw_line(
                    rect.x + 26.0,
                    rect.y + 14.0,
                    rect.x + 26.0,
                    rect.y + 54.0,
                    3.0,
                    Color::from_rgba(231, 71, 92, 255),
                );
                draw_line(
                    rect.x + 42.0,
                    rect.y + 14.0,
                    rect.x + 42.0,
                    rect.y + 54.0,
                    3.0,
                    Color::from_rgba(231, 71, 92, 255),
                );
                for ring in 0..4 {
                    let y = rect.y + 18.0 + ring as f32 * 9.0;
                    draw_line(
                        rect.x + 25.0,
                        y,
                        rect.x + if ring % 2 == 0 { 43.0 } else { 25.0 },
                        y + 7.0,
                        3.0,
                        Color::from_rgba(255, 103, 122, 255),
                    );
                }
            }
        }
        draw_text(
            &format!("{}  {}", index + 1, tool.name()),
            rect.x - 32.0,
            rect.y + rect.h + 14.0,
            11.0,
            WHITE,
        );
        if is_mouse_button_pressed(MouseButton::Left) && rect.contains(mouse) {
            *selected_tool = tool;
        }
    }
    draw_text(
        "F: swing",
        slot_x - 4.0,
        first_y + 2.0 * (slot_size + 16.0) + 18.0,
        11.0,
        Color::from_rgba(208, 218, 238, 255),
    );
}

fn draw_studio_button(rect: Rect, label: &str, color: Color, scale: f32) {
    draw_rectangle(rect.x, rect.y, rect.w, rect.h, color);
    draw_rectangle_lines(
        rect.x,
        rect.y,
        rect.w,
        rect.h,
        scale,
        Color::from_rgba(16, 17, 19, 255),
    );
    let dimensions = measure_text(label, None, (12.0 * scale) as u16, 1.0);
    draw_text(
        label,
        rect.x + (rect.w - dimensions.width) * 0.5,
        rect.y + rect.h * 0.66,
        12.0 * scale,
        WHITE,
    );
}

fn profile_directory() -> Result<PathBuf, String> {
    let root = env::var_os("LOCALAPPDATA")
        .map(PathBuf::from)
        .unwrap_or_else(env::temp_dir);
    Ok(root.join("EBlocks"))
}

fn load_body_color() -> Result<Color, String> {
    let path = profile_directory()?.join("avatar.json");
    let bytes = match fs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(BODY_COLORS[0]),
        Err(error) => return Err(error.to_string()),
    };
    let color: [f32; 3] = serde_json::from_slice(&bytes)
        .map_err(|error| format!("Could not read {}: {error}", path.display()))?;
    if !color
        .iter()
        .all(|channel| channel.is_finite() && (0.0..=1.0).contains(channel))
    {
        return Err("Saved body color contains invalid color values.".to_owned());
    }
    Ok(Color::new(color[0], color[1], color[2], 1.0))
}

fn save_body_color(color: Color) -> Result<(), String> {
    let directory = profile_directory()?;
    fs::create_dir_all(&directory).map_err(|error| error.to_string())?;
    let json =
        serde_json::to_vec(&[color.r, color.g, color.b]).map_err(|error| error.to_string())?;
    fs::write(directory.join("avatar.json"), json).map_err(|error| error.to_string())
}

fn load_avatar_accessory() -> Result<Option<AccessoryId>, String> {
    let path = profile_directory()?.join("avatar_accessory.json");
    let bytes = match fs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.to_string()),
    };
    serde_json::from_slice(&bytes)
        .map_err(|error| format!("Could not read {}: {error}", path.display()))
}

fn save_avatar_accessory(accessory: Option<AccessoryId>) -> Result<(), String> {
    let directory = profile_directory()?;
    fs::create_dir_all(&directory).map_err(|error| error.to_string())?;
    let json = serde_json::to_vec(&accessory).map_err(|error| error.to_string())?;
    fs::write(directory.join("avatar_accessory.json"), json).map_err(|error| error.to_string())
}

#[derive(Clone)]
struct StudioGameInfo {
    id: String,
    name: String,
    blocks: usize,
}

#[derive(Deserialize, Serialize)]
struct StudioGameFile {
    name: String,
    blocks: Vec<StudioBlock>,
    #[serde(default)]
    tools_enabled: bool,
}

static CURRENT_STUDIO_GAME: std::sync::Mutex<Option<(String, String)>> = std::sync::Mutex::new(None);

fn studio_games_directory() -> Result<PathBuf, String> {
    Ok(profile_directory()?.join("studio_games"))
}

fn read_studio_game_file(path: &std::path::Path) -> Result<StudioGameFile, String> {
    let bytes = fs::read(path).map_err(|error| error.to_string())?;
    let game: StudioGameFile = serde_json::from_slice(&bytes)
        .map_err(|error| format!("Could not read {}: {error}", path.display()))?;
    if game.blocks.len() > 5000 || game.blocks.iter().any(|block| !block.is_valid()) {
        return Err("Saved game contains unsupported blocks.".to_owned());
    }
    Ok(game)
}

fn list_studio_games() -> Vec<StudioGameInfo> {
    let Ok(directory) = studio_games_directory() else {
        return Vec::new();
    };
    if !directory.exists() {
        // Bring over the single world from older versions.
        if let Ok(bytes) = fs::read(profile_directory().unwrap_or_default().join("my-first-game.json")) {
            if let Ok(blocks) = serde_json::from_slice::<Vec<StudioBlock>>(&bytes) {
                if !blocks.is_empty() && blocks.iter().all(StudioBlock::is_valid) {
                    let _ = write_studio_game("game-1", "My First Game", &blocks, false);
                }
            }
        }
    }
    let mut games = Vec::new();
    if let Ok(entries) = fs::read_dir(&directory) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|value| value.to_str()) != Some("json") {
                continue;
            }
            let Some(id) = path.file_stem().and_then(|value| value.to_str()) else {
                continue;
            };
            if let Ok(game) = read_studio_game_file(&path) {
                let modified = entry.metadata().and_then(|meta| meta.modified()).ok();
                games.push((modified, StudioGameInfo { id: id.to_owned(), name: game.name, blocks: game.blocks.len() }));
            }
        }
    }
    games.sort_by(|a, b| b.0.cmp(&a.0));
    games.into_iter().map(|(_, info)| info).collect()
}

fn write_studio_game(
    id: &str,
    name: &str,
    blocks: &[StudioBlock],
    tools_enabled: bool,
) -> Result<(), String> {
    let directory = studio_games_directory()?;
    fs::create_dir_all(&directory).map_err(|error| error.to_string())?;
    let json = serde_json::to_vec(&StudioGameFile { name: name.to_owned(), blocks: blocks.to_vec(), tools_enabled })
        .map_err(|error| error.to_string())?;
    fs::write(directory.join(format!("{id}.json")), json).map_err(|error| error.to_string())
}

fn load_studio_game(id: &str) -> Result<(Vec<StudioBlock>, bool), String> {
    read_studio_game_file(&studio_games_directory()?.join(format!("{id}.json")))
        .map(|game| (game.blocks, game.tools_enabled))
}

fn delete_studio_game(id: &str) -> Result<(), String> {
    fs::remove_file(studio_games_directory()?.join(format!("{id}.json"))).map_err(|error| error.to_string())
}

// Starts editing a saved game, or creates a new one when no id is given.
fn begin_studio_game(id: Option<&str>, name: &str) -> String {
    STUDIO_TOOLS_ENABLED.store(false, std::sync::atomic::Ordering::Relaxed);
    let mut status = String::new();
    let id = match id {
        Some(id) => id.to_owned(),
        None => {
            let millis = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |duration| duration.as_millis());
            let id = format!("game-{millis}");
            if let Err(error) = write_studio_game(&id, name, &[], false) {
                status = format!("Could not create the game: {error}");
            }
            id
        }
    };
    *CURRENT_STUDIO_GAME.lock().unwrap() = Some((id, name.to_owned()));
    status
}

static STUDIO_TOOLS_ENABLED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

fn save_studio_blocks(blocks: &[StudioBlock]) -> Result<(), String> {
    let tools_enabled = STUDIO_TOOLS_ENABLED.load(std::sync::atomic::Ordering::Relaxed);
    if blocks.len() > 5000 {
        return Err("A world can contain at most 5,000 blocks.".to_owned());
    }
    let current = CURRENT_STUDIO_GAME.lock().unwrap().clone();
    let (id, name) = current.ok_or("No game is open.")?;
    write_studio_game(&id, &name, blocks, tools_enabled)
}

fn block_color(x: i32, z: i32) -> Color {
    match (x + z).rem_euclid(4) {
        0 => Color::from_rgba(49, 133, 238, 255),
        1 => Color::from_rgba(238, 104, 78, 255),
        2 => Color::from_rgba(91, 193, 127, 255),
        _ => Color::from_rgba(200, 122, 230, 255),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn camera_field_of_view_values_are_radians() {
        assert!((0.0..std::f32::consts::PI).contains(&AVATAR_PREVIEW_FOVY));
        assert!((0.0..std::f32::consts::PI).contains(&WORLD_CAMERA_FOVY));
    }

    #[test]
    fn jump_coil_gives_a_higher_jump_than_the_sword() {
        assert!(jump_impulse(PlayerTool::JumpCoil) > jump_impulse(PlayerTool::Sword));
    }

    #[test]
    fn dynabloxx_avatar_model_loads_all_six_body_parts() {
        let model = AvatarModel::from_obj(include_str!("../assets/dynabloxx_avatar.obj"))
            .expect("the bundled Dynabloxx avatar OBJ should be valid");

        assert!(model.triangles.iter().all(|part| !part.is_empty()));
        assert!(
            model
                .triangles
                .iter()
                .zip(&model.texture_coordinates)
                .all(|(triangles, coordinates)| triangles.len() == coordinates.len())
        );
        let bounds = model
            .triangles
            .iter()
            .flatten()
            .map(|vertex| avatar_model_position(*vertex))
            .fold(
                (f32::INFINITY, f32::NEG_INFINITY),
                |(min_y, max_y), point| (min_y.min(point.y), max_y.max(point.y)),
            );
        assert!(bounds.0.abs() < 0.01);
        assert!((1.9..=2.1).contains(&bounds.1));
    }

    #[test]
    fn guest_avatar_uses_the_dynabloxx_model_without_exceeding_mesh_limits() {
        let avatar = AvatarModel::from_obj(include_str!("../assets/dynabloxx_avatar.obj"))
            .expect("the bundled Dynabloxx avatar OBJ should be valid");
        let guest = GuestAvatarModel::from_avatar_model(&avatar);
        let expected_vertices = avatar.triangles.iter().map(Vec::len).sum::<usize>();

        assert_eq!(guest.mesh.vertices.len(), expected_vertices);
        assert_eq!(guest.mesh.indices.len(), expected_vertices);
        assert_eq!(guest.mesh.indices.len() % 3, 0);
        assert!(guest.mesh.indices.len() < 5_000);
        assert!(
            guest
                .mesh
                .indices
                .iter()
                .all(|index| (*index as usize) < expected_vertices)
        );
        let bounds = guest
            .mesh
            .vertices
            .iter()
            .map(|vertex| vertex.position.y)
            .fold((f32::INFINITY, f32::NEG_INFINITY), |(min_y, max_y), y| {
                (min_y.min(y), max_y.max(y))
            });
        assert!(bounds.0.abs() < 0.01);
        assert!((1.9..=2.1).contains(&bounds.1));
    }

    #[test]
    fn supplied_guest_avatar_gltf_loads_textured_triangle_mesh_data() {
        let model = GuestAvatarModel::from_gltf(
            include_str!("../assets/guest_avatar.gltf"),
            include_bytes!("../assets/guest_avatar.bin"),
            None,
        )
        .expect("the bundled guest avatar glTF should be valid");

        assert_eq!(model.mesh.vertices.len(), 4399);
        assert_eq!(model.mesh.indices.len(), 18_564);
        assert_eq!(model.mesh.indices.len() % 3, 0);
        assert!(
            model
                .mesh
                .vertices
                .iter()
                .all(|vertex| { vertex.position.is_finite() && vertex.uv.is_finite() })
        );
        let bounds = model
            .mesh
            .vertices
            .iter()
            .map(|vertex| vertex.position.y)
            .fold((f32::INFINITY, f32::NEG_INFINITY), |(min_y, max_y), y| {
                (min_y.min(y), max_y.max(y))
            });
        assert!(bounds.0.abs() < 0.001);
        assert!((1.99..=2.01).contains(&bounds.1));
    }

    #[test]
    fn accessory_catalog_loads_hair_hat_and_back_gear() {
        let model =
            AvatarAccessories::from_obj(include_str!("../assets/accessories/catalog.obj"), None)
                .expect("the bundled accessory catalog should be valid");

        for accessory in AccessoryId::ALL {
            assert!(!model.triangles[accessory.index()].is_empty());
            assert_eq!(
                model.triangles[accessory.index()].len(),
                model.texture_coordinates[accessory.index()].len()
            );
            assert_eq!(model.triangles[accessory.index()].len() % 3, 0);
        }
        assert_eq!(AccessoryId::from_wire_id("sword"), Some(AccessoryId::Sword));
        assert_eq!(AccessoryId::from_wire_id("unknown"), None);
    }

    #[test]
    fn accessory_selection_round_trips_through_saved_format() {
        for accessory in AccessoryId::ALL {
            let bytes = serde_json::to_vec(&Some(accessory)).unwrap();
            let restored: Option<AccessoryId> = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(restored, Some(accessory));
        }
        let empty = serde_json::to_vec(&None::<AccessoryId>).unwrap();
        let restored: Option<AccessoryId> = serde_json::from_slice(&empty).unwrap();
        assert_eq!(restored, None);
    }

    #[test]
    fn peer_updates_without_accessories_remain_compatible() {
        let update: PeerUpdate = serde_json::from_str(
            r#"{"id":"peer","username":"player","position":[0,0,0],"body_color":[0.2,0.3,0.4],"left":false}"#,
        )
        .unwrap();
        assert_eq!(update.accessory, None);
    }

    #[test]
    fn npc_park_is_bundled_as_a_nonempty_textured_map() {
        let asset = include_bytes!("../assets/npcmap/map.mesh");
        let mut reader = Cursor::new(asset.as_slice());
        let mut magic = [0; 4];
        reader.read_exact(&mut magic).unwrap();
        assert_eq!(&magic, b"EBM1");
        let group_count = read_map_u32(&mut reader).unwrap();
        assert!(group_count > 0);
        let mut total_vertices = 0;
        let mut textured_groups = 0;
        for _ in 0..group_count {
            let texture_index = read_map_u16(&mut reader).unwrap();
            let vertex_count = read_map_u32(&mut reader).unwrap();
            assert!(texture_index <= 11);
            assert!(vertex_count > 0 && vertex_count % 3 == 0);
            textured_groups += usize::from(texture_index > 0);
            total_vertices += vertex_count;
            for _ in 0..vertex_count {
                let mut vertex = [0; 24];
                reader.read_exact(&mut vertex).unwrap();
                for offset in (0..20).step_by(4) {
                    assert!(
                        f32::from_le_bytes(vertex[offset..offset + 4].try_into().unwrap())
                            .is_finite()
                    );
                }
            }
        }
        assert_eq!(reader.position(), asset.len() as u64);
        assert!(total_vertices / 3 > 80_000);
        assert!(textured_groups > 0);
        assert!(
            EXPERIENCES
                .iter()
                .any(|experience| experience.slug == "npc-map")
        );
    }

    #[test]
    fn obby_rift_map_loads_and_has_a_walkable_start() {
        let solids = obby_solids();
        assert_eq!(solids.len(), 253);
        assert_eq!(
            obby_landing_height(vec3(0.0, 1.3, 0.0), 1.5),
            Some(1.4)
        );
        assert!(obby_landing_height(vec3(500.0, 1.3, 0.0), 1.5).is_none());
        let meshes = build_obby_meshes();
        assert!(meshes
            .iter()
            .all(|mesh| mesh.vertices.len() <= 4800 && mesh.indices.len() <= 4800));
    }

    #[test]
    fn coastal_drive_includes_a_large_road_and_procedural_scenery() {
        assert!(
            EXPERIENCES
                .iter()
                .any(|experience| experience.slug == "coastal-drive")
        );
        let world = DrivingWorld::new();
        assert!(world.road.len() > 1);
        assert!(
            world
                .road
                .iter()
                .all(|mesh| mesh.vertices.len() <= 6000 && mesh.indices.len() <= 9000)
        );
        assert!(
            world
                .road
                .iter()
                .map(|mesh| mesh.vertices.len())
                .sum::<usize>()
                > 10_000
        );
        assert!(
            world
                .road
                .iter()
                .map(|mesh| mesh.indices.len())
                .sum::<usize>()
                > 15_000
        );
        assert!(world.props.len() > 100);
        assert!(DRIVE_ISLAND_HALF_WIDTH >= 800.0);
        assert!(DRIVE_ISLAND_HALF_LENGTH >= 700.0);
    }

    #[test]
    fn city_has_houses_and_a_clear_spawn_and_depot() {
        assert!(city_house_indices().len() > 100);
        assert!(!city_collides(0.0, 0.0));
        assert!(!city_collides(DEPOT_X, DEPOT_Z));
        let mut state = DeliveryState::new();
        state.update(vec3(DEPOT_X, 0.0, DEPOT_Z), 0.1);
        assert!(state.carrying);
        let house = city_buildings()[state.target];
        assert_eq!(house.kind, BuildingKind::House);
        state.update(vec3(house.x, 0.0, house.z), 0.1);
        assert!(!state.carrying);
        assert_eq!(state.deliveries, 1);
        assert!(state.money >= 10);
    }

    #[test]
    fn avatar_model_rejects_faces_without_a_body_part() {
        assert!(AvatarModel::from_obj("v 0 0 0\nv 1 0 0\nv 0 1 0\nf 1 2 3").is_err());
    }

    #[test]
    fn beveled_block_mesh_has_beveled_edges_and_a_highlighted_top() {
        let mesh = beveled_block_mesh(Vec3::ZERO, vec3(2.0, 1.0, 2.0), WHITE);

        assert_eq!(mesh.vertices.len(), 36);
        assert_eq!(mesh.indices.len(), 54);
        assert!(mesh.indices.iter().all(|index| *index < 36));
        let lowest_y = mesh
            .vertices
            .iter()
            .map(|vertex| vertex.position.y)
            .fold(f32::INFINITY, f32::min);
        let highest_y = mesh
            .vertices
            .iter()
            .map(|vertex| vertex.position.y)
            .fold(f32::NEG_INFINITY, f32::max);
        assert!((lowest_y + 0.5).abs() < 0.001);
        assert!((highest_y - 0.5).abs() < 0.001);
        let side_shade = mesh.vertices[0].color[0];
        let top_shade = mesh.vertices[32].color[0];
        assert!(top_shade > side_shade);
    }

    #[test]
    fn hub_game_search_matches_builtin_and_published_titles_case_insensitively() {
        let published = PublishedGame {
            id: "game-id".to_owned(),
            title: "Moon Tower".to_owned(),
            owner: "builder".to_owned(),
            blocks: Vec::new(),
            tools_enabled: false,
        };
        assert_eq!(search_game_titles("  BOX ", &[]), vec!["Box Delivery"]);
        assert_eq!(search_game_titles("moon", &[published]), vec!["Moon Tower"]);
        assert!(search_game_titles("unknown game", &[]).is_empty());
        assert!(search_game_titles(" ", &[]).is_empty());
    }

    #[test]
    fn movement_follows_camera_heading() {
        let forward = camera_relative_direction(vec2(0.0, 1.0), 0.0);
        assert!(forward.x.abs() < 0.001);
        assert!((forward.y + 1.0).abs() < 0.001);

        let rotated_forward =
            camera_relative_direction(vec2(0.0, 1.0), std::f32::consts::FRAC_PI_2);
        assert!((rotated_forward.x + 1.0).abs() < 0.001);
        assert!(rotated_forward.y.abs() < 0.001);
    }

    #[test]
    fn avatar_walk_cycle_advances_while_moving_and_fades_when_still() {
        let mut player = Player {
            position: Vec3::ZERO,
            velocity: Vec3::ZERO,
            grounded: true,
            walk_phase: 0.0,
            walk_amount: 0.0,
            facing_yaw: 0.0,
        };

        update_walk_animation(&mut player, true, 0.1);
        assert!(player.walk_phase > 0.0);
        assert!(player.walk_amount > 0.0);

        player.walk_amount = 1.0;
        update_walk_animation(&mut player, false, 0.1);
        assert!(player.walk_amount < 1.0);
    }

    #[test]
    fn avatar_turns_to_face_its_movement_direction() {
        let mut player = Player {
            position: Vec3::ZERO,
            velocity: Vec3::ZERO,
            grounded: true,
            walk_phase: 0.0,
            walk_amount: 0.0,
            facing_yaw: 0.0,
        };

        update_player_facing(&mut player, vec2(1.0, 0.0), 0.1);

        assert!((player.facing_yaw - std::f32::consts::FRAC_PI_2).abs() < 0.001);
    }

    #[test]
    fn sky_sprint_landing_uses_platform_crossing() {
        let mut player = Player {
            position: vec3(3.0, 1.5, -3.0),
            velocity: vec3(0.0, -3.0, 0.0),
            grounded: false,
            walk_phase: 0.0,
            walk_amount: 0.0,
            facing_yaw: 0.0,
        };

        update_player_vertical("sky-sprint", &mut player, 0.1);

        assert_eq!(player.position.y, 1.4);
        assert_eq!(player.velocity.y, 0.0);
        assert!(player.grounded);
    }

    #[test]
    fn sky_sprint_fall_respawns_at_start() {
        let mut player = Player {
            position: vec3(12.0, -9.9, -12.0),
            velocity: vec3(0.0, -5.0, 0.0),
            grounded: false,
            walk_phase: 0.0,
            walk_amount: 0.0,
            facing_yaw: 0.0,
        };

        update_player_vertical("sky-sprint", &mut player, 0.1);

        assert_eq!(player.position, Vec3::ZERO);
        assert_eq!(player.velocity, Vec3::ZERO);
        assert!(player.grounded);
    }
}
