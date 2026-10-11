use super::*;

const STUDIO_TEXTURE_TILE: f32 = 4.0;
const BLOCKS_PER_MESH: usize = 130;
const HANDLE_RADIUS: f32 = 12.0;
const ROTATE_STEP: f32 = std::f32::consts::PI / 12.0;
const MIN_BLOCK_SIZE: f32 = 0.25;
const MAX_BLOCK_SIZE: f32 = 20.0;
const WORLD_LIMIT: f32 = 100.0;
const PLAYER_RADIUS: f32 = 0.4;
const PLAYER_HEIGHT: f32 = 1.8;
const STEP_SIZES: [f32; 3] = [1.0, 0.5, 0.25];
const AXIS_COLORS: [Color; 3] = [
    Color::new(0.91, 0.30, 0.24, 1.0),
    Color::new(0.18, 0.80, 0.44, 1.0),
    Color::new(0.20, 0.60, 0.86, 1.0),
];

fn unit_axis(axis: usize) -> Vec3 {
    match axis {
        0 => Vec3::X,
        1 => Vec3::Y,
        _ => Vec3::Z,
    }
}

impl StudioBlock {
    pub fn new(center: Vec3, color_index: usize) -> Self {
        let size = [2.0, 1.0, 2.0];
        Self {
            position: [center.x, center.y - size[1] * 0.5, center.z],
            color_index: color_index.min(BODY_COLORS.len() - 1),
            size,
            rotation: 0.0,
            tilt: [0.0; 2],
        }
    }

    pub fn center(&self) -> Vec3 {
        vec3(
            self.position[0],
            self.position[1] + self.size[1] * 0.5,
            self.position[2],
        )
    }

    fn set_center(&mut self, center: Vec3) {
        self.position = [
            center.x.clamp(-WORLD_LIMIT, WORLD_LIMIT),
            (center.y - self.size[1] * 0.5).clamp(-WORLD_LIMIT, WORLD_LIMIT),
            center.z.clamp(-WORLD_LIMIT, WORLD_LIMIT),
        ];
    }

    pub fn orientation(&self) -> Quat {
        Quat::from_euler(EulerRot::YXZ, self.rotation, self.tilt[0], self.tilt[1])
    }

    fn half_size(&self) -> Vec3 {
        Vec3::from_array(self.size) * 0.5
    }

    fn is_tilted(&self) -> bool {
        self.tilt != [0.0; 2]
    }

    fn world_half_extents(&self) -> Vec3 {
        let orientation = self.orientation();
        let half = self.half_size();
        (orientation * Vec3::X).abs() * half.x
            + (orientation * Vec3::Y).abs() * half.y
            + (orientation * Vec3::Z).abs() * half.z
    }

    pub fn is_valid(&self) -> bool {
        self.color_index < BODY_COLORS.len()
            && self
                .position
                .iter()
                .all(|value| value.is_finite() && value.abs() <= WORLD_LIMIT)
            && self
                .size
                .iter()
                .all(|value| value.is_finite() && (0.1..=MAX_BLOCK_SIZE).contains(value))
            && self.rotation.is_finite()
            && self.tilt.iter().all(|value| value.is_finite())
    }

    fn resize_side(&mut self, axis: usize, sign: f32, amount: f32) -> bool {
        let old = self.size[axis];
        let new = (old + amount).clamp(MIN_BLOCK_SIZE, MAX_BLOCK_SIZE);
        let change = new - old;
        if change.abs() < 1e-5 {
            return false;
        }
        let center = self.center() + self.orientation() * unit_axis(axis) * sign * change * 0.5;
        self.size[axis] = new;
        self.set_center(center);
        true
    }

    fn translate(&mut self, delta: Vec3) {
        let center = self.center() + delta;
        self.set_center(center);
    }

    fn rotate_axis(&mut self, axis: usize, angle: f32) {
        let target = match axis {
            0 => &mut self.tilt[0],
            1 => &mut self.rotation,
            _ => &mut self.tilt[1],
        };
        *target = (*target + angle).rem_euclid(std::f32::consts::TAU);
    }

    fn ray_hit(&self, origin: Vec3, direction: Vec3) -> Option<f32> {
        let inverse = self.orientation().inverse();
        let local_origin = inverse * (origin - self.center());
        let local_direction = inverse * direction;
        let half = self.half_size();
        let (mut near, mut far) = (0.0_f32, f32::INFINITY);
        for axis in 0..3 {
            if local_direction[axis].abs() < 1e-6 {
                if local_origin[axis].abs() > half[axis] {
                    return None;
                }
            } else {
                let first = (-half[axis] - local_origin[axis]) / local_direction[axis];
                let second = (half[axis] - local_origin[axis]) / local_direction[axis];
                near = near.max(first.min(second));
                far = far.min(first.max(second));
                if near > far {
                    return None;
                }
            }
        }
        Some(near)
    }

    fn vertical_range(&self) -> (f32, f32) {
        let center = self.center();
        if self.is_tilted() {
            let extent = self.world_half_extents().y;
            (center.y - extent, center.y + extent)
        } else {
            (center.y - self.size[1] * 0.5, center.y + self.size[1] * 0.5)
        }
    }

    fn contains_xz(&self, x: f32, z: f32, margin: f32) -> bool {
        let center = self.center();
        if self.is_tilted() {
            let extent = self.world_half_extents();
            (x - center.x).abs() <= extent.x + margin && (z - center.z).abs() <= extent.z + margin
        } else {
            let local = self.orientation().inverse() * vec3(x - center.x, 0.0, z - center.z);
            let half = self.half_size();
            local.x.abs() <= half.x + margin && local.z.abs() <= half.z + margin
        }
    }
}

struct StudioHandle {
    axis: usize,
    sign: f32,
    world: Vec3,
}

fn studio_handles(block: &StudioBlock, tool: StudioTool) -> Vec<StudioHandle> {
    let center = block.center();
    let orientation = block.orientation();
    let extents = block.world_half_extents();
    let mut handles = Vec::with_capacity(6);
    for axis in 0..3 {
        for sign in [-1.0_f32, 1.0] {
            let world = match tool {
                StudioTool::Scale => {
                    center + orientation * unit_axis(axis) * sign * (block.size[axis] * 0.5 + 0.7)
                }
                StudioTool::Move | StudioTool::Rotate => {
                    center + unit_axis(axis) * sign * (extents[axis] + 1.0)
                }
            };
            handles.push(StudioHandle { axis, sign, world });
        }
    }
    handles
}

fn apply_handle(
    block: &mut StudioBlock,
    tool: StudioTool,
    handle: &StudioHandle,
    step: f32,
    shrink: bool,
) -> bool {
    match tool {
        StudioTool::Scale => {
            block.resize_side(handle.axis, handle.sign, if shrink { -step } else { step })
        }
        StudioTool::Move => {
            block.translate(unit_axis(handle.axis) * handle.sign * step);
            true
        }
        StudioTool::Rotate => {
            block.rotate_axis(handle.axis, handle.sign * ROTATE_STEP);
            true
        }
    }
}

pub fn studio_camera(player: &Player, orbit: &CameraOrbit) -> Camera3D {
    let horizontal_distance = CAMERA_DISTANCE * orbit.pitch.cos();
    Camera3D {
        position: player.position
            + vec3(
                orbit.yaw.sin() * horizontal_distance,
                CAMERA_DISTANCE * orbit.pitch.sin(),
                orbit.yaw.cos() * horizontal_distance,
            ),
        target: player.position + vec3(0.0, 1.0, 0.0),
        up: vec3(0.0, 1.0, 0.0),
        fovy: WORLD_CAMERA_FOVY,
        ..Default::default()
    }
}

fn project_point(matrix: &Mat4, point: Vec3) -> Option<Vec2> {
    let clip = *matrix * point.extend(1.0);
    if clip.w <= 0.001 {
        return None;
    }
    let ndc = clip.truncate() / clip.w;
    (ndc.z <= 1.0).then(|| {
        vec2(
            (ndc.x * 0.5 + 0.5) * screen_width(),
            (0.5 - ndc.y * 0.5) * screen_height(),
        )
    })
}

fn mouse_ray(matrix: &Mat4, mouse: Vec2) -> (Vec3, Vec3) {
    let inverse = matrix.inverse();
    let ndc_x = mouse.x / screen_width() * 2.0 - 1.0;
    let ndc_y = 1.0 - mouse.y / screen_height() * 2.0;
    let near = inverse.project_point3(vec3(ndc_x, ndc_y, -1.0));
    let far = inverse.project_point3(vec3(ndc_x, ndc_y, 1.0));
    (near, (far - near).normalize_or_zero())
}

fn pick_block(blocks: &[StudioBlock], matrix: &Mat4, mouse: Vec2) -> Option<usize> {
    let (origin, direction) = mouse_ray(matrix, mouse);
    blocks
        .iter()
        .enumerate()
        .filter_map(|(index, block)| block.ray_hit(origin, direction).map(|t| (index, t)))
        .min_by(|a, b| a.1.total_cmp(&b.1))
        .map(|(index, _)| index)
}

fn studio_scale() -> f32 {
    (screen_width() / 1280.0)
        .min(screen_height() / 720.0)
        .clamp(0.7, 1.3)
}

fn studio_top_height() -> f32 {
    52.0 * studio_scale()
}

fn studio_bottom_height() -> f32 {
    58.0 * studio_scale()
}

// 0 Add, 1 Move, 2 Scale, 3 Rotate, 4 Delete, 5 Step
fn studio_button(index: usize) -> Rect {
    let scale = studio_scale();
    Rect::new(
        (190.0 + index as f32 * 104.0) * scale,
        10.0 * scale,
        96.0 * scale,
        32.0 * scale,
    )
}

fn studio_publish_button() -> Rect {
    let scale = studio_scale();
    Rect::new(
        screen_width() - 236.0 * scale,
        10.0 * scale,
        120.0 * scale,
        32.0 * scale,
    )
}

fn studio_exit_button() -> Rect {
    let scale = studio_scale();
    Rect::new(
        screen_width() - 108.0 * scale,
        10.0 * scale,
        96.0 * scale,
        32.0 * scale,
    )
}

fn studio_swatch(index: usize) -> Rect {
    let scale = studio_scale();
    let total = BODY_COLORS.len() as f32 * 42.0 * scale - 8.0 * scale;
    Rect::new(
        (screen_width() - total) * 0.5 + index as f32 * 42.0 * scale,
        screen_height() - studio_bottom_height() + 12.0 * scale,
        34.0 * scale,
        34.0 * scale,
    )
}

fn studio_dialog() -> Rect {
    let scale = studio_scale();
    Rect::new(
        (screen_width() - 380.0 * scale) * 0.5,
        (screen_height() - 176.0 * scale) * 0.5,
        380.0 * scale,
        176.0 * scale,
    )
}

fn studio_dialog_name_field() -> Rect {
    let (dialog, scale) = (studio_dialog(), studio_scale());
    Rect::new(
        dialog.x + 18.0 * scale,
        dialog.y + 62.0 * scale,
        dialog.w - 36.0 * scale,
        31.0 * scale,
    )
}

fn studio_dialog_publish() -> Rect {
    let (dialog, scale) = (studio_dialog(), studio_scale());
    Rect::new(
        dialog.x + 18.0 * scale,
        dialog.y + 114.0 * scale,
        148.0 * scale,
        34.0 * scale,
    )
}

fn studio_dialog_cancel() -> Rect {
    let (dialog, scale) = (studio_dialog(), studio_scale());
    Rect::new(
        dialog.x + dialog.w - 112.0 * scale,
        dialog.y + 114.0 * scale,
        94.0 * scale,
        34.0 * scale,
    )
}

fn over_studio_bars(mouse: Vec2) -> bool {
    mouse.y <= studio_top_height() || mouse.y >= screen_height() - studio_bottom_height()
}

fn add_studio_block(
    blocks: &mut Vec<StudioBlock>,
    status: &mut String,
    color_index: usize,
    player: &Player,
    orbit: &CameraOrbit,
) -> Option<usize> {
    if blocks.len() >= 5000 {
        *status = "A world can contain at most 5,000 blocks.".to_owned();
        return None;
    }
    let forward = vec3(-orbit.yaw.sin(), 0.0, -orbit.yaw.cos());
    let ahead = player.position + forward * 4.0;
    let mut block = StudioBlock::new(Vec3::ZERO, color_index);
    let mut center = vec3(ahead.x, player.position.y.max(0.0) + 0.5, ahead.z);
    for _ in 0..200 {
        block.set_center(center);
        let taken = blocks
            .iter()
            .any(|other| (other.center() - block.center()).length() < 0.1);
        if !taken {
            break;
        }
        center.y += 1.0;
    }
    blocks.push(block);
    report_studio_save(status, blocks);
    Some(blocks.len() - 1)
}

pub fn studio_controls(
    blocks: &mut Vec<StudioBlock>,
    status: &mut String,
    selected_color: &mut usize,
    player: &Player,
    orbit: &CameraOrbit,
    ui: &mut StudioUi,
    base_url: &str,
    token: &str,
    published_games: &mut Vec<PublishedGame>,
) {
    if ui.selected_block.is_some_and(|index| index >= blocks.len()) {
        ui.selected_block = None;
    }
    let (mouse_x, mouse_y) = mouse_position();
    let mouse = vec2(mouse_x, mouse_y);
    let pressed = is_mouse_button_pressed(MouseButton::Left);

    if ui.publish_open {
        if !pressed {
            return;
        }
        if studio_dialog_name_field().contains(mouse) {
            ui.publish_focused = true;
        } else if studio_dialog_cancel().contains(mouse) {
            ui.publish_open = false;
            ui.publish_focused = false;
        } else if studio_dialog_publish().contains(mouse) {
            let name = ui.publish_name.trim().to_owned();
            if !(3..=40).contains(&name.chars().count()) {
                *status = "Enter a game name of 3-40 characters first.".to_owned();
            } else {
                match publish_game_to_server(base_url, token, &name, blocks, ui.tools_enabled) {
                    Ok(game) => {
                        if let Some(existing) = published_games
                            .iter_mut()
                            .find(|existing| existing.id == game.id)
                        {
                            *existing = game;
                        } else {
                            published_games.insert(0, game);
                        }
                        *status = format!("Published! Other players can now play {name}.");
                        ui.publish_open = false;
                        ui.publish_focused = false;
                    }
                    Err(error) => *status = error,
                }
            }
        }
        return;
    }

    if is_key_pressed(KeyCode::Delete) {
        delete_selected(blocks, status, ui);
    }
    if !pressed {
        return;
    }

    if studio_button(0).contains(mouse) {
        if let Some(index) = add_studio_block(blocks, status, *selected_color, player, orbit) {
            ui.selected_block = Some(index);
        }
        return;
    }
    for (index, tool) in [
        (1, StudioTool::Move),
        (2, StudioTool::Scale),
        (3, StudioTool::Rotate),
    ] {
        if studio_button(index).contains(mouse) {
            ui.tool = tool;
            return;
        }
    }
    if studio_button(4).contains(mouse) {
        delete_selected(blocks, status, ui);
        return;
    }
    if studio_button(5).contains(mouse) {
        ui.step_index = (ui.step_index + 1) % STEP_SIZES.len();
        return;
    }
    if studio_button(6).contains(mouse) {
        ui.tools_enabled = !ui.tools_enabled;
        STUDIO_TOOLS_ENABLED.store(ui.tools_enabled, std::sync::atomic::Ordering::Relaxed);
        report_studio_save(status, blocks);
        *status = if ui.tools_enabled {
            "Players will get the Sword and Jump Coil in this game.".to_owned()
        } else {
            "Sword and Jump Coil are removed from this game.".to_owned()
        };
        return;
    }
    if studio_publish_button().contains(mouse) {
        ui.publish_open = true;
        ui.publish_focused = true;
        return;
    }
    if studio_exit_button().contains(mouse) {
        ui.exit_requested = true;
        return;
    }
    for color in 0..BODY_COLORS.len() {
        if studio_swatch(color).contains(mouse) {
            *selected_color = color;
            if let Some(block) = ui.selected_block.and_then(|index| blocks.get_mut(index)) {
                block.color_index = color;
                report_studio_save(status, blocks);
            }
            return;
        }
    }
    if over_studio_bars(mouse) {
        return;
    }

    let matrix = studio_camera(player, orbit).matrix();
    if let Some(index) = ui.selected_block {
        let shrink = is_key_down(KeyCode::LeftShift) || is_key_down(KeyCode::RightShift);
        let step = STEP_SIZES[ui.step_index];
        let handles = studio_handles(&blocks[index], ui.tool);
        let hit = handles
            .iter()
            .filter_map(|handle| {
                project_point(&matrix, handle.world).map(|point| (handle, point.distance(mouse)))
            })
            .filter(|(_, distance)| *distance <= HANDLE_RADIUS + 4.0)
            .min_by(|a, b| a.1.total_cmp(&b.1));
        if let Some((handle, _)) = hit {
            if apply_handle(&mut blocks[index], ui.tool, handle, step, shrink) {
                report_studio_save(status, blocks);
            }
            return;
        }
    }
    ui.selected_block = pick_block(blocks, &matrix, mouse);
}

fn delete_selected(blocks: &mut Vec<StudioBlock>, status: &mut String, ui: &mut StudioUi) {
    match ui.selected_block.take() {
        Some(index) if index < blocks.len() => {
            blocks.remove(index);
            report_studio_save(status, blocks);
        }
        _ => *status = "Click a block first, then press DELETE.".to_owned(),
    }
}

fn push_block_mesh(block: &StudioBlock, vertices: &mut Vec<Vertex>, indices: &mut Vec<u16>) {
    let center = block.center();
    let orientation = block.orientation();
    let half = block.half_size();
    let tint = BODY_COLORS[block.color_index.min(BODY_COLORS.len() - 1)];
    let light = vec3(0.35, 0.85, 0.4).normalize();
    for axis in 0..3 {
        for sign in [-1.0_f32, 1.0] {
            let u_axis = (axis + 1) % 3;
            let v_axis = (axis + 2) % 3;
            let normal = orientation * unit_axis(axis) * sign;
            let shade = 0.55 + 0.45 * (0.5 + 0.5 * normal.dot(light));
            let color = Color::new(tint.r * shade, tint.g * shade, tint.b * shade, 1.0);
            let base = vertices.len() as u16;
            let u_length = block.size[u_axis] / STUDIO_TEXTURE_TILE;
            let v_length = block.size[v_axis] / STUDIO_TEXTURE_TILE;
            for (u_sign, v_sign, u, v) in [
                (-1.0_f32, -1.0_f32, 0.0, 0.0),
                (1.0, -1.0, u_length, 0.0),
                (1.0, 1.0, u_length, v_length),
                (-1.0, 1.0, 0.0, v_length),
            ] {
                let local = unit_axis(axis) * sign * half[axis]
                    + unit_axis(u_axis) * u_sign * half[u_axis]
                    + unit_axis(v_axis) * v_sign * half[v_axis];
                let point = center + orientation * local;
                vertices.push(Vertex::new(point.x, point.y, point.z, u, v, color));
            }
            indices.extend_from_slice(&[base, base + 1, base + 2, base, base + 2, base + 3]);
        }
    }
}

fn studio_texture() -> Texture2D {
    let texture = Texture2D::from_file_with_format(
        include_bytes!("../assets/studio_block.png"),
        Some(ImageFormat::Png),
    );
    texture.set_filter(FilterMode::Linear);
    {
        let context = unsafe { get_internal_gl() }.quad_context;
        context.texture_generate_mipmaps(texture.raw_miniquad_id());
        context.texture_set_min_filter(
            texture.raw_miniquad_id(),
            miniquad::FilterMode::Linear,
            miniquad::MipmapFilterMode::Linear,
        );
    }
    unsafe { get_internal_gl() }.quad_context.texture_set_wrap(
        texture.raw_miniquad_id(),
        miniquad::TextureWrap::Repeat,
        miniquad::TextureWrap::Repeat,
    );
    texture
}

fn cached_studio_texture() -> Texture2D {
    thread_local! {
        static TEXTURE: Texture2D = studio_texture();
    }
    TEXTURE.with(|texture| texture.clone())
}

fn baseplate_bounds() -> (Vec3, Vec3) {
    let mut min = Vec3::splat(f32::MAX);
    let mut max = Vec3::splat(f32::MIN);
    for line in include_str!("../assets/roblox_baseplate.obj").lines() {
        let Some(rest) = line.strip_prefix("v ") else {
            continue;
        };
        let values: Vec<f32> = rest
            .split_whitespace()
            .filter_map(|value| value.parse().ok())
            .collect();
        if let [x, y, z] = values[..] {
            min = min.min(vec3(x, y, z));
            max = max.max(vec3(x, y, z));
        }
    }
    (min, max)
}

// The Roblox baseplate model, drawn with the studded texture.
pub fn draw_studio_baseplate() {
    thread_local! {
        static MESH: Mesh = {
            let (min, max) = baseplate_bounds();
            let size = max - min;
            let center = (min + max) * 0.5;
            let half = size * 0.5;
            let tile = size.x / 64.0;
            let tint = Color::new(0.62, 0.62, 0.64, 1.0);
            let mut vertices = Vec::new();
            let mut indices = Vec::new();
            for axis in 0..3 {
                for sign in [-1.0_f32, 1.0] {
                    let (u_axis, v_axis) = ((axis + 1) % 3, (axis + 2) % 3);
                    let shade = if axis == 1 && sign > 0.0 { 1.0 } else { 0.7 };
                    let color = Color::new(tint.r * shade, tint.g * shade, tint.b * shade, 1.0);
                    let base = vertices.len() as u16;
                    for (u_sign, v_sign) in [(-1.0_f32, -1.0_f32), (1.0, -1.0), (1.0, 1.0), (-1.0, 1.0)] {
                        let point = center
                            + unit_axis(axis) * sign * half[axis]
                            + unit_axis(u_axis) * u_sign * half[u_axis]
                            + unit_axis(v_axis) * v_sign * half[v_axis];
                        let u = (u_sign + 1.0) * 0.5 * size[u_axis] / tile;
                        let v = (v_sign + 1.0) * 0.5 * size[v_axis] / tile;
                        vertices.push(Vertex::new(point.x, point.y, point.z, u, v, color));
                    }
                    indices.extend_from_slice(&[base, base + 1, base + 2, base, base + 2, base + 3]);
                }
            }
            Mesh { vertices, indices, texture: Some(cached_studio_texture()) }
        };
    }
    MESH.with(draw_mesh);
}

// W/S fly along the camera view, A/D strafe, Space/E rise and Q/Ctrl descend.
pub fn fly_studio_camera(player: &mut Player, orbit: &CameraOrbit, delta: f32, enabled: bool) {
    if !enabled {
        return;
    }
    let forward = -vec3(
        orbit.yaw.sin() * orbit.pitch.cos(),
        orbit.pitch.sin(),
        orbit.yaw.cos() * orbit.pitch.cos(),
    );
    let right = forward.cross(Vec3::Y).normalize_or_zero();
    let axis = |positive: &[KeyCode], negative: &[KeyCode]| {
        positive.iter().any(|key| is_key_down(*key)) as i32 as f32
            - negative.iter().any(|key| is_key_down(*key)) as i32 as f32
    };
    let movement = forward * axis(&[KeyCode::W, KeyCode::Up], &[KeyCode::S, KeyCode::Down])
        + right * axis(&[KeyCode::D, KeyCode::Right], &[KeyCode::A, KeyCode::Left])
        + Vec3::Y * axis(&[KeyCode::Space, KeyCode::E], &[KeyCode::Q, KeyCode::LeftControl]);
    player.position += movement.normalize_or_zero() * 14.0 * delta;
    player.position.x = player.position.x.clamp(-110.0, 110.0);
    player.position.z = player.position.z.clamp(-135.0, 135.0);
    player.position.y = player.position.y.clamp(0.5, 80.0);
    player.velocity = Vec3::ZERO;
    player.grounded = false;
}

#[derive(Default)]
pub struct StudioMenu {
    scroll: usize,
    confirm_delete: Option<usize>,
}

pub enum StudioMenuAction {
    Back,
    New,
    Open(usize),
    Delete(usize),
}

pub fn draw_studio_menu(
    menu: &mut StudioMenu,
    games: &[StudioGameInfo],
    status: &str,
) -> Option<StudioMenuAction> {
    set_default_camera();
    clear_background(Color::from_rgba(18, 20, 26, 255));
    let scale = studio_scale();
    let width = screen_width();
    let height = screen_height();
    let mouse = vec2(mouse_position().0, mouse_position().1);
    let clicked = is_mouse_button_pressed(MouseButton::Left);
    let text = Color::from_rgba(239, 241, 245, 255);
    let muted = Color::from_rgba(174, 179, 188, 255);

    draw_text("E BLOCKS STUDIO", (width - 300.0 * scale) * 0.5, 70.0 * scale, 34.0 * scale, text);
    draw_text("Pick a game to keep building, or start a new one.", (width - 380.0 * scale) * 0.5, 96.0 * scale, 16.0 * scale, muted);

    let back = Rect::new(20.0 * scale, 20.0 * scale, 100.0 * scale, 36.0 * scale);
    draw_studio_button(back, "BACK", Color::from_rgba(73, 77, 90, 255), scale);
    let new_game = Rect::new((width - 300.0 * scale) * 0.5, 118.0 * scale, 300.0 * scale, 50.0 * scale);
    draw_studio_button(new_game, "+ NEW GAME", Color::from_rgba(44, 139, 79, 255), scale);

    let row_width = (640.0 * scale).min(width - 40.0);
    let row_height = 56.0 * scale;
    let list_top = 190.0 * scale;
    let visible = (((height - list_top - 40.0 * scale) / (row_height + 8.0 * scale)) as usize).max(1);
    let wheel = mouse_wheel().1;
    if wheel > 0.0 {
        menu.scroll = menu.scroll.saturating_sub(1);
    } else if wheel < 0.0 {
        menu.scroll = (menu.scroll + 1).min(games.len().saturating_sub(visible));
    }
    menu.scroll = menu.scroll.min(games.len().saturating_sub(visible));

    let mut action = None;
    if clicked && back.contains(mouse) {
        action = Some(StudioMenuAction::Back);
    } else if clicked && new_game.contains(mouse) {
        action = Some(StudioMenuAction::New);
    }
    if games.is_empty() {
        draw_text("You have no games yet. Press NEW GAME to make one.", (width - 440.0 * scale) * 0.5, list_top + 30.0 * scale, 18.0 * scale, muted);
    }
    for (slot, (index, game)) in games.iter().enumerate().skip(menu.scroll).take(visible).enumerate() {
        let x = (width - row_width) * 0.5;
        let y = list_top + slot as f32 * (row_height + 8.0 * scale);
        draw_rectangle(x, y, row_width, row_height, Color::from_rgba(36, 39, 46, 255));
        draw_text(&game.name, x + 16.0 * scale, y + 25.0 * scale, 20.0 * scale, text);
        draw_text(&format!("{} blocks", game.blocks), x + 16.0 * scale, y + 46.0 * scale, 14.0 * scale, muted);
        let open = Rect::new(x + row_width - 190.0 * scale, y + 10.0 * scale, 86.0 * scale, 36.0 * scale);
        let delete = Rect::new(x + row_width - 94.0 * scale, y + 10.0 * scale, 86.0 * scale, 36.0 * scale);
        let armed = menu.confirm_delete == Some(index);
        draw_studio_button(open, "OPEN", Color::from_rgba(64, 104, 150, 255), scale);
        draw_studio_button(delete, if armed { "SURE?" } else { "DELETE" }, Color::from_rgba(150, 56, 56, 255), scale);
        if clicked && open.contains(mouse) {
            action = Some(StudioMenuAction::Open(index));
        } else if clicked && delete.contains(mouse) {
            if armed {
                menu.confirm_delete = None;
                action = Some(StudioMenuAction::Delete(index));
            } else {
                menu.confirm_delete = Some(index);
            }
        }
    }
    if games.len() > visible {
        draw_text("Scroll the mouse wheel for more games.", 20.0 * scale, height - 14.0 * scale, 14.0 * scale, muted);
    }
    draw_text(status, (width - 300.0 * scale).max(20.0), height - 14.0 * scale, 14.0 * scale, muted);
    action
}
pub fn draw_studio_blocks(blocks: &[StudioBlock], selected: Option<usize>) {
    let texture = cached_studio_texture();
    for chunk in blocks.chunks(BLOCKS_PER_MESH) {
        let mut vertices = Vec::with_capacity(chunk.len() * 24);
        let mut indices = Vec::with_capacity(chunk.len() * 36);
        for block in chunk {
            push_block_mesh(block, &mut vertices, &mut indices);
        }
        draw_mesh(&Mesh {
            vertices,
            indices,
            texture: Some(texture.clone()),
        });
    }
    if let Some(block) = selected.and_then(|index| blocks.get(index)) {
        let center = block.center();
        let orientation = block.orientation();
        let half = block.half_size() + Vec3::splat(0.02);
        let corner = |x: f32, y: f32, z: f32| center + orientation * (vec3(x, y, z) * half);
        for a in [-1.0_f32, 1.0] {
            for b in [-1.0_f32, 1.0] {
                for (start, end) in [
                    (corner(-1.0, a, b), corner(1.0, a, b)),
                    (corner(a, -1.0, b), corner(a, 1.0, b)),
                    (corner(a, b, -1.0), corner(a, b, 1.0)),
                ] {
                    draw_line_3d(start, end, Color::from_rgba(255, 236, 120, 255));
                }
            }
        }
    }
}

fn draw_handle(
    block_center: Option<Vec2>,
    handle: &StudioHandle,
    point: Vec2,
    tool: StudioTool,
    hovered: bool,
) {
    let base = AXIS_COLORS[handle.axis];
    let shade = if handle.sign < 0.0 { 0.75 } else { 1.0 };
    let color = Color::new(base.r * shade, base.g * shade, base.b * shade, 1.0);
    if let Some(center) = block_center {
        draw_line(
            center.x,
            center.y,
            point.x,
            point.y,
            2.0,
            Color::new(color.r, color.g, color.b, 0.55),
        );
    }
    let outline = if hovered {
        WHITE
    } else {
        Color::new(0.0, 0.0, 0.0, 0.8)
    };
    match tool {
        StudioTool::Scale => {
            draw_rectangle(point.x - 10.0, point.y - 10.0, 20.0, 20.0, color);
            draw_rectangle_lines(point.x - 10.0, point.y - 10.0, 20.0, 20.0, 2.0, outline);
        }
        StudioTool::Move => {
            let direction = block_center.map_or(0.0, |center| {
                (point.y - center.y).atan2(point.x - center.x).to_degrees()
            });
            draw_poly(point.x, point.y, 3, 14.0, direction, color);
            draw_poly_lines(point.x, point.y, 3, 14.0, direction, 2.0, outline);
        }
        StudioTool::Rotate => {
            draw_circle(point.x, point.y, 11.0, color);
            draw_circle_lines(point.x, point.y, 11.0, 2.0, outline);
            draw_circle(point.x, point.y, 4.0, Color::new(1.0, 1.0, 1.0, 0.85));
        }
    }
}

pub fn draw_studio_overlay(
    blocks: &[StudioBlock],
    status: &str,
    selected_color: usize,
    player: &Player,
    orbit: &CameraOrbit,
    ui: &StudioUi,
) {
    let scale = studio_scale();
    let (mouse_x, mouse_y) = mouse_position();
    let mouse = vec2(mouse_x, mouse_y);
    let width = screen_width();
    let height = screen_height();
    let text = Color::from_rgba(239, 241, 245, 255);
    let muted = Color::from_rgba(174, 179, 188, 255);
    let button_color = Color::from_rgba(52, 57, 65, 255);
    let active_color = Color::from_rgba(64, 104, 150, 255);
    let tool_color = |tool| if ui.tool == tool { active_color } else { button_color };

    if let Some(block) = ui.selected_block.and_then(|index| blocks.get(index)) {
        let matrix = studio_camera(player, orbit).matrix();
        let center = project_point(&matrix, block.center());
        for handle in studio_handles(block, ui.tool) {
            if let Some(point) = project_point(&matrix, handle.world) {
                let hovered = !ui.publish_open && point.distance(mouse) <= HANDLE_RADIUS + 4.0;
                draw_handle(center, &handle, point, ui.tool, hovered);
            }
        }
    }

    draw_rectangle(
        0.0,
        0.0,
        width,
        studio_top_height(),
        Color::from_rgba(24, 25, 28, 255),
    );
    draw_text("E BLOCKS STUDIO", 14.0 * scale, 32.0 * scale, 20.0 * scale, text);
    draw_studio_button(studio_button(0), "ADD", Color::from_rgba(44, 139, 79, 255), scale);
    draw_studio_button(studio_button(1), "MOVE", tool_color(StudioTool::Move), scale);
    draw_studio_button(studio_button(2), "SCALE", tool_color(StudioTool::Scale), scale);
    draw_studio_button(studio_button(3), "ROTATE", tool_color(StudioTool::Rotate), scale);
    draw_studio_button(studio_button(4), "DELETE", Color::from_rgba(150, 56, 56, 255), scale);
    draw_studio_button(
        studio_button(5),
        &format!("STEP {}", STEP_SIZES[ui.step_index]),
        button_color,
        scale,
    );
    draw_studio_button(
        studio_button(6),
        if ui.tools_enabled { "TOOLS: ON" } else { "TOOLS: OFF" },
        if ui.tools_enabled { Color::from_rgba(44, 139, 79, 255) } else { button_color },
        scale,
    );
    draw_studio_button(
        studio_publish_button(),
        "PUBLISH",
        Color::from_rgba(44, 139, 79, 255),
        scale,
    );
    draw_studio_button(
        studio_exit_button(),
        "EXIT",
        Color::from_rgba(73, 77, 90, 255),
        scale,
    );

    let hint = match ui.tool {
        StudioTool::Move => "MOVE: click a block, then a colored arrow to push it that way.",
        StudioTool::Scale => {
            "SCALE: click a colored square to make that side longer (hold Shift to shrink)."
        }
        StudioTool::Rotate => "ROTATE: click a colored dot to turn the block around that color.",
    };
    draw_text(hint, 14.0 * scale, studio_top_height() + 20.0 * scale, 15.0 * scale, text);
    draw_text(
        "Red = X, Green = Y (up/down), Blue = Z.   WASD walk   Right-drag camera   Delete removes a block.",
        14.0 * scale,
        studio_top_height() + 38.0 * scale,
        13.0 * scale,
        muted,
    );

    let bottom = studio_bottom_height();
    draw_rectangle(
        0.0,
        height - bottom,
        width,
        bottom,
        Color::from_rgba(24, 25, 28, 255),
    );
    draw_text(
        "COLOR",
        studio_swatch(0).x - 64.0 * scale,
        height - bottom + 34.0 * scale,
        15.0 * scale,
        muted,
    );
    let selected_block = ui.selected_block.and_then(|index| blocks.get(index));
    let current_color = selected_block.map_or(selected_color, |block| block.color_index);
    for (index, color) in BODY_COLORS.iter().enumerate() {
        let rect = studio_swatch(index);
        draw_rectangle(rect.x, rect.y, rect.w, rect.h, *color);
        let selected = current_color == index;
        draw_rectangle_lines(
            rect.x,
            rect.y,
            rect.w,
            rect.h,
            if selected { 4.0 } else { 1.0 },
            if selected {
                WHITE
            } else {
                Color::from_rgba(16, 17, 19, 255)
            },
        );
    }
    let info = match selected_block {
        Some(block) => format!(
            "Block selected: size {:.2} x {:.2} x {:.2}",
            block.size[0], block.size[1], block.size[2]
        ),
        None => format!(
            "{} blocks. Click a block to select it, or press ADD.",
            blocks.len()
        ),
    };
    draw_text(&info, 14.0 * scale, height - bottom + 22.0 * scale, 14.0 * scale, text);
    draw_text(status, 14.0 * scale, height - bottom + 44.0 * scale, 13.0 * scale, muted);

    if ui.publish_open {
        draw_rectangle(0.0, 0.0, width, height, Color::new(0.0, 0.0, 0.0, 0.45));
        let dialog = studio_dialog();
        draw_rectangle(
            dialog.x,
            dialog.y,
            dialog.w,
            dialog.h,
            Color::from_rgba(30, 31, 35, 255),
        );
        draw_rectangle_lines(
            dialog.x,
            dialog.y,
            dialog.w,
            dialog.h,
            1.5 * scale,
            Color::from_rgba(96, 103, 117, 255),
        );
        draw_text(
            "Publish your game",
            dialog.x + 18.0 * scale,
            dialog.y + 27.0 * scale,
            18.0 * scale,
            text,
        );
        draw_text(
            "Choose a name so other players can find and play it.",
            dialog.x + 18.0 * scale,
            dialog.y + 49.0 * scale,
            12.0 * scale,
            muted,
        );
        let field = studio_dialog_name_field();
        draw_rectangle(field.x, field.y, field.w, field.h, Color::from_rgba(19, 20, 23, 255));
        draw_rectangle_lines(
            field.x,
            field.y,
            field.w,
            field.h,
            scale,
            if ui.publish_focused {
                Color::from_rgba(85, 151, 223, 255)
            } else {
                Color::from_rgba(74, 77, 84, 255)
            },
        );
        draw_text(
            if ui.publish_name.is_empty() {
                "Game name (3-40 characters)"
            } else {
                ui.publish_name.as_str()
            },
            field.x + 9.0 * scale,
            field.y + 21.0 * scale,
            14.0 * scale,
            if ui.publish_name.is_empty() { muted } else { text },
        );
        draw_studio_button(
            studio_dialog_publish(),
            "Publish Game",
            Color::from_rgba(44, 139, 79, 255),
            scale,
        );
        draw_studio_button(studio_dialog_cancel(), "Cancel", button_color, scale);
    }
}

// Blocks are solid in the studio and in published games.
pub fn apply_block_collisions(blocks: &[StudioBlock], previous: Vec3, player: &mut Player) {
    let previous_y = previous.y;
    if player.velocity.y <= 0.0 {
        let landing = blocks
            .iter()
            .filter(|block| block.contains_xz(player.position.x, player.position.z, 0.25))
            .map(|block| block.vertical_range().1)
            .filter(|top| previous_y >= top - 0.05 && player.position.y <= *top)
            .max_by(f32::total_cmp);
        if let Some(top) = landing {
            player.position.y = top;
            player.velocity.y = 0.0;
            player.grounded = true;
        }
    } else {
        let head = player.position.y + PLAYER_HEIGHT;
        let bump = blocks.iter().any(|block| {
            let (bottom, _) = block.vertical_range();
            block.contains_xz(player.position.x, player.position.z, 0.25)
                && previous_y + PLAYER_HEIGHT <= bottom + 0.05
                && head > bottom
        });
        if bump {
            player.velocity.y = 0.0;
        }
    }

    let blocked = |x: f32, z: f32, y: f32| {
        blocks.iter().any(|block| {
            let (bottom, top) = block.vertical_range();
            y + PLAYER_HEIGHT > bottom + 0.05
                && y < top - 0.2
                && block.contains_xz(x, z, PLAYER_RADIUS)
        })
    };
    let position = player.position;
    if blocked(position.x, position.z, position.y) {
        if !blocked(previous.x, position.z, position.y) {
            player.position.x = previous.x;
        } else if !blocked(position.x, previous.z, position.y) {
            player.position.z = previous.z;
        } else {
            player.position.x = previous.x;
            player.position.z = previous.z;
        }
    }

    if player.grounded && player.position.y > 0.001 {
        let supported = blocks.iter().any(|block| {
            block.contains_xz(player.position.x, player.position.z, 0.25)
                && (block.vertical_range().1 - player.position.y).abs() < 0.05
        });
        if !supported {
            player.grounded = false;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_player(position: Vec3) -> Player {
        Player {
            position,
            velocity: Vec3::ZERO,
            grounded: false,
            walk_phase: 0.0,
            walk_amount: 0.0,
            facing_yaw: 0.0,
        }
    }

    #[test]
    fn scaling_one_side_keeps_the_opposite_side_fixed() {
        let mut block = StudioBlock::new(vec3(0.0, 0.5, 0.0), 0);
        assert!(block.resize_side(0, 1.0, 1.0));
        assert_eq!(block.size[0], 3.0);
        assert!((block.center().x - 0.5).abs() < 1e-5);
        assert!(block.resize_side(0, -1.0, 2.0));
        assert!((block.center().x + 0.5).abs() < 1e-5);
        assert_eq!(block.size[0], 5.0);
    }

    #[test]
    fn scaling_follows_the_rotated_side() {
        let mut block = StudioBlock::new(vec3(0.0, 0.5, 0.0), 0);
        block.rotate_axis(1, std::f32::consts::FRAC_PI_2);
        block.resize_side(0, 1.0, 2.0);
        let center = block.center();
        assert!(center.x.abs() < 1e-4 && (center.z.abs() - 1.0).abs() < 1e-4);
    }

    #[test]
    fn sizes_are_limited() {
        let mut block = StudioBlock::new(Vec3::ZERO, 0);
        for _ in 0..40 {
            block.resize_side(1, 1.0, 1.0);
        }
        assert_eq!(block.size[1], MAX_BLOCK_SIZE);
        for _ in 0..80 {
            block.resize_side(1, 1.0, -1.0);
        }
        assert_eq!(block.size[1], MIN_BLOCK_SIZE);
        assert!(block.is_valid());
    }

    #[test]
    fn handles_move_and_rotate_blocks() {
        let mut block = StudioBlock::new(vec3(0.0, 0.5, 0.0), 0);
        let handles = studio_handles(&block, StudioTool::Move);
        let up = handles
            .iter()
            .find(|handle| handle.axis == 1 && handle.sign > 0.0)
            .unwrap();
        assert!(apply_handle(&mut block, StudioTool::Move, up, 0.5, false));
        assert!((block.center().y - 1.0).abs() < 1e-5);
        let rotate = StudioHandle {
            axis: 2,
            sign: 1.0,
            world: Vec3::ZERO,
        };
        apply_handle(&mut block, StudioTool::Rotate, &rotate, 1.0, false);
        assert!((block.tilt[1] - ROTATE_STEP).abs() < 1e-5);
    }

    #[test]
    fn rays_pick_the_closest_block() {
        let near = StudioBlock::new(vec3(0.0, 0.5, 0.0), 0);
        let far = StudioBlock::new(vec3(0.0, 0.5, -10.0), 0);
        let origin = vec3(0.0, 0.5, 10.0);
        let direction = vec3(0.0, 0.0, -1.0);
        let near_hit = near.ray_hit(origin, direction).unwrap();
        assert!(near_hit < far.ray_hit(origin, direction).unwrap());
        assert!(near.ray_hit(origin, vec3(1.0, 0.0, 0.0)).is_none());
    }

    #[test]
    fn older_integer_blocks_still_load() {
        let block: StudioBlock =
            serde_json::from_str(r#"{"position":[1,2,3],"color_index":2}"#).unwrap();
        assert_eq!(block.position, [1.0, 2.0, 3.0]);
        assert_eq!(block.size, [1.0; 3]);
        assert!(block.is_valid());
    }

    #[test]
    fn players_land_on_and_are_stopped_by_blocks() {
        let blocks = [StudioBlock::new(vec3(0.0, 0.5, 0.0), 0)];
        let mut player = test_player(vec3(0.0, 0.98, 0.0));
        player.velocity.y = -3.0;
        apply_block_collisions(&blocks, vec3(0.0, 1.05, 0.0), &mut player);
        assert_eq!(player.position.y, 1.0);
        assert!(player.grounded);

        let mut walker = test_player(vec3(-1.2, 0.0, 0.0));
        walker.grounded = true;
        apply_block_collisions(&blocks, vec3(-1.5, 0.0, 0.0), &mut walker);
        assert_eq!(walker.position.x, -1.5);

        let mut walked_off = test_player(vec3(5.0, 1.0, 0.0));
        walked_off.grounded = true;
        apply_block_collisions(&blocks, vec3(4.9, 1.0, 0.0), &mut walked_off);
        assert!(!walked_off.grounded);
    }

    #[test]
    fn block_meshes_stay_within_draw_limits() {
        let mut vertices = Vec::new();
        let mut indices = Vec::new();
        for _ in 0..BLOCKS_PER_MESH {
            push_block_mesh(&StudioBlock::new(Vec3::ZERO, 0), &mut vertices, &mut indices);
        }
        assert!(vertices.len() <= 10_000 && indices.len() <= 5_000);
    }
}
