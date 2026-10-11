# E Blocks

E Blocks is a Rust game-hub prototype inspired by the blocky worlds and avatars in the supplied reference image.

## Play

In PowerShell, open the project directory:

```powershell
cd "$env:USERPROFILE\Documents\PlayVortex"
$env:PATH = "C:\msys64\ucrt64\bin;$env:USERPROFILE\.cargo\bin;$env:PATH"
```

Start the local game server in one terminal. The database is saved in the current project directory:

```powershell
$secret = [Convert]::ToHexString([Security.Cryptography.RandomNumberGenerator]::GetBytes(32))
$env:PLAYVORTEX_JWT_SECRET = $secret
$env:PLAYVORTEX_BIND = "127.0.0.1:3000"
cargo run -p playvortex-server
```

Open a second terminal, set the same project directory and `PATH`, and launch the client:

```powershell
cargo run -p playvortex-client
```

Create an account in the sign-in screen. Accounts and saved worlds stay on this computer by default. In the hub, use **Search** to look up a registered username or find built-in and published games by name; searches show a friendly not-found message when there is no match. Play Sunset Plaza, Block Bash, Sky Sprint, the imported NPC Park map, the **Obby Rift** obstacle course (jump across the platforms; falling respawns you at the start), or **Coastal Drive**. In Coastal Drive, cruise a giant island with a sweeping coastal loop road, a city skyline, forests, and hills. **WASD** or the arrow keys accelerate, reverse, and steer; the speedometer shows your speed, and **Esc** returns to the hub. Other worlds use **WASD** to move relative to the camera, **Space** to jump, and right mouse drag to orbit the camera. Your avatar uses the supplied Dynabloxx model. Click **Avatar Catalog** to equip or remove the red hair, cone hat, or back sword, or visit the **Colors** tab to customize your avatar. Your selection is saved locally, appears in every world, and is shared with other players on the updated server. The in-game **Avatar** button also lets you switch gear.

To play without creating an account, choose **Play as Guest** on the sign-in screen. Guest play uses the same fixed Dynabloxx model with default colors; it cannot be customized and has **GUEST** displayed on its back. Guest games run locally without multiplayer, community games, or Studio publishing.

## Studio

Choose **Open Studio** to build a world out of textured blocks. Press **ADD** to spawn a block in front of you, then click a block to select it. Coloured handles appear around it (red = X, green = up/down, blue = Z): with **SCALE**, click a handle to make the block longer on that side (hold **Shift** to shrink); with **MOVE**, click a handle to push the block that way; with **ROTATE**, click a handle to turn it around that axis. **TOOLS: ON/OFF** decides whether players get the Sword and Jump Coil in your published game (off by default; they are removed from the game unless you turn it on). **STEP** changes how far each click moves or grows a block, **DELETE** removes it, and the colour swatches at the bottom recolour it. Walk with **WASD**/**Space** and drag the **right mouse button** to orbit the camera. Click **PUBLISH**, name your game, and other signed-in players can find it in the hub and play it (blocks are solid). Blocks save automatically.

Choose **Publish**, enter a 3â€“40 character game name, and publish it to the configured server. Published games appear in **Community Games** on the hub so other players using that server can refresh the catalog and play them. Publishing requires a signed-in account and an available server. The included local server is reachable only on this computer by default; a public server must be deployed and configured before people on other devices can join.

## Chat and multiplayer

Open chat by clicking its input or pressing **/**, type a message, and press **Enter** or the send button. Players logged into the same server and experience can see one another and chat.

The included server listens on this computer only by default in the example above. To play over a network, deploy the Rust server to a trusted host, configure a strong persistent `PLAYVORTEX_JWT_SECRET`, set `PLAYVORTEX_BIND` as needed, and set `PLAYVORTEX_SERVER_URL` on each client. Use HTTPS/WSS when exposing the server publicly; never share the signing secret.

Avatar color and your Studio world are saved under `%LOCALAPPDATA%\EBlocks`.
