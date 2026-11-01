# Edge-Cloud Digital Twin Dashboard

React + Vite dashboard for the existing Edge-Cloud Digital Twin Docker project.

## What it connects to

- Edge: `GET /health`, `GET /metrics`, `POST /network`, `POST /publish`
- Cloud: `GET /health`, `GET /metrics`, `GET /state`

The Vite development server proxies `/api/edge/*` to `http://localhost:8080` and `/api/cloud/*` to `http://localhost:8081`, so no backend CORS changes are required.

## Run

1. Extract this `dashboard` folder into the root of `edge_cloud_digital_twin`, alongside `cloud`, `edge`, `mosquitto`, and `docker-compose.yml`.
2. Make sure Docker Desktop is running and your existing services are up:
   ```powershell
   docker compose up -d
   docker compose ps
   ```
3. Open a second VS Code terminal:
   ```powershell
   cd C:\Users\Dell\Desktop\edge_cloud_digital_twin\dashboard
   npm install
   npm run dev
   ```
4. Open `http://localhost:5173`.

## Notes

- The dashboard polls the APIs every 2 seconds.
- Network buttons call `POST /network` with `{ "online": false }` or `{ "online": true }`.
- Publish calls `POST /publish` with a twin ID and numeric value.
- The chart records newly observed Cloud state event IDs during the current browser session. The Cloud `/state` API returns only the latest row per twin, so this is not a full historical time-series chart.
- The sensor service in the supplied Compose file uses the `sensor` profile. If it is not running and you want automatic sensor events, start it with:
  ```powershell
  docker compose --profile sensor up -d
  ```
