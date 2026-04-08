# Discord Data Package Viewer

Desktop Electron app for loading and exploring a Discord data export ZIP.

## Scripts

- `npm start` – run the Electron app locally.
- `npm run lint` – syntax-check the main process, preload, and parser files.
- `npm run build` – create an unpacked production build using `electron-builder --dir`.
- `npm run dist` – create distributable installers/artifacts using `electron-builder`.

## Usage

1. Install dependencies:

   ```bash
   npm install
   ```

2. Start the app in development:

   ```bash
   npm start
   ```

3. Run lint checks before committing:

   ```bash
   npm run lint
   ```

4. Build packaging artifacts:

   ```bash
   npm run build
   npm run dist
   ```
