# Discord Data Package Viewer

## Migration note: analytics-only mode

As of this migration, the app is **analytics-only**:

- Channel transcript browsing is disabled.
- Full message timeline rendering is disabled.
- IPC/data flow no longer returns full channel message bodies to the renderer.

### Why this changed

The previous channel/timeline experience required shipping large message payloads from the main process to the renderer, which increased memory pressure and UI latency on large Discord exports. The analytics-only architecture keeps extraction focused on aggregate insights (badges, premium history, billing, connections, emoji usage), which improves responsiveness and reduces the risk of exposing full transcripts in the UI layer.

### What remains supported

- ZIP import and archive validation
- Analytics extraction across supported export sections
- Dashboard panels for premium history, billing/Nitro, badges, connections, and emoji analytics
