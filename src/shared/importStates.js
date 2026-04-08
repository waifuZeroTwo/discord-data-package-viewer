const IMPORT_STATES = Object.freeze({
  IDLE: 'idle',
  SELECTING_FILE: 'selecting_file',
  EXTRACTING_ZIP: 'extracting_zip',
  SCANNING_FILES: 'scanning_files',
  PARSING: 'parsing',
  AGGREGATING: 'aggregating',
  STALLED: 'stalled',
  COMPLETED: 'completed',
  FAILED: 'failed',
  CANCELED: 'canceled',
})

const TERMINAL_IMPORT_STATES = new Set([
  IMPORT_STATES.COMPLETED,
  IMPORT_STATES.FAILED,
  IMPORT_STATES.CANCELED,
])

module.exports = {
  IMPORT_STATES,
  TERMINAL_IMPORT_STATES,
}
