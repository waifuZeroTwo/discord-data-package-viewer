const { contextBridge, ipcRenderer } = require('electron')
const { IMPORT_STATES } = require('./src/shared/importStates')

contextBridge.exposeInMainWorld('ddpApi', {
  pickDataPackage: () => ipcRenderer.invoke('dialog:pick-data-package'),
  selectZip: (options) => ipcRenderer.invoke('dialog:select-zip', options),
  getParserAnalytics: (options) => ipcRenderer.invoke('parser:get-analytics', options),
  cancelParserJob: (options) => ipcRenderer.invoke('parser:cancel-job', options),
  onParserProgress: (callback) => {
    const listener = (_event, payload) => {
      callback(payload)
    }
    ipcRenderer.on('parser:progress', listener)
    return () => ipcRenderer.removeListener('parser:progress', listener)
  },
  onImportStatus: (callback) => {
    const listener = (_event, payload) => {
      callback(payload)
    }
    ipcRenderer.on('import:status', listener)
    return () => ipcRenderer.removeListener('import:status', listener)
  },
  importStates: IMPORT_STATES,
  getRuntimeVersions: () => ({
    electron: process.versions.electron,
    chrome: process.versions.chrome,
    node: process.versions.node,
  }),
})
