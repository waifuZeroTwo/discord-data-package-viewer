const path = require('path')
const fs = require('fs/promises')
const {
  extractPremiumEventsFromPayload,
  normalizePremiumEventsToIntervals,
} = require('./premiumHistory')
const { mapLegacyBadge, mapPremiumType, mapPublicFlags } = require('../../shared/badges')

const DEFAULT_SORT_DIRECTION = 'asc'

function normalizeSortDirection(sortDirection) {
  return sortDirection === 'desc' ? 'desc' : DEFAULT_SORT_DIRECTION
}

async function collectJsonFiles(rootPath) {
  const files = []
  const stack = [rootPath]

  while (stack.length > 0) {
    const currentPath = stack.pop()
    const entries = await fs.readdir(currentPath, { withFileTypes: true })

    for (const entry of entries) {
      const fullPath = path.join(currentPath, entry.name)
      if (entry.isDirectory()) {
        stack.push(fullPath)
      } else if (entry.isFile() && entry.name.toLowerCase().endsWith('.json')) {
        files.push(fullPath)
      }
    }
  }

  return files
}

function normalizeChannel(rawChannel, sourcePath, warnings) {
  if (!rawChannel || typeof rawChannel !== 'object') {
    return null
  }

  const id = rawChannel.id ?? rawChannel.channelId ?? rawChannel.channel_id
  const name = rawChannel.name ?? rawChannel.channelName ?? rawChannel.channel_name
  const type = rawChannel.type ?? rawChannel.channelType ?? rawChannel.channel_type ?? 'unknown'

  if (id === undefined || id === null || id === '') {
    warnings.push(`Skipped channel without id in ${sourcePath}`)
    return null
  }

  return {
    id: String(id),
    name: typeof name === 'string' && name.trim() ? name : `Channel ${id}`,
    type: typeof type === 'string' && type.trim() ? type : 'unknown',
  }
}

function parseTimestamp(rawValue) {
  if (rawValue === undefined || rawValue === null || rawValue === '') {
    return { value: null, epochMs: Number.NaN }
  }

  const isoString = rawValue instanceof Date ? rawValue.toISOString() : String(rawValue)
  const epochMs = Date.parse(isoString)
  if (Number.isNaN(epochMs)) {
    return { value: null, epochMs: Number.NaN }
  }

  return { value: new Date(epochMs).toISOString(), epochMs }
}

function normalizeAttachments(rawMessage) {
  const attachmentCollection = rawMessage.attachments ?? rawMessage.files ?? []
  if (!Array.isArray(attachmentCollection)) {
    return []
  }

  return attachmentCollection
    .map((attachment) => {
      if (!attachment || typeof attachment !== 'object') {
        return null
      }

      return {
        id: attachment.id ? String(attachment.id) : null,
        filename: attachment.filename ?? attachment.fileName ?? null,
        url: attachment.url ?? attachment.proxy_url ?? attachment.proxyUrl ?? null,
        size: Number.isFinite(attachment.size) ? attachment.size : null,
      }
    })
    .filter(Boolean)
}

function normalizeMessage(rawMessage, sourcePath, warnings) {
  if (!rawMessage || typeof rawMessage !== 'object') {
    return null
  }

  const channelId = rawMessage.channelId ?? rawMessage.channel_id ?? rawMessage.channel?.id
  if (!channelId) {
    warnings.push(`Skipped message without channel id in ${sourcePath}`)
    return null
  }

  const authorName =
    rawMessage.author?.name ??
    rawMessage.author?.username ??
    rawMessage.author_name ??
    rawMessage.username ??
    'Unknown Author'

  const timestampInput = rawMessage.timestamp ?? rawMessage.created_at ?? rawMessage.createdAt
  const timestamp = parseTimestamp(timestampInput)

  if (timestampInput && !timestamp.value) {
    warnings.push(`Message in ${sourcePath} had invalid timestamp: ${String(timestampInput)}`)
  }

  return {
    id: rawMessage.id ? String(rawMessage.id) : null,
    channelId: String(channelId),
    author: typeof authorName === 'string' ? authorName : 'Unknown Author',
    content: typeof rawMessage.content === 'string' ? rawMessage.content : '',
    timestamp: timestamp.value,
    timestampEpochMs: timestamp.epochMs,
    attachments: normalizeAttachments(rawMessage),
  }
}

function maybeExtractChannels(payload, sourcePath, warnings) {
  if (!payload || typeof payload !== 'object') {
    return []
  }

  const candidates = []

  if (Array.isArray(payload.channels)) {
    candidates.push(...payload.channels)
  }

  if (Array.isArray(payload.guilds)) {
    for (const guild of payload.guilds) {
      if (Array.isArray(guild?.channels)) {
        candidates.push(...guild.channels)
      }
    }
  }

  if (payload.id && (payload.type || payload.name) && !Array.isArray(payload)) {
    candidates.push(payload)
  }

  return candidates.map((item) => normalizeChannel(item, sourcePath, warnings)).filter(Boolean)
}

function maybeExtractMessages(payload, sourcePath, warnings) {
  const messageCandidates = []

  if (Array.isArray(payload)) {
    messageCandidates.push(...payload)
  } else if (payload && typeof payload === 'object') {
    if (Array.isArray(payload.messages)) {
      messageCandidates.push(...payload.messages)
    }

    if (payload.message && typeof payload.message === 'object') {
      messageCandidates.push(payload.message)
    }
  }

  return messageCandidates.map((item) => normalizeMessage(item, sourcePath, warnings)).filter(Boolean)
}

function sortMessages(messages, sortDirection) {
  const direction = normalizeSortDirection(sortDirection)
  const comparator = direction === 'desc' ? -1 : 1

  messages.sort((a, b) => {
    const aEpoch = Number.isNaN(a.timestampEpochMs) ? Number.MAX_SAFE_INTEGER : a.timestampEpochMs
    const bEpoch = Number.isNaN(b.timestampEpochMs) ? Number.MAX_SAFE_INTEGER : b.timestampEpochMs

    if (aEpoch === bEpoch) {
      return 0
    }

    return aEpoch > bEpoch ? comparator : -comparator
  })
}

function extractBadgeCandidates(payload, jsonPath, sink = []) {
  if (!payload || typeof payload !== 'object') {
    return sink
  }

  if (Array.isArray(payload)) {
    payload.forEach((entry) => extractBadgeCandidates(entry, jsonPath, sink))
    return sink
  }

  const hasBadgeKeys =
    payload.public_flags !== undefined ||
    payload.publicFlags !== undefined ||
    payload.flags !== undefined ||
    payload.premium_type !== undefined ||
    payload.premiumType !== undefined ||
    Array.isArray(payload.badges) ||
    Array.isArray(payload.legacy_badges) ||
    Array.isArray(payload.event_badges)

  if (hasBadgeKeys) {
    sink.push({ candidate: payload, jsonPath })
  }

  for (const value of Object.values(payload)) {
    if (value && typeof value === 'object') {
      extractBadgeCandidates(value, jsonPath, sink)
    }
  }

  return sink
}

function extractBadgesFromPayload(payload, jsonPath) {
  const badgeCandidates = extractBadgeCandidates(payload, jsonPath)
  const badges = []

  for (const { candidate } of badgeCandidates) {
    badges.push(...mapPublicFlags(candidate.public_flags ?? candidate.publicFlags ?? candidate.flags))
    badges.push(...mapPremiumType(candidate.premium_type ?? candidate.premiumType))

    const legacyCollections = [candidate.badges, candidate.legacy_badges, candidate.event_badges]
    for (const collection of legacyCollections) {
      if (!Array.isArray(collection)) {
        continue
      }

      for (const entry of collection) {
        const rawValue =
          entry && typeof entry === 'object'
            ? entry.id ?? entry.code ?? entry.name ?? entry.badge ?? entry.value
            : entry
        const mapped = mapLegacyBadge(rawValue)
        if (mapped) {
          badges.push(mapped)
        }
      }
    }
  }

  return badges.map((badge) => ({ ...badge, source: jsonPath }))
}

function getPaginatedParserOutput(parsedExport, options = {}) {
  const channelPage = Math.max(1, Number(options.channelPage) || 1)
  const channelPageSize = Math.max(1, Number(options.channelPageSize) || 25)
  const messagePage = Math.max(1, Number(options.messagePage) || 1)
  const messagePageSize = Math.max(1, Number(options.messagePageSize) || 200)

  const channelStart = (channelPage - 1) * channelPageSize
  const channelEnd = channelStart + channelPageSize
  const channels = parsedExport.channels.slice(channelStart, channelEnd)

  const messagesByChannel = {}
  const messagePageInfoByChannel = {}

  for (const channel of channels) {
    const allMessages = parsedExport.messagesByChannel[channel.id] ?? []
    const msgStart = (messagePage - 1) * messagePageSize
    const msgEnd = msgStart + messagePageSize

    messagesByChannel[channel.id] = allMessages.slice(msgStart, msgEnd).map((message) => ({
      ...message,
      timestampEpochMs: undefined,
    }))

    messagePageInfoByChannel[channel.id] = {
      page: messagePage,
      pageSize: messagePageSize,
      totalMessages: allMessages.length,
      hasMore: msgEnd < allMessages.length,
    }
  }

  return {
    channels,
    messagesByChannel,
    premiumHistory: parsedExport.premiumHistory ?? [],
    badges: parsedExport.badges ?? [],
    warnings: parsedExport.warnings,
    channelPageInfo: {
      page: channelPage,
      pageSize: channelPageSize,
      totalChannels: parsedExport.channels.length,
      hasMore: channelEnd < parsedExport.channels.length,
    },
    messagePageInfoByChannel,
  }
}

async function parseDiscordExport(rootPath, options = {}) {
  const warnings = []
  const channelsById = new Map()
  const messagesByChannel = {}
  const sortDirection = normalizeSortDirection(options.sortDirection)
  const premiumEvents = []
  const badges = []

  let jsonFiles = []

  try {
    jsonFiles = await collectJsonFiles(rootPath)
  } catch (error) {
    return {
      channels: [],
      messagesByChannel: {},
      premiumHistory: [],
      badges: [],
      warnings: [`Failed to scan extracted archive: ${error.message}`],
      sortDirection,
    }
  }

  if (jsonFiles.length === 0) {
    warnings.push('No JSON files were found in the extracted archive.')
  }

  for (const jsonPath of jsonFiles) {
    let rawContent = null

    try {
      rawContent = await fs.readFile(jsonPath, 'utf8')
    } catch (error) {
      warnings.push(`Unable to read ${jsonPath}: ${error.message}`)
      continue
    }

    let payload
    try {
      payload = JSON.parse(rawContent)
    } catch (error) {
      warnings.push(`Malformed JSON in ${jsonPath}: ${error.message}`)
      continue
    }

    const channels = maybeExtractChannels(payload, jsonPath, warnings)
    for (const channel of channels) {
      channelsById.set(channel.id, channel)
      if (!messagesByChannel[channel.id]) {
        messagesByChannel[channel.id] = []
      }
    }

    premiumEvents.push(...extractPremiumEventsFromPayload(payload, jsonPath, warnings))
    badges.push(...extractBadgesFromPayload(payload, jsonPath))

    const messages = maybeExtractMessages(payload, jsonPath, warnings)
    for (const message of messages) {
      if (!messagesByChannel[message.channelId]) {
        messagesByChannel[message.channelId] = []
      }

      if (!channelsById.has(message.channelId)) {
        channelsById.set(message.channelId, {
          id: message.channelId,
          name: `Channel ${message.channelId}`,
          type: 'unknown',
        })
      }

      messagesByChannel[message.channelId].push(message)
    }
  }

  for (const channelId of Object.keys(messagesByChannel)) {
    sortMessages(messagesByChannel[channelId], sortDirection)
  }

  const channels = Array.from(channelsById.values()).sort((a, b) => a.name.localeCompare(b.name))
  const premiumHistory = normalizePremiumEventsToIntervals(premiumEvents, warnings)
  const dedupedBadges = Array.from(
    new Map(
      badges.map((badge) => [
        `${badge.sourceType}:${badge.rawValue}:${badge.displayName}:${badge.iconKey}`,
        badge,
      ]),
    ).values(),
  ).sort((a, b) => a.displayName.localeCompare(b.displayName))

  return {
    channels,
    messagesByChannel,
    premiumHistory,
    badges: dedupedBadges,
    warnings,
    sortDirection,
  }
}

module.exports = {
  parseDiscordExport,
  getPaginatedParserOutput,
}
