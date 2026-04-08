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

function extractCustomEmojiMatches(text) {
  if (typeof text !== 'string' || text.length === 0) {
    return []
  }

  const matches = []
  const pattern = /<(?<animated>a?):(?<name>[a-zA-Z0-9_~]+):(?<id>\d+)>/g
  for (const match of text.matchAll(pattern)) {
    const groups = match.groups || {}
    matches.push({
      type: 'custom',
      name: groups.name || 'emoji',
      customId: groups.id || null,
      animated: groups.animated === 'a',
      raw: match[0],
      count: 1,
    })
  }

  return matches
}

function extractUnicodeEmojiMatches(text) {
  if (typeof text !== 'string' || text.length === 0) {
    return []
  }

  const matches = []
  const pattern =
    /(?:\p{Regional_Indicator}{2}|[#*0-9]\uFE0F?\u20E3|(?:\p{Extended_Pictographic}(?:\uFE0F|\uFE0E)?(?:\p{Emoji_Modifier})?)(?:\u200D(?:\p{Extended_Pictographic}(?:\uFE0F|\uFE0E)?(?:\p{Emoji_Modifier})?))*)/gu
  for (const match of text.matchAll(pattern)) {
    matches.push({
      type: 'unicode',
      unicode: match[0],
      raw: match[0],
      count: 1,
    })
  }

  return matches
}

function normalizeReactions(rawMessage) {
  const collection = rawMessage.reactions ?? []
  if (!Array.isArray(collection)) {
    return []
  }

  return collection
    .map((reaction) => {
      if (!reaction || typeof reaction !== 'object') {
        return null
      }

      const emoji = reaction.emoji && typeof reaction.emoji === 'object' ? reaction.emoji : reaction
      const count = Number.isFinite(reaction.count) ? Math.max(1, reaction.count) : 1
      const customId = emoji.id ? String(emoji.id) : null
      const unicode = !customId && typeof emoji.name === 'string' ? emoji.name : null
      const name = typeof emoji.name === 'string' && emoji.name ? emoji.name : 'emoji'

      return {
        type: customId ? 'custom' : 'unicode',
        name,
        customId,
        unicode,
        animated: Boolean(emoji.animated),
        raw: customId ? `<${emoji.animated ? 'a' : ''}:${name}:${customId}>` : unicode ?? name,
        count,
      }
    })
    .filter(Boolean)
}

function extractEmojiUsage(rawMessage) {
  const content = typeof rawMessage.content === 'string' ? rawMessage.content : ''
  return [
    ...extractCustomEmojiMatches(content),
    ...extractUnicodeEmojiMatches(content),
    ...normalizeReactions(rawMessage),
  ]
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
    emojiUsage: extractEmojiUsage(rawMessage),
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

function getEmojiAssetUrl(item) {
  if (item.type !== 'custom' || !item.customId) {
    return null
  }

  const extension = item.animated ? 'gif' : 'png'
  return `https://cdn.discordapp.com/emojis/${item.customId}.${extension}?size=64&quality=lossless`
}

function buildEmojiDatasets(messagesByChannel) {
  const index = new Map()

  for (const [channelId, messages] of Object.entries(messagesByChannel)) {
    for (const message of messages) {
      if (!Array.isArray(message.emojiUsage)) {
        continue
      }

      for (const usage of message.emojiUsage) {
        const key =
          usage.type === 'custom' && usage.customId
            ? `custom:${usage.customId}`
            : `unicode:${usage.unicode || usage.raw || 'unknown'}`
        const existing =
          index.get(key) ||
          {
            key,
            type: usage.type,
            name: usage.name || usage.unicode || usage.raw || 'emoji',
            unicode: usage.unicode || null,
            customId: usage.customId || null,
            animated: Boolean(usage.animated),
            raw: usage.raw || '',
            totalUses: 0,
            lastUsedTimestamp: null,
            lastUsedEpochMs: Number.NaN,
            channelIds: new Set(),
          }

        existing.totalUses += Number.isFinite(usage.count) ? usage.count : 1
        existing.channelIds.add(channelId)

        if (!Number.isNaN(message.timestampEpochMs)) {
          if (Number.isNaN(existing.lastUsedEpochMs) || message.timestampEpochMs > existing.lastUsedEpochMs) {
            existing.lastUsedEpochMs = message.timestampEpochMs
            existing.lastUsedTimestamp = message.timestamp
          }
        }

        index.set(key, existing)
      }
    }
  }

  const normalized = Array.from(index.values()).map((item) => ({
    key: item.key,
    type: item.type,
    name: item.name,
    unicode: item.unicode,
    customId: item.customId,
    animated: item.animated,
    raw: item.raw,
    totalUses: item.totalUses,
    lastUsedTimestamp: item.lastUsedTimestamp,
    channelCount: item.channelIds.size,
    assetUrl: getEmojiAssetUrl(item),
  }))

  const recentEmojis = [...normalized].sort((a, b) => {
    const aEpoch = a.lastUsedTimestamp ? Date.parse(a.lastUsedTimestamp) : Number.NEGATIVE_INFINITY
    const bEpoch = b.lastUsedTimestamp ? Date.parse(b.lastUsedTimestamp) : Number.NEGATIVE_INFINITY
    return bEpoch - aEpoch || b.totalUses - a.totalUses
  })

  const favoriteEmojis = [...normalized].sort((a, b) => b.totalUses - a.totalUses || a.name.localeCompare(b.name))

  return { recentEmojis, favoriteEmojis }
}

function normalizeConnection(rawConnection, sourcePath, warnings) {
  if (!rawConnection || typeof rawConnection !== 'object') {
    return null
  }

  const type = rawConnection.type ?? rawConnection.service ?? rawConnection.provider
  const name =
    rawConnection.name ??
    rawConnection.username ??
    rawConnection.display_name ??
    rawConnection.displayName
  const id = rawConnection.id ?? rawConnection.account_id ?? rawConnection.accountId
  const visibility = rawConnection.visibility ?? rawConnection.visibility_type ?? rawConnection.visibilityType
  const verified = rawConnection.verified ?? rawConnection.is_verified ?? rawConnection.isVerified
  const linkedAtInput = rawConnection.linked_at ?? rawConnection.linkedAt ?? rawConnection.created_at

  if (!type && !name && !id) {
    warnings.push(`Skipped malformed connection entry in ${sourcePath}`)
    return null
  }

  const linkedAt = parseTimestamp(linkedAtInput)
  if (linkedAtInput && !linkedAt.value) {
    warnings.push(`Connection in ${sourcePath} had invalid linkedAt timestamp: ${String(linkedAtInput)}`)
  }

  return {
    type: typeof type === 'string' && type.trim() ? type.trim().toLowerCase() : 'unknown',
    name: typeof name === 'string' && name.trim() ? name.trim() : id ? `Account ${id}` : 'Unknown Account',
    id: id === undefined || id === null || id === '' ? 'unknown' : String(id),
    visibility:
      typeof visibility === 'string' && visibility.trim()
        ? visibility.trim().toLowerCase()
        : Number.isFinite(visibility)
          ? String(visibility)
          : 'unknown',
    verified: typeof verified === 'boolean' ? verified : null,
    linkedAt: linkedAt.value ?? null,
  }
}

function extractConnectionsFromPayload(payload, sourcePath, warnings, sink = []) {
  if (!payload || typeof payload !== 'object') {
    return sink
  }

  if (Array.isArray(payload)) {
    payload.forEach((item) => extractConnectionsFromPayload(item, sourcePath, warnings, sink))
    return sink
  }

  const directConnectionCollections = [
    payload.connections,
    payload.connected_accounts,
    payload.connectedAccounts,
    payload.integrations,
  ]

  for (const collection of directConnectionCollections) {
    if (!Array.isArray(collection)) {
      continue
    }

    for (const entry of collection) {
      const normalized = normalizeConnection(entry, sourcePath, warnings)
      if (normalized) {
        sink.push(normalized)
      }
    }
  }

  for (const [key, value] of Object.entries(payload)) {
    if (key === 'connections' || key === 'connected_accounts' || key === 'connectedAccounts') {
      continue
    }

    if (value && typeof value === 'object') {
      extractConnectionsFromPayload(value, sourcePath, warnings, sink)
    }
  }

  return sink
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
    connections: parsedExport.connections ?? [],
    recentEmojis: parsedExport.recentEmojis ?? [],
    favoriteEmojis: parsedExport.favoriteEmojis ?? [],
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
  const connections = []

  let jsonFiles = []

  try {
    jsonFiles = await collectJsonFiles(rootPath)
  } catch (error) {
    return {
      channels: [],
      messagesByChannel: {},
      premiumHistory: [],
      badges: [],
      connections: [],
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
    connections.push(...extractConnectionsFromPayload(payload, jsonPath, warnings))

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
  const dedupedConnections = Array.from(
    new Map(
      connections.map((connection) => [
        `${connection.type}:${connection.id}:${connection.name}`,
        connection,
      ]),
    ).values(),
  ).sort((a, b) => a.type.localeCompare(b.type) || a.name.localeCompare(b.name))
  const { recentEmojis, favoriteEmojis } = buildEmojiDatasets(messagesByChannel)

  return {
    channels,
    messagesByChannel,
    premiumHistory,
    badges: dedupedBadges,
    connections: dedupedConnections,
    recentEmojis,
    favoriteEmojis,
    warnings,
    sortDirection,
  }
}

module.exports = {
  parseDiscordExport,
  getPaginatedParserOutput,
}
