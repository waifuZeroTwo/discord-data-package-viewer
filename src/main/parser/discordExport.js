const path = require('path')
const fs = require('fs/promises')
const {
  extractPremiumEventsFromPayload,
  normalizePremiumEventsToIntervals,
} = require('./premiumHistory')
const { createAnalyticsIndexer } = require('./analyticsIndexer')
const { mapLegacyBadge, mapPremiumType, mapPublicFlags } = require('../../shared/badges')

const DEFAULT_SORT_DIRECTION = 'asc'
const PANEL_KEYS = ['premium', 'badges', 'emojis', 'connections', 'billing']

function normalizeSortDirection(sortDirection) {
  return sortDirection === 'desc' ? 'desc' : DEFAULT_SORT_DIRECTION
}

function createPanelMetadata() {
  const metadata = {}

  for (const key of PANEL_KEYS) {
    metadata[key] = {
      sourcePaths: new Set(),
      warnings: [],
      recordCount: 0,
    }
  }

  return metadata
}

function createWarningCollector(globalWarnings, panelMetadata, panelKey) {
  return {
    push(message) {
      globalWarnings.push(message)
      if (panelMetadata[panelKey]) {
        panelMetadata[panelKey].warnings.push(message)
      }
    },
  }
}

function pushPanelWarning(globalWarnings, panelMetadata, panelKey, message) {
  globalWarnings.push(message)
  if (panelMetadata[panelKey]) {
    panelMetadata[panelKey].warnings.push(message)
  }
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

  const channelTypeRaw =
    rawMessage.channel?.type ?? rawMessage.channel_type ?? rawMessage.channelType ?? rawMessage.type ?? null
  const channelType = typeof channelTypeRaw === 'string' ? channelTypeRaw.toLowerCase() : String(channelTypeRaw ?? '')
  const isDirectMessage = channelType.includes('dm') && !channelType.includes('group')
  const isGroupDirectMessage = channelType.includes('group')

  return {
    id: rawMessage.id ? String(rawMessage.id) : null,
    channelId: String(channelId),
    channelName:
      rawMessage.channel?.name ??
      rawMessage.channel_name ??
      rawMessage.channelName ??
      rawMessage.conversation?.name ??
      null,
    guildId: rawMessage.guild?.id ?? rawMessage.guild_id ?? rawMessage.guildId ?? null,
    guildName: rawMessage.guild?.name ?? rawMessage.guild_name ?? rawMessage.guildName ?? null,
    dmUserId: isDirectMessage
      ? rawMessage.recipient?.id ??
        rawMessage.dmUser?.id ??
        rawMessage.recipient_id ??
        rawMessage.dm_user_id ??
        null
      : null,
    dmUserName: isDirectMessage
      ? rawMessage.recipient?.username ??
        rawMessage.recipient?.name ??
        rawMessage.dmUser?.username ??
        rawMessage.dm_user_name ??
        null
      : null,
    groupDmId: isGroupDirectMessage
      ? rawMessage.groupDm?.id ?? rawMessage.group_dm_id ?? rawMessage.groupDMId ?? rawMessage.channel?.id ?? null
      : null,
    groupDmName: isGroupDirectMessage
      ? rawMessage.groupDm?.name ??
        rawMessage.group_dm_name ??
        rawMessage.groupDMName ??
        rawMessage.channel?.name ??
        null
      : null,
    mentions: Array.isArray(rawMessage.mentions) ? rawMessage.mentions : [],
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

function extractBadgesFromPayload(payload, jsonPath, warnings) {
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
        } else if (rawValue !== undefined && rawValue !== null && rawValue !== '') {
          warnings.push(`Skipped unrecognized badge value "${String(rawValue)}" in ${jsonPath}`)
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

function normalizeCurrency(rawCurrency) {
  if (typeof rawCurrency !== 'string' || rawCurrency.trim() === '') {
    return null
  }

  return rawCurrency.trim().toUpperCase()
}

function parseAmountToMajorUnit(rawAmount) {
  if (rawAmount === undefined || rawAmount === null || rawAmount === '') {
    return null
  }

  const numericAmount = Number(rawAmount)
  if (!Number.isFinite(numericAmount)) {
    return null
  }

  if (Number.isInteger(numericAmount) && Math.abs(numericAmount) >= 100) {
    return numericAmount / 100
  }

  return numericAmount
}

function normalizeTransactionType(rawType, fallbackDescription = '') {
  const normalized = typeof rawType === 'string' ? rawType.trim().toLowerCase() : ''
  if (
    normalized.includes('refund') ||
    fallbackDescription.includes('refund') ||
    fallbackDescription.includes('chargeback')
  ) {
    return 'refund'
  }

  if (normalized.includes('gift') || fallbackDescription.includes('gift')) {
    return 'gift'
  }

  if (normalized.includes('invoice')) {
    return 'invoice'
  }

  if (normalized.includes('subscription') || normalized.includes('renewal') || normalized.includes('nitro')) {
    return 'subscription'
  }

  if (normalized.includes('charge') || normalized.includes('payment')) {
    return 'charge'
  }

  return normalized || 'unknown'
}

function normalizeBillingTransaction(rawRecord, sourcePath, warnings) {
  if (!rawRecord || typeof rawRecord !== 'object') {
    return null
  }

  const dateInput =
    rawRecord.date ??
    rawRecord.created_at ??
    rawRecord.createdAt ??
    rawRecord.timestamp ??
    rawRecord.purchased_at ??
    rawRecord.purchase_date
  const parsedDate = parseTimestamp(dateInput)
  if (dateInput && !parsedDate.value) {
    warnings.push(`Billing record in ${sourcePath} had invalid timestamp: ${String(dateInput)}`)
  }

  const amount =
    parseAmountToMajorUnit(rawRecord.amount) ??
    parseAmountToMajorUnit(rawRecord.amount_total) ??
    parseAmountToMajorUnit(rawRecord.total_amount) ??
    parseAmountToMajorUnit(rawRecord.subtotal) ??
    parseAmountToMajorUnit(rawRecord.price)
  const currency = normalizeCurrency(rawRecord.currency ?? rawRecord.currency_code ?? rawRecord.currencyCode)

  const descriptionParts = [
    rawRecord.description,
    rawRecord.memo,
    rawRecord.name,
    rawRecord.plan_name,
    rawRecord.planName,
    rawRecord.title,
  ].filter((item) => typeof item === 'string' && item.trim())
  const description = descriptionParts.length > 0 ? descriptionParts.join(' • ') : 'Billing transaction'

  const statusRaw = rawRecord.status ?? rawRecord.state ?? rawRecord.payment_status ?? rawRecord.paymentStatus
  const status = typeof statusRaw === 'string' && statusRaw.trim() ? statusRaw.trim().toLowerCase() : 'unknown'

  const relatedSubscriptionId =
    rawRecord.subscription_id ??
    rawRecord.subscriptionId ??
    rawRecord.plan_id ??
    rawRecord.planId ??
    rawRecord.renewal_id

  const type = normalizeTransactionType(
    rawRecord.type ?? rawRecord.transaction_type ?? rawRecord.transactionType ?? rawRecord.kind,
    description.toLowerCase(),
  )

  if (!parsedDate.value && amount === null && type === 'unknown') {
    return null
  }

  return {
    date: parsedDate.value,
    amount,
    currency,
    type,
    status,
    description,
    relatedSubscriptionId:
      relatedSubscriptionId === undefined || relatedSubscriptionId === null || relatedSubscriptionId === ''
        ? null
        : String(relatedSubscriptionId),
    source: sourcePath,
  }
}

function normalizeGiftRedemption(rawGift, sourcePath, warnings) {
  if (!rawGift || typeof rawGift !== 'object') {
    return null
  }

  const dateInput =
    rawGift.redeemed_at ??
    rawGift.redeemedAt ??
    rawGift.claimed_at ??
    rawGift.claimedAt ??
    rawGift.created_at
  const parsedDate = parseTimestamp(dateInput)
  if (dateInput && !parsedDate.value) {
    warnings.push(`Gift redemption in ${sourcePath} had invalid timestamp: ${String(dateInput)}`)
  }

  const subscriptionId =
    rawGift.subscription_id ??
    rawGift.subscriptionId ??
    rawGift.premium_subscription_id ??
    rawGift.premiumSubscriptionId
  const amount = parseAmountToMajorUnit(rawGift.amount ?? rawGift.value ?? rawGift.price)
  const currency = normalizeCurrency(rawGift.currency ?? rawGift.currency_code ?? rawGift.currencyCode)
  const code = rawGift.code ?? rawGift.gift_code ?? rawGift.giftCode
  const description = rawGift.description ?? rawGift.plan_name ?? rawGift.planName ?? 'Gifted Nitro redemption'

  if (!parsedDate.value && !subscriptionId && !code) {
    return null
  }

  return {
    date: parsedDate.value,
    amount,
    currency,
    type: 'gift',
    status: 'redeemed',
    description: String(description),
    relatedSubscriptionId:
      subscriptionId === undefined || subscriptionId === null || subscriptionId === '' ? null : String(subscriptionId),
    source: sourcePath,
    giftCode: code === undefined || code === null || code === '' ? null : String(code),
  }
}

function extractBillingDataFromPayload(payload, sourcePath, warnings, sink = { transactions: [], giftedNitro: [] }) {
  if (!payload || typeof payload !== 'object') {
    return sink
  }

  if (Array.isArray(payload)) {
    for (const item of payload) {
      extractBillingDataFromPayload(item, sourcePath, warnings, sink)
    }
    return sink
  }

  const transactionCollections = [
    payload.invoices,
    payload.invoice_history,
    payload.payments,
    payload.payment_history,
    payload.charges,
    payload.transactions,
    payload.refunds,
    payload.subscriptions,
    payload.subscription_history,
  ]

  for (const collection of transactionCollections) {
    if (!Array.isArray(collection)) {
      continue
    }

    for (const entry of collection) {
      const normalized = normalizeBillingTransaction(entry, sourcePath, warnings)
      if (normalized) {
        sink.transactions.push(normalized)
      }
    }
  }

  const giftCollections = [
    payload.gift_redemptions,
    payload.giftRedemptions,
    payload.gifts_received,
    payload.giftsReceived,
    payload.redeemed_gifts,
  ]

  for (const collection of giftCollections) {
    if (!Array.isArray(collection)) {
      continue
    }

    for (const entry of collection) {
      const normalizedGift = normalizeGiftRedemption(entry, sourcePath, warnings)
      if (normalizedGift) {
        sink.giftedNitro.push(normalizedGift)
        sink.transactions.push({
          date: normalizedGift.date,
          amount: normalizedGift.amount,
          currency: normalizedGift.currency,
          type: normalizedGift.type,
          status: normalizedGift.status,
          description: normalizedGift.description,
          relatedSubscriptionId: normalizedGift.relatedSubscriptionId,
          source: normalizedGift.source,
        })
      }
    }
  }

  for (const [key, value] of Object.entries(payload)) {
    if (
      key === 'invoices' ||
      key === 'payments' ||
      key === 'charges' ||
      key === 'transactions' ||
      key === 'refunds' ||
      key === 'subscriptions' ||
      key === 'gift_redemptions' ||
      key === 'giftRedemptions'
    ) {
      continue
    }

    if (value && typeof value === 'object') {
      extractBillingDataFromPayload(value, sourcePath, warnings, sink)
    }
  }

  return sink
}

function computeBillingSummary(transactions, giftedNitro) {
  const metrics = {
    transactionCount: transactions.length,
    totalSpentGross: 0,
    totalSpentNet: 0,
    refundTotal: 0,
    giftedNitroCount: giftedNitro.length,
    giftedNitroValueReceived: 0,
  }

  for (const transaction of transactions) {
    const amount = Number(transaction.amount)
    if (!Number.isFinite(amount)) {
      continue
    }

    metrics.totalSpentGross += amount

    if (transaction.type !== 'refund') {
      metrics.totalSpentNet += amount
    } else {
      metrics.refundTotal += Math.abs(amount)
    }
  }

  for (const gift of giftedNitro) {
    const amount = Number(gift.amount)
    if (Number.isFinite(amount)) {
      metrics.giftedNitroValueReceived += amount
    }
  }

  const round = (value) => Math.round(value * 100) / 100
  const knownCurrencies = Array.from(
    new Set(transactions.map((item) => item.currency).filter((value) => typeof value === 'string' && value.trim())),
  )

  return {
    transactionCount: metrics.transactionCount,
    totalSpentGross: round(metrics.totalSpentGross),
    totalSpentNet: round(metrics.totalSpentNet),
    refundTotal: round(metrics.refundTotal),
    giftedNitroCount: metrics.giftedNitroCount,
    giftedNitroValueReceived: round(metrics.giftedNitroValueReceived),
    summaryCurrency: knownCurrencies.length === 1 ? knownCurrencies[0] : null,
  }
}

function getPaginatedParserOutput(parsedExport, options = {}) {
  return {
    premiumHistory: parsedExport.premiumHistory ?? [],
    badges: parsedExport.badges ?? [],
    connections: parsedExport.connections ?? [],
    recentEmojis: parsedExport.recentEmojis ?? [],
    favoriteEmojis: parsedExport.favoriteEmojis ?? [],
    billingTransactions: parsedExport.billingTransactions ?? [],
    giftedNitro: parsedExport.giftedNitro ?? [],
    billingSummary: parsedExport.billingSummary ?? {
      transactionCount: 0,
      totalSpentGross: 0,
      totalSpentNet: 0,
      refundTotal: 0,
      giftedNitroCount: 0,
      giftedNitroValueReceived: 0,
      summaryCurrency: null,
    },
    parserMetadata: parsedExport.parserMetadata ?? {},
    analyticsSummary: parsedExport.analyticsSummary ?? {
      channelCount: 0,
      messageCount: 0,
    },
    dashboardMetrics: parsedExport.dashboardMetrics ?? {},
    warnings: parsedExport.warnings,
  }
}

async function parseDiscordExport(rootPath, options = {}) {
  const warnings = []
  const panelMetadata = createPanelMetadata()
  const channelsById = new Map()
  const analyticsIndexer = createAnalyticsIndexer()
  const sortDirection = normalizeSortDirection(options.sortDirection)
  const premiumEvents = []
  const badges = []
  const connections = []
  const billingTransactions = []
  const giftedNitro = []

  let jsonFiles = []

  try {
    jsonFiles = await collectJsonFiles(rootPath)
  } catch (error) {
    return {
      premiumHistory: [],
      badges: [],
      connections: [],
      billingTransactions: [],
      giftedNitro: [],
      billingSummary: computeBillingSummary([], []),
      parserMetadata: {},
      analyticsSummary: {
        channelCount: 0,
        messageCount: 0,
      },
      dashboardMetrics: {},
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
    }

    const premiumWarningCollector = createWarningCollector(warnings, panelMetadata, 'premium')
    const connectionsWarningCollector = createWarningCollector(warnings, panelMetadata, 'connections')
    const billingWarningCollector = createWarningCollector(warnings, panelMetadata, 'billing')
    const badgeWarningCollector = createWarningCollector(warnings, panelMetadata, 'badges')

    const extractedPremiumEvents = extractPremiumEventsFromPayload(
      payload,
      jsonPath,
      premiumWarningCollector,
    )
    if (extractedPremiumEvents.length > 0) {
      panelMetadata.premium.sourcePaths.add(jsonPath)
      premiumEvents.push(...extractedPremiumEvents)
    }

    const extractedBadges = extractBadgesFromPayload(payload, jsonPath, badgeWarningCollector)
    if (extractedBadges.length > 0) {
      panelMetadata.badges.sourcePaths.add(jsonPath)
      badges.push(...extractedBadges)
    }

    const extractedConnections = extractConnectionsFromPayload(
      payload,
      jsonPath,
      connectionsWarningCollector,
    )
    if (extractedConnections.length > 0) {
      panelMetadata.connections.sourcePaths.add(jsonPath)
      connections.push(...extractedConnections)
    }

    const extractedBilling = extractBillingDataFromPayload(
      payload,
      jsonPath,
      billingWarningCollector,
    )
    if (extractedBilling.transactions.length > 0 || extractedBilling.giftedNitro.length > 0) {
      panelMetadata.billing.sourcePaths.add(jsonPath)
    }
    billingTransactions.push(...extractedBilling.transactions)
    giftedNitro.push(...extractedBilling.giftedNitro)

    const messages = maybeExtractMessages(payload, jsonPath, warnings)
    for (const message of messages) {
      if (!channelsById.has(message.channelId)) {
        channelsById.set(message.channelId, {
          id: message.channelId,
          name: `Channel ${message.channelId}`,
          type: 'unknown',
        })
      }

      analyticsIndexer.ingestMessage(message)
      if (Array.isArray(message.emojiUsage) && message.emojiUsage.length > 0) {
        panelMetadata.emojis.sourcePaths.add(jsonPath)
      }
    }
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
  const { recentEmojis, favoriteEmojis } = analyticsIndexer.getEmojiDatasets()
  const dashboardMetrics = analyticsIndexer.getDashboardSummary()
  const dedupedBillingTransactions = Array.from(
    new Map(
      billingTransactions.map((entry) => [
        [
          entry.date || '',
          entry.amount ?? '',
          entry.currency || '',
          entry.type || '',
          entry.status || '',
          entry.description || '',
          entry.relatedSubscriptionId || '',
          entry.source || '',
        ].join('|'),
        entry,
      ]),
    ).values(),
  ).sort((a, b) => {
    const aEpoch = a.date ? Date.parse(a.date) : Number.NEGATIVE_INFINITY
    const bEpoch = b.date ? Date.parse(b.date) : Number.NEGATIVE_INFINITY
    return bEpoch - aEpoch
  })
  const dedupedGiftedNitro = Array.from(
    new Map(
      giftedNitro.map((gift) => [
        [gift.date || '', gift.giftCode || '', gift.relatedSubscriptionId || '', gift.source || ''].join('|'),
        gift,
      ]),
    ).values(),
  ).sort((a, b) => {
    const aEpoch = a.date ? Date.parse(a.date) : Number.NEGATIVE_INFINITY
    const bEpoch = b.date ? Date.parse(b.date) : Number.NEGATIVE_INFINITY
    return bEpoch - aEpoch
  })
  const billingSummary = computeBillingSummary(dedupedBillingTransactions, dedupedGiftedNitro)

  panelMetadata.premium.recordCount = premiumHistory.length
  panelMetadata.badges.recordCount = dedupedBadges.length
  panelMetadata.emojis.recordCount = recentEmojis.length
  panelMetadata.connections.recordCount = dedupedConnections.length
  panelMetadata.billing.recordCount = dedupedBillingTransactions.length

  for (const interval of premiumHistory) {
    if (interval.source) {
      const [sourcePath] = String(interval.source).split(':')
      if (sourcePath) {
        panelMetadata.premium.sourcePaths.add(sourcePath)
      }
    }
  }

  if (premiumEvents.length > 0 && premiumHistory.length === 0) {
    pushPanelWarning(
      warnings,
      panelMetadata,
      'premium',
      'Premium entries were detected but none could be normalized into displayable intervals.',
    )
  }

  const parserMetadata = {}
  for (const panelKey of PANEL_KEYS) {
    parserMetadata[panelKey] = {
      sourcePaths: Array.from(panelMetadata[panelKey].sourcePaths).sort(),
      recordCount: panelMetadata[panelKey].recordCount,
      warnings: panelMetadata[panelKey].warnings,
    }
  }

  const messageCount = analyticsIndexer.getMessageCount()

  return {
    premiumHistory,
    badges: dedupedBadges,
    connections: dedupedConnections,
    recentEmojis,
    favoriteEmojis,
    billingTransactions: dedupedBillingTransactions,
    giftedNitro: dedupedGiftedNitro,
    billingSummary,
    parserMetadata,
    analyticsSummary: {
      channelCount: channels.length,
      messageCount,
    },
    dashboardMetrics,
    warnings,
    sortDirection,
  }
}

module.exports = {
  parseDiscordExport,
  getPaginatedParserOutput,
}
