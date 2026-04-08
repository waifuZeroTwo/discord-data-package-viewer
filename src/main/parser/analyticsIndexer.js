const OLDEST_MESSAGE_LIMIT = 1000
const TOP_METRIC_LIMIT = 25

function normalizeContentForCharacterCount(content) {
  if (typeof content !== 'string') {
    return ''
  }

  return content.normalize('NFC').replace(/\r\n?/g, '\n').replace(/\u00A0/g, ' ')
}

function inferAttachmentExtension(attachment) {
  const filename =
    typeof attachment?.filename === 'string' && attachment.filename.trim() ? attachment.filename.trim() : null
  if (filename) {
    const lastSegment = filename.split('.').pop()
    if (lastSegment && lastSegment !== filename) {
      return lastSegment.toLowerCase()
    }
  }

  const url = typeof attachment?.url === 'string' && attachment.url.trim() ? attachment.url.trim() : null
  if (!url) {
    return null
  }

  const withoutQuery = url.split('?')[0]
  const lastPathSegment = withoutQuery.split('/').pop()
  if (!lastPathSegment) {
    return null
  }

  const fromUrl = lastPathSegment.split('.').pop()
  if (!fromUrl || fromUrl === lastPathSegment) {
    return null
  }

  return fromUrl.toLowerCase()
}

function getIsoWeekKey(epochMs) {
  const date = new Date(epochMs)
  const utcDate = new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate()))
  const day = utcDate.getUTCDay() || 7
  utcDate.setUTCDate(utcDate.getUTCDate() + 4 - day)
  const yearStart = new Date(Date.UTC(utcDate.getUTCFullYear(), 0, 1))
  const weekNo = Math.ceil(((utcDate - yearStart) / 86400000 + 1) / 7)
  return `${utcDate.getUTCFullYear()}-W${String(weekNo).padStart(2, '0')}`
}

function toTopList(counterMap, limit = TOP_METRIC_LIMIT) {
  return Array.from(counterMap.entries())
    .map(([key, value]) => ({ key, ...value }))
    .sort(
      (a, b) =>
        b.count - a.count ||
        (b.characterCount || 0) - (a.characterCount || 0) ||
        String(a.label || a.key).localeCompare(String(b.label || b.key)),
    )
    .slice(0, limit)
}

function buildEmojiAssetUrl(emoji) {
  if (emoji.type !== 'custom' || !emoji.customId) {
    return null
  }

  const extension = emoji.animated ? 'gif' : 'png'
  return `https://cdn.discordapp.com/emojis/${emoji.customId}.${extension}?size=64&quality=lossless`
}

function bumpMetric(map, key, label, increment = 1, extra = {}, characterIncrement = 0) {
  if (!key) {
    return
  }

  const existing = map.get(key) || { label: label || key, count: 0, characterCount: 0, ...extra }
  existing.count += increment
  existing.characterCount += characterIncrement
  if (!existing.label && label) {
    existing.label = label
  }
  map.set(key, existing)
}

function formatUnknownLabel(entityName, entityId) {
  if (entityId) {
    return `Unknown ${entityName} (${entityId})`
  }

  return `Unknown ${entityName}`
}

function createAnalyticsIndexer() {
  const metrics = {
    messageCount: 0,
    characterCount: 0,
    mentionCount: 0,
    attachmentCount: 0,
    attachmentBytes: 0,
    attachmentSizeUnavailableCount: 0,
    attachmentTypeUnavailableCount: 0,
    mentionsFromArrayCount: 0,
    mentionsFromTokenCount: 0,
  }

  const counters = {
    channels: new Map(),
    dmUsers: new Map(),
    guilds: new Map(),
    groupDMs: new Map(),
    customEmojis: new Map(),
    allEmojis: new Map(),
    activeHours: new Map(),
    activeWeeks: new Map(),
    activeMonths: new Map(),
    activeYears: new Map(),
    attachmentTypes: new Map(),
  }

  const oldestMessages = []

  function pushOldestMessageCandidate(message) {
    if (!Number.isFinite(message.timestampEpochMs)) {
      return
    }

    const candidate = {
      id: message.id,
      channelId: message.channelId,
      channelName: message.channelName || `Channel ${message.channelId}`,
      author: message.author,
      timestamp: message.timestamp,
      timestampEpochMs: message.timestampEpochMs,
      content: message.content,
      characterCount: message.content.length,
      attachmentCount: Array.isArray(message.attachments) ? message.attachments.length : 0,
    }

    oldestMessages.push(candidate)
    oldestMessages.sort((a, b) => b.timestampEpochMs - a.timestampEpochMs)
    if (oldestMessages.length > OLDEST_MESSAGE_LIMIT) {
      oldestMessages.shift()
    }
  }

  return {
    ingestMessage(message) {
      metrics.messageCount += 1

      const content = normalizeContentForCharacterCount(message.content)
      const characterCount = content.length
      metrics.characterCount += characterCount

      const arrayMentions = Array.isArray(message.mentions) ? message.mentions.length : 0
      const tokenMentions = content.match(/<@!?\d+>|<@&\d+>|<#\d+>|@everyone|@here/g)?.length || 0
      const mentionCount = Math.max(arrayMentions, tokenMentions)

      metrics.mentionCount += mentionCount
      metrics.mentionsFromArrayCount += arrayMentions
      metrics.mentionsFromTokenCount += tokenMentions

      if (Array.isArray(message.attachments)) {
        metrics.attachmentCount += message.attachments.length
        for (const attachment of message.attachments) {
          const size = Number(attachment?.size)
          if (Number.isFinite(size)) {
            metrics.attachmentBytes += size
          } else {
            metrics.attachmentSizeUnavailableCount += 1
          }

          const extension = inferAttachmentExtension(attachment)
          if (extension) {
            bumpMetric(counters.attachmentTypes, extension, extension)
          } else {
            metrics.attachmentTypeUnavailableCount += 1
          }
        }
      }

      bumpMetric(
        counters.channels,
        message.channelId,
        message.channelName || formatUnknownLabel('Channel', message.channelId),
        1,
        {},
        characterCount,
      )

      if (message.dmUserId || message.dmUserName) {
        bumpMetric(
          counters.dmUsers,
          String(message.dmUserId || message.dmUserName),
          message.dmUserName || formatUnknownLabel('User', message.dmUserId),
          1,
          {},
          characterCount,
        )
      }

      if (message.guildId || message.guildName) {
        bumpMetric(
          counters.guilds,
          String(message.guildId || message.guildName),
          message.guildName || formatUnknownLabel('Guild', message.guildId),
          1,
          {},
          characterCount,
        )
      }

      if (message.groupDmId || message.groupDmName) {
        bumpMetric(
          counters.groupDMs,
          String(message.groupDmId || message.groupDmName),
          message.groupDmName || formatUnknownLabel('Group DM', message.groupDmId),
          1,
          {},
          characterCount,
        )
      }

      if (Number.isFinite(message.timestampEpochMs)) {
        const date = new Date(message.timestampEpochMs)
        const hourKey = String(date.getUTCHours()).padStart(2, '0')
        const monthKey = `${date.getUTCFullYear()}-${String(date.getUTCMonth() + 1).padStart(2, '0')}`
        const yearKey = String(date.getUTCFullYear())

        bumpMetric(counters.activeHours, hourKey, `${hourKey}:00 UTC`)
        bumpMetric(counters.activeWeeks, getIsoWeekKey(message.timestampEpochMs), getIsoWeekKey(message.timestampEpochMs))
        bumpMetric(counters.activeMonths, monthKey, monthKey)
        bumpMetric(counters.activeYears, yearKey, yearKey)
      }

      if (Array.isArray(message.emojiUsage)) {
        for (const emoji of message.emojiUsage) {
          const count = Number.isFinite(emoji.count) ? emoji.count : 1
          const key =
            emoji.type === 'custom' && emoji.customId
              ? `custom:${emoji.customId}`
              : `unicode:${emoji.unicode || emoji.raw || 'unknown'}`

          const map = emoji.type === 'custom' && emoji.customId ? counters.customEmojis : counters.allEmojis
          const allMap = counters.allEmojis

          const label = emoji.raw || emoji.unicode || emoji.name || 'emoji'
          bumpMetric(map, key, label, count, {
            type: emoji.type,
            name: emoji.name || emoji.unicode || 'emoji',
            unicode: emoji.unicode || null,
            customId: emoji.customId || null,
            animated: Boolean(emoji.animated),
          })
          if (map !== allMap) {
            bumpMetric(allMap, key, label, count, {
              type: emoji.type,
              name: emoji.name || emoji.unicode || 'emoji',
              unicode: emoji.unicode || null,
              customId: emoji.customId || null,
              animated: Boolean(emoji.animated),
            })
          }
        }
      }

      pushOldestMessageCandidate(message)
    },

    getDashboardSummary() {
      return {
        topChannels: toTopList(counters.channels),
        topDmedUsers: toTopList(counters.dmUsers),
        topGuilds: toTopList(counters.guilds),
        topGroupDms: toTopList(counters.groupDMs),
        characterCount: {
          key: 'characterCount',
          count: metrics.characterCount,
        },
        topCustomEmojis: toTopList(counters.customEmojis).map((item) => ({
          ...item,
          assetUrl: buildEmojiAssetUrl(item),
        })),
        topEmojis: toTopList(counters.allEmojis).map((item) => ({
          ...item,
          assetUrl: buildEmojiAssetUrl(item),
        })),
        activeHours: toTopList(counters.activeHours, 24),
        activeWeeks: toTopList(counters.activeWeeks, 52),
        activeMonths: toTopList(counters.activeMonths, 120),
        activeYears: toTopList(counters.activeYears, 25),
        oldestMessages: [...oldestMessages].sort((a, b) => a.timestampEpochMs - b.timestampEpochMs),
        attachments: {
          key: 'attachments',
          count: metrics.attachmentCount,
          totalBytes: metrics.attachmentBytes,
          topFileTypes: toTopList(counters.attachmentTypes, 10),
          sizeUnavailableCount: metrics.attachmentSizeUnavailableCount,
          typeUnavailableCount: metrics.attachmentTypeUnavailableCount,
        },
        mentions: {
          key: 'mentions',
          count: metrics.mentionCount,
          fromMentionArrays: metrics.mentionsFromArrayCount,
          fromMentionTokens: metrics.mentionsFromTokenCount,
        },
        dataAvailabilityNotes: [
          metrics.attachmentSizeUnavailableCount > 0
            ? `${metrics.attachmentSizeUnavailableCount} attachments were missing a size field.`
            : null,
          metrics.attachmentTypeUnavailableCount > 0
            ? `${metrics.attachmentTypeUnavailableCount} attachments were missing a filename/extension.`
            : null,
          metrics.mentionsFromArrayCount === 0 && metrics.mentionsFromTokenCount > 0
            ? 'Mention arrays were unavailable in message records, so mention tokens from message content were used.'
            : null,
          metrics.mentionsFromArrayCount > 0 && metrics.mentionsFromTokenCount > 0
            ? 'Mentions were derived from both mention arrays and content tokens (deduplicated per message via max count).'
            : null,
        ].filter(Boolean),
        characterNormalization: {
          strategy: 'NFC unicode + CRLF→LF + NBSP→space',
        },
      }
    },

    getMessageCount() {
      return metrics.messageCount
    },

    getEmojiDatasets() {
      const favoriteEmojis = toTopList(counters.allEmojis, 250).map((item) => ({
        key: item.key,
        type: item.type,
        name: item.name,
        unicode: item.unicode,
        customId: item.customId,
        animated: item.animated,
        raw: item.label,
        totalUses: item.count,
        lastUsedTimestamp: null,
        channelCount: null,
        assetUrl: buildEmojiAssetUrl(item),
      }))

      return {
        favoriteEmojis,
        recentEmojis: favoriteEmojis,
      }
    },
  }
}

module.exports = {
  createAnalyticsIndexer,
}
