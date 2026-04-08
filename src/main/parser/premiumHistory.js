const PREMIUM_KEYWORD_REGEX = /(premium|nitro|subscription|gift)/i
const PLAN_TYPE_REGEX = /(nitro\s*basic|nitro\s*classic|nitro)/i

function parseTimestamp(rawValue) {
  if (rawValue === undefined || rawValue === null || rawValue === '') {
    return null
  }

  const isoString = rawValue instanceof Date ? rawValue.toISOString() : String(rawValue)
  const epochMs = Date.parse(isoString)

  if (Number.isNaN(epochMs)) {
    return null
  }

  return {
    value: new Date(epochMs).toISOString(),
    epochMs,
  }
}

function normalizePlanType(rawValue) {
  if (!rawValue) {
    return 'unknown'
  }

  const text = String(rawValue).trim().toLowerCase()
  if (!text) {
    return 'unknown'
  }

  if (text.includes('basic')) {
    return 'nitro_basic'
  }

  if (text.includes('classic')) {
    return 'nitro_classic'
  }

  if (text.includes('nitro')) {
    return 'nitro'
  }

  return text.replace(/\s+/g, '_')
}

function inferPlanType(candidate) {
  if (!candidate || typeof candidate !== 'object') {
    return 'unknown'
  }

  const planKeys = [
    'planType',
    'plan_type',
    'subscriptionPlan',
    'subscription_plan',
    'skuName',
    'sku_name',
    'premiumType',
    'premium_type',
  ]

  for (const key of planKeys) {
    if (candidate[key]) {
      return normalizePlanType(candidate[key])
    }
  }

  const blob = JSON.stringify(candidate)
  const match = blob.match(PLAN_TYPE_REGEX)
  if (match) {
    return normalizePlanType(match[1])
  }

  return 'unknown'
}

function inferGifted(candidate) {
  if (!candidate || typeof candidate !== 'object') {
    return false
  }

  const giftKeys = [
    'isGifted',
    'is_gifted',
    'gifted',
    'isGift',
    'is_gift',
    'viaGift',
    'via_gift',
    'giftCode',
    'gift_code',
  ]

  for (const key of giftKeys) {
    if (candidate[key] !== undefined && candidate[key] !== null && candidate[key] !== '') {
      return Boolean(candidate[key])
    }
  }

  return /gift/i.test(JSON.stringify(candidate))
}

function inferEventType(candidate) {
  const directType =
    candidate?.eventType ??
    candidate?.event_type ??
    candidate?.type ??
    candidate?.action ??
    candidate?.status ??
    candidate?.name ??
    ''

  const haystack = `${String(directType)} ${JSON.stringify(candidate || {})}`.toLowerCase()

  if (/(gift|redeem|claim|activation)/.test(haystack)) {
    return 'gift_activation'
  }

  if (/(cancel|cancellation)/.test(haystack)) {
    return 'cancellation'
  }

  if (/(renew|recurr|invoice_paid|payment_succeeded)/.test(haystack)) {
    return 'renewal'
  }

  if (/(expire|ended|end|termination)/.test(haystack)) {
    return 'end'
  }

  if (/(start|begin|subscribe|purchase|activation|activated)/.test(haystack)) {
    return 'start'
  }

  return 'unknown'
}

function getEventTimestamp(candidate) {
  if (!candidate || typeof candidate !== 'object') {
    return null
  }

  const timestampKeys = [
    'timestamp',
    'eventAt',
    'event_at',
    'date',
    'createdAt',
    'created_at',
    'updatedAt',
    'updated_at',
    'startedAt',
    'started_at',
    'startAt',
    'start_at',
    'startsAt',
    'starts_at',
    'currentPeriodStart',
    'current_period_start',
    'renewalAt',
    'renewal_at',
    'canceledAt',
    'canceled_at',
    'endedAt',
    'ended_at',
    'endAt',
    'end_at',
    'endsAt',
    'ends_at',
    'currentPeriodEnd',
    'current_period_end',
  ]

  for (const key of timestampKeys) {
    const parsed = parseTimestamp(candidate[key])
    if (parsed) {
      return parsed
    }
  }

  return null
}

function collectPremiumCandidates(payload, parentPath = '$', sink = []) {
  if (!payload || typeof payload !== 'object') {
    return sink
  }

  if (Array.isArray(payload)) {
    payload.forEach((item, index) => collectPremiumCandidates(item, `${parentPath}[${index}]`, sink))
    return sink
  }

  const keyBlob = Object.keys(payload).join(' ').toLowerCase()
  const valueBlob = JSON.stringify(payload).toLowerCase()
  const mayBePremium = PREMIUM_KEYWORD_REGEX.test(keyBlob) || PREMIUM_KEYWORD_REGEX.test(valueBlob)

  if (mayBePremium) {
    sink.push({ candidate: payload, sourcePath: parentPath })
  }

  for (const [key, value] of Object.entries(payload)) {
    if (value && typeof value === 'object') {
      collectPremiumCandidates(value, `${parentPath}.${key}`, sink)
    }
  }

  return sink
}

function extractPremiumEventsFromPayload(payload, jsonPath, warnings) {
  const candidates = collectPremiumCandidates(payload)
  const events = []

  for (const item of candidates) {
    const timestamp = getEventTimestamp(item.candidate)
    const eventType = inferEventType(item.candidate)

    const explicitStart = parseTimestamp(
      item.candidate.startAt ??
        item.candidate.start_at ??
        item.candidate.startedAt ??
        item.candidate.started_at ??
        item.candidate.startsAt ??
        item.candidate.starts_at ??
        item.candidate.currentPeriodStart ??
        item.candidate.current_period_start,
    )

    const explicitEnd = parseTimestamp(
      item.candidate.endAt ??
        item.candidate.end_at ??
        item.candidate.endedAt ??
        item.candidate.ended_at ??
        item.candidate.endsAt ??
        item.candidate.ends_at ??
        item.candidate.currentPeriodEnd ??
        item.candidate.current_period_end,
    )

    if (explicitStart && explicitEnd) {
      events.push({
        kind: 'direct_interval',
        timestampEpochMs: explicitStart.epochMs,
        timestamp: explicitStart.value,
        endEpochMs: explicitEnd.epochMs,
        endTimestamp: explicitEnd.value,
        source: `${jsonPath}:${item.sourcePath}`,
        isGifted: inferGifted(item.candidate),
        planType: inferPlanType(item.candidate),
      })
      continue
    }

    if (!timestamp) {
      continue
    }

    events.push({
      kind: eventType,
      timestampEpochMs: timestamp.epochMs,
      timestamp: timestamp.value,
      source: `${jsonPath}:${item.sourcePath}`,
      isGifted: inferGifted(item.candidate),
      planType: inferPlanType(item.candidate),
    })
  }

  if (candidates.length > 0 && events.length === 0) {
    warnings.push(`Detected premium-related data in ${jsonPath} but no parseable premium timestamps were found.`)
  }

  return events
}

function mergeAdjacentIntervals(intervals) {
  if (intervals.length <= 1) {
    return intervals
  }

  const merged = [intervals[0]]

  for (let index = 1; index < intervals.length; index += 1) {
    const current = intervals[index]
    const previous = merged[merged.length - 1]

    if (previous.endAt && current.startAt && previous.endAt === current.startAt) {
      previous.endAt = current.endAt
      previous.source = Array.from(new Set([previous.source, current.source])).join(' | ')
      previous.isGifted = previous.isGifted && current.isGifted
      if (previous.planType === 'unknown' && current.planType !== 'unknown') {
        previous.planType = current.planType
      }
      continue
    }

    merged.push(current)
  }

  return merged
}

function normalizePremiumEventsToIntervals(rawEvents, warnings) {
  if (!Array.isArray(rawEvents) || rawEvents.length === 0) {
    return []
  }

  const orderedEvents = [...rawEvents].sort((a, b) => a.timestampEpochMs - b.timestampEpochMs)
  const intervals = []
  let activeInterval = null

  const closeActiveInterval = (endEpochMs, endTimestamp) => {
    if (!activeInterval) {
      return
    }

    if (endEpochMs !== null && endEpochMs !== undefined && endEpochMs >= activeInterval.startEpochMs) {
      activeInterval.endAt = endTimestamp
      activeInterval.endEpochMs = endEpochMs
    }

    intervals.push({
      startAt: activeInterval.startAt,
      endAt: activeInterval.endAt,
      source: activeInterval.source,
      isGifted: activeInterval.isGifted,
      planType: activeInterval.planType,
      startEpochMs: activeInterval.startEpochMs,
      endEpochMs: activeInterval.endEpochMs,
    })

    activeInterval = null
  }

  for (const event of orderedEvents) {
    if (!Number.isFinite(event.timestampEpochMs)) {
      continue
    }

    if (event.kind === 'direct_interval') {
      if (activeInterval) {
        closeActiveInterval(event.timestampEpochMs, event.timestamp)
      }

      intervals.push({
        startAt: event.timestamp,
        endAt: event.endTimestamp,
        source: event.source,
        isGifted: event.isGifted,
        planType: event.planType,
        startEpochMs: event.timestampEpochMs,
        endEpochMs: event.endEpochMs,
      })
      continue
    }

    if (event.kind === 'end' || event.kind === 'cancellation') {
      if (activeInterval) {
        closeActiveInterval(event.timestampEpochMs, event.timestamp)
      } else {
        warnings.push(
          `Premium ${event.kind} event in ${event.source} had no matching start; ignored for interval output.`,
        )
      }
      continue
    }

    if (event.kind === 'start' || event.kind === 'renewal' || event.kind === 'gift_activation') {
      if (!activeInterval) {
        activeInterval = {
          startAt: event.timestamp,
          endAt: null,
          source: event.source,
          isGifted: event.isGifted,
          planType: event.planType,
          startEpochMs: event.timestampEpochMs,
          endEpochMs: null,
        }
        continue
      }

      if (activeInterval.endEpochMs !== null && event.timestampEpochMs > activeInterval.endEpochMs) {
        intervals.push({
          startAt: activeInterval.startAt,
          endAt: activeInterval.endAt,
          source: activeInterval.source,
          isGifted: activeInterval.isGifted,
          planType: activeInterval.planType,
          startEpochMs: activeInterval.startEpochMs,
          endEpochMs: activeInterval.endEpochMs,
        })

        activeInterval = {
          startAt: event.timestamp,
          endAt: null,
          source: event.source,
          isGifted: event.isGifted,
          planType: event.planType,
          startEpochMs: event.timestampEpochMs,
          endEpochMs: null,
        }
        continue
      }

      activeInterval.source = Array.from(new Set([activeInterval.source, event.source])).join(' | ')
      activeInterval.isGifted = activeInterval.isGifted || event.isGifted
      if (activeInterval.planType === 'unknown' && event.planType !== 'unknown') {
        activeInterval.planType = event.planType
      }
      continue
    }
  }

  if (activeInterval) {
    intervals.push({
      startAt: activeInterval.startAt,
      endAt: activeInterval.endAt,
      source: activeInterval.source,
      isGifted: activeInterval.isGifted,
      planType: activeInterval.planType,
      startEpochMs: activeInterval.startEpochMs,
      endEpochMs: activeInterval.endEpochMs,
    })
  }

  intervals.sort((a, b) => a.startEpochMs - b.startEpochMs)

  const nonOverlapping = []
  for (const interval of intervals) {
    if (nonOverlapping.length === 0) {
      nonOverlapping.push(interval)
      continue
    }

    const previous = nonOverlapping[nonOverlapping.length - 1]

    if (previous.endEpochMs !== null && interval.startEpochMs < previous.endEpochMs) {
      const adjustedStartEpoch = previous.endEpochMs
      const adjustedStartIso = new Date(adjustedStartEpoch).toISOString()

      if (interval.endEpochMs !== null && interval.endEpochMs <= adjustedStartEpoch) {
        warnings.push(`Dropped overlapping premium interval from ${interval.source} after normalization.`)
        continue
      }

      interval.startEpochMs = adjustedStartEpoch
      interval.startAt = adjustedStartIso
    }

    nonOverlapping.push(interval)
  }

  const merged = mergeAdjacentIntervals(nonOverlapping)

  return merged.map((interval) => ({
    startAt: interval.startAt,
    endAt: interval.endAt,
    source: interval.source,
    isGifted: interval.isGifted,
    planType: interval.planType,
  }))
}

module.exports = {
  extractPremiumEventsFromPayload,
  normalizePremiumEventsToIntervals,
}
