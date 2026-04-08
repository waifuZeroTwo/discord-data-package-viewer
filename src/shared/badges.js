const PUBLIC_FLAG_BADGES = {
  1: {
    displayName: 'Discord Staff',
    iconKey: 'staff',
    description: 'Official Discord employee account.',
  },
  2: {
    displayName: 'Partnered Server Owner',
    iconKey: 'partner',
    description: 'Owner of a Discord Partner server.',
  },
  4: {
    displayName: 'HypeSquad Events',
    iconKey: 'hypesquad_events',
    description: 'Member of the HypeSquad Events team.',
  },
  8: {
    displayName: 'Bug Hunter Level 1',
    iconKey: 'bug_hunter_1',
    description: 'Reported valid bugs through Discord bug hunting.',
  },
  64: {
    displayName: 'HypeSquad Bravery',
    iconKey: 'hypesquad_bravery',
    description: 'Member of HypeSquad House Bravery.',
  },
  128: {
    displayName: 'HypeSquad Brilliance',
    iconKey: 'hypesquad_brilliance',
    description: 'Member of HypeSquad House Brilliance.',
  },
  256: {
    displayName: 'HypeSquad Balance',
    iconKey: 'hypesquad_balance',
    description: 'Member of HypeSquad House Balance.',
  },
  512: {
    displayName: 'Early Supporter',
    iconKey: 'early_supporter',
    description: 'Supported Discord Nitro in the early period.',
  },
  16384: {
    displayName: 'Bug Hunter Level 2',
    iconKey: 'bug_hunter_2',
    description: 'High-tier Discord bug hunter.',
  },
  65536: {
    displayName: 'Verified Bot Developer',
    iconKey: 'verified_bot_developer',
    description: 'Legacy badge for early verified bot developers.',
  },
  131072: {
    displayName: 'Certified Moderator',
    iconKey: 'certified_moderator',
    description: 'Completed Discord Moderator Programs certification.',
  },
  262144: {
    displayName: 'Bot HTTP Interactions',
    iconKey: 'bot_http_interactions',
    description: 'Bot account using HTTP interactions.',
  },
  524288: {
    displayName: 'Active Developer',
    iconKey: 'active_developer',
    description: 'Active developer building apps for Discord.',
  },
}

const PREMIUM_TYPE_BADGES = {
  1: {
    displayName: 'Nitro Classic',
    iconKey: 'nitro_classic',
    description: 'Legacy Discord Nitro Classic subscription.',
  },
  2: {
    displayName: 'Nitro',
    iconKey: 'nitro',
    description: 'Discord Nitro subscription.',
  },
  3: {
    displayName: 'Nitro Basic',
    iconKey: 'nitro_basic',
    description: 'Discord Nitro Basic subscription.',
  },
}

const LEGACY_BADGE_MAP = {
  discord_employee: {
    displayName: 'Discord Employee',
    iconKey: 'staff',
    description: 'Legacy account marker for Discord employees.',
  },
  discord_partner: {
    displayName: 'Discord Partner',
    iconKey: 'partner',
    description: 'Legacy account marker for Discord Partners.',
  },
  early_verified_bot_developer: {
    displayName: 'Early Verified Bot Developer',
    iconKey: 'verified_bot_developer',
    description: 'Legacy badge for early verified bot developers.',
  },
  moderator_programs_alumni: {
    displayName: 'Moderator Programs Alumni',
    iconKey: 'certified_moderator',
    description: 'Legacy marker for Discord moderation program alumni.',
  },
}

function toFiniteNumber(value) {
  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed : null
}

function mapPublicFlags(rawFlags) {
  const numericFlags = toFiniteNumber(rawFlags)
  if (numericFlags === null || numericFlags <= 0) {
    return []
  }

  const badges = []
  let unknownBits = numericFlags

  for (const [bitAsText, badgeMeta] of Object.entries(PUBLIC_FLAG_BADGES)) {
    const bit = Number(bitAsText)
    if ((numericFlags & bit) !== bit) {
      continue
    }

    unknownBits &= ~bit
    badges.push({
      sourceType: 'public_flags',
      rawValue: bit,
      ...badgeMeta,
    })
  }

  let bitCursor = 1
  while (unknownBits > 0) {
    if ((unknownBits & bitCursor) === bitCursor) {
      badges.push({
        sourceType: 'public_flags',
        rawValue: bitCursor,
        displayName: `Unknown Badge (${bitCursor})`,
        iconKey: 'unknown',
        description: `Unknown public flag bit: ${bitCursor}`,
      })
      unknownBits &= ~bitCursor
    }
    bitCursor <<= 1
  }

  return badges
}

function mapPremiumType(rawPremiumType) {
  const numericType = toFiniteNumber(rawPremiumType)
  if (numericType === null || numericType <= 0) {
    return []
  }

  const known = PREMIUM_TYPE_BADGES[numericType]
  if (known) {
    return [
      {
        sourceType: 'premium_type',
        rawValue: numericType,
        ...known,
      },
    ]
  }

  return [
    {
      sourceType: 'premium_type',
      rawValue: numericType,
      displayName: `Unknown Badge (${numericType})`,
      iconKey: 'unknown',
      description: `Unknown premium type value: ${numericType}`,
    },
  ]
}

function mapLegacyBadge(rawBadgeValue) {
  const key = String(rawBadgeValue || '')
    .trim()
    .toLowerCase()
    .replace(/\s+/g, '_')
  if (!key) {
    return null
  }

  const known = LEGACY_BADGE_MAP[key]
  if (known) {
    return {
      sourceType: 'legacy_badge',
      rawValue: key,
      ...known,
    }
  }

  return {
    sourceType: 'legacy_badge',
    rawValue: key,
    displayName: `Unknown Badge (${key})`,
    iconKey: 'unknown',
    description: `Unknown legacy/event badge identifier: ${key}`,
  }
}

module.exports = {
  mapLegacyBadge,
  mapPremiumType,
  mapPublicFlags,
}
