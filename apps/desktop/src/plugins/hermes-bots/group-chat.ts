/**
 * The group-chat room store: the room atoms, the durable record in plugin
 * storage, the bounded cross-client projection that rides the default
 * profile's ui_meta, and the small pure helpers every room surface shares
 * (identity, thread ids, entry append, the #93127 decision predicates).
 *
 * The sync projection lives here rather than in its own module because
 * updateGroupChat schedules it and the scheduler reads the same atom — one
 * store, one writer.
 */

import { atom, host } from '@hermes/plugin-sdk'

import { $botMeta, $lastRoster, botRosterKey } from './data'
import { groupMemberReferencesConnection, markOrphanedGroupMemberDescriptor } from './hygiene'
import { displayName } from './labels'
import { botRosterMeta } from './routing'
import { getPluginCtx } from './shared'
import type {
  Attachment,
  GroupChat,
  GroupHold,
  GroupMember,
  GroupMessage,
  GroupMessageAuthor,
  GroupPrompt,
  RosterRow
} from './types'

/** Optional secondary navigation inside the Bots pane (group-chat rooms). */

/** Group-chat rooms: { [group]: { log: [{from:{kind,name},text,at}], watermarks:{[member]:idx}, epoch, running } }.
 *  Log + watermarks persist via plugin storage; epoch/running are runtime-only. */
export const $groupChats = atom<Record<string, GroupChatRoom>>({})
/** Group whose room view is open in the Bots pane (secondary navigation
 *  inside the pane; a normal row click returns to the roster). */
export const $groupChatWorkspace = atom<null | string>(null)
/** Groups whose latest room activity mentions @user — the needs-you badge. */
export const $groupNeedsYou = atom<Record<string, boolean>>({})
// Pending prompts (clarify questions AND command approvals) raised inside
// hidden group-member sessions, keyed `${group}::${memberKey}` (#90694).
// Members run in invisible plumbing sessions, so a member's blocking prompt
// used to park server-side with no surface to answer it — the user saw
// "is thinking…" until the prompt timeout. The turn poll mirrors each
// member's `open_requests` / `pending_approval` resume fields in here;
// the room renders answer cards from it.
export const $groupClarify = atom<Record<string, GroupPrompt>>({})

const GROUP_CHAT_SYNC_META_KEY = 'hermes-bots-groups'
// Gateway ui_meta is capped after Python JSON serialization. Keep a healthy
// margin below that limit because Python escapes Unicode while JS does not.
const GROUP_CHAT_SYNC_MAX_BYTES = 48000
const GROUP_CHAT_SYNC_MESSAGES = 16
const GROUP_CHAT_SYNC_TEXT_CHARS = 1200
const GROUP_CHAT_SYNC_TRUNCATION_MARK = '… [truncated]'
const GROUP_CHAT_SYNC_IMAGE_CHARS = 24000
let groupChatSyncTimer: ReturnType<typeof setTimeout> | null = null

/** One room inside the bounded ui_meta projection: a compacted log plus the
 *  identity fields, without any of `GroupChat`'s runtime/orchestration state. */
interface GroupChatSyncRoom {
  holdDetection?: boolean
  image?: null | string
  log: GroupMessage[]
  members?: GroupMember[]
  name?: string
  /** At least this many earlier room entries exist that the projection does
   *  not carry (head-trimmed to the message/byte budget). */
  omitted?: number
  revision?: number
  roomId?: string
}

/** The v3 envelope stored under the default profile's `hermes-bots-groups`
 *  ui_meta key. `deleted` maps a room key to its tombstone revision. */
interface GroupChatSyncSnapshot {
  deleted?: Record<string, number>
  rooms: Record<string, GroupChatSyncRoom>
  updatedAt?: number
  version: number
}

/** A queued publish for one gateway, coalesced while the debounce runs. */
interface GroupChatSyncJob {
  allowEmpty?: boolean
  changedRooms?: string[]
  connectionId: string
  deletedRooms?: string[]
}
// Fan-out scheduler state, keyed by gateway connectionId ('' = active/local).
// Every connected gateway carries the full projection so a room survives any
// single gateway being removed and surfaces on every remote backend.
const groupChatSyncPendingByConnection = new Map<string, GroupChatSyncJob>()
const groupChatSyncInFlightConnections = new Set<string>()
const groupChatSyncRetryTimers = new Map<string, ReturnType<typeof setTimeout>>()
const groupChatSyncRetryCounts = new Map<string, number>()
export let groupChatSyncDisposed = false

// Durable disband memory in the mirror's own tombstone shape (room key ->
// tombstone revision). A pending sync job forgets its deletedRooms once the
// retry ladder gives up or the window closes, and "missing remote rooms are
// not deletions" — so a gateway mirror that missed the tombstone push would
// re-merge the room on every later pull. This map rides every publish and
// every pull merge until the room is gone from every mirror (#105275).
const groupChatTombstones: Record<string, number> = {}
const GROUP_CHAT_TOMBSTONES_KEY = 'group-chat-tombstones'

export function groupChatTombstoneMemory(): Record<string, number> {
  return { ...groupChatTombstones }
}

/** Remember a disband durably — ONLY by roomId. A name key would outlive the
 *  room: a same-name recreate starts at syncRevision 0 and the memory is
 *  applied on every pull with `deletedRevision >= syncRevision`, so the
 *  fresh room would be deleted forever. Legacy name-only rooms keep the
 *  job-scoped tombstone of the pending sync (the pre-#105275 behaviour).
 *  Revision = the room's last known sync revision + 1, the ordering the
 *  live tombstone merge applies. */
export function rememberGroupChatTombstone(name: string, roomId?: null | string, syncRevision?: number) {
  if (typeof roomId !== 'string' || !roomId) {
    return Promise.resolve()
  }

  const key = `id:${roomId}`
  groupChatTombstones[key] = Math.max(Number(groupChatTombstones[key] || 0), Math.max(0, Number(syncRevision || 0)) + 1)

  for (const stale of Object.keys(groupChatTombstones)
    .sort((left, right) => groupChatTombstones[right] - groupChatTombstones[left])
    .slice(64)) {
    delete groupChatTombstones[stale]
  }

  try {
    return Promise.resolve(getPluginCtx()?.storage?.set(GROUP_CHAT_TOMBSTONES_KEY, { ...groupChatTombstones }).catch(
      () => undefined
    ))
  } catch {
    return Promise.resolve()
  }
}

export function hydrateGroupChatTombstones(value: unknown) {
  for (const key of Object.keys(groupChatTombstones)) {
    delete groupChatTombstones[key]
  }

  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    return
  }

  for (const [key, revision] of Object.entries(value as Record<string, unknown])) {
    // Older builds persisted `name:` keys too; they are exactly the memory
    // that blocks a same-name recreate, so they are dropped on hydrate.
    if (key.startsWith('id:')) {
      groupChatTombstones[key] = Math.max(0, Number(revision || 0))
    }
  }
}

/** Cut one room body to a per-message budget and mark the cut (the sync
 *  projection and the per-turn delta window share the convention).
 *  Receivers used to see a silent mid-sentence slice with no signal that the
 *  body continued. Keep the mark inside the same char budget so CJK/envelope
 *  accounting does not grow. */
export function compactGroupChatSyncText(text: string, limit = GROUP_CHAT_SYNC_TEXT_CHARS) {
  const raw = String(text || '')

  if (raw.length <= limit) {
    return { text: raw }
  }

  const budget = Math.max(0, limit - GROUP_CHAT_SYNC_TRUNCATION_MARK.length)

  return {
    text: `${raw.slice(0, budget)}${GROUP_CHAT_SYNC_TRUNCATION_MARK}`,
    truncated: true as const
  }
}

/** #114341: the ui_meta mirror is the only on-disk copy of a room, so a
 *  head-trimmed log must say how many earlier entries it does not carry —
 *  a bare slice reads as "the room never said it". */
function noteGroupChatSyncOmitted(room: GroupChatSyncRoom, total: number) {
  const omitted = total - room.log.length

  if (omitted > 0) {
    room.omitted = omitted
  }
}

/** Conservative byte count for the gateway's ensure_ascii JSON encoding.
 *  Python also inserts separator spaces, so reserve one extra byte per JS
 *  structural separator on top of escaped Unicode code-point widths. */
export function groupChatGatewayJsonSize(value: unknown) {
  const json = JSON.stringify(value)
  let bytes = 0

  for (const character of json) {
    // Non-null: string iteration yields whole code points, never an empty string.
    const codePoint = character.codePointAt(0)!

    if (codePoint <= 0x7f) {
      bytes += 1

      if (character === ',' || character === ':') {
        bytes += 1
      }
    } else {
      bytes += codePoint <= 0xffff ? 6 : 12
    }
  }

  return bytes
}

/** Durable room identity for the sync projection. Rooms minted on current
 *  builds carry an immutable roomId; the projection keys rooms by
 *  `id:<roomId>` so rename is a display-name edit, not a distributed
 *  delete+create, and disband tombstones follow the room itself. Legacy
 *  rooms (no roomId) fall back to `name:<name>` keys with the older
 *  revision-gated tombstone semantics. */
export function groupChatRoomKey(name: string, room: GroupChat) {
  return typeof room?.roomId === 'string' && room.roomId ? `id:${room.roomId}` : `name:${String(name)}`
}

/** Lift any historical projection shape (v1 wall-clock, v2 name-keyed) to
 *  the v3 room-key shape so one merge path serves mixed-version fleets. */
function normalizeGroupChatSyncSnapshot(snapshot: GroupChatSyncSnapshot | null | undefined): GroupChatSyncSnapshot {
  if (!snapshot || typeof snapshot !== 'object') {
    return {
      version: 3,
      rooms: {},
      deleted: {},
    }
  }

  if (Number(snapshot.version || 0) >= 3) {
    return {
      version: 3,
      updatedAt: Number(snapshot.updatedAt || 0),
      rooms: snapshot.rooms && typeof snapshot.rooms === 'object' ? snapshot.rooms : {},
      deleted: snapshot.deleted && typeof snapshot.deleted === 'object' ? snapshot.deleted : {},
    }
  }

  const rooms: Record<string, GroupChatSyncRoom> = {}

  for (const [name, room] of Object.entries(snapshot.rooms || {})) {
    if (!room || !Array.isArray(room.log)) {
      continue
    }

    rooms[`name:${name}`] = {
      ...room,
      name,
    }
  }

  const deleted: Record<string, number> = {}

  for (const [name, at] of Object.entries(snapshot.deleted || {})) {
    // v1 tombstones carried wall-clock ms, not gateway revisions — they must
    // not outrank real revisions.
    deleted[`name:${name}`] = Number(snapshot.version || 0) >= 2 ? Math.max(0, Number(at || 0)) : 0
  }

  return {
    version: 3,
    updatedAt: Number(snapshot.updatedAt || 0),
    rooms,
    deleted,
  }
}

/** Compact, display-oriented copy of Desktop's room log for gateway clients.
 *  The live orchestration state stays in plugin storage; this bounded mirror
 *  rides the default profile's ui_meta so mobile can show the same messages.
 *  Newest rooms/messages win when the profile metadata size cap is reached. */
export function groupChatSyncSnapshot(
  // `revision` is the pre-`syncRevision` field name, still read below as a
  // fallback for rooms hydrated from an older plugin-storage record.
  all: Record<string, GroupChat & { revision?: number } = $groupChats.get(),
  deleted: Record<string, number> = {},
): GroupChatSyncSnapshot {
  const ranked = Object.entries(all || {})
    // Empty runtime tombstones are used to stop an in-flight room after
    // disband. They are not real rooms and must never reappear on mobile.
    .filter(([, room]) => room && Array.isArray(room.log) && room.log.length > 0)
    .sort(([, left], [, right]) => {
      const leftAt = Number(left.log[left.log.length - 1]?.at || 0)
      const rightAt = Number(right.log[right.log.length - 1]?.at || 0)

      return rightAt - leftAt
    })

  const rooms: Record<string, GroupChatSyncRoom> = {}

  const boundedDeleted = Object.fromEntries(
    Object.entries(deleted)
      .sort(([, left], [, right]) => Number(right || 0) - Number(left || 0))
      .slice(0, 64),
  )

  const envelope: GroupChatSyncSnapshot = {
    version: 3,
    updatedAt: Date.now(),
    rooms,
    ...Object.keys(boundedDeleted).length
      ? {
          deleted: boundedDeleted,
        }
      : {},
  }

  for (const [name, room] of ranked) {
    const log: GroupMessage[] = room.log.slice(-GROUP_CHAT_SYNC_MESSAGES).map(entry => {
      const compacted = compactGroupChatSyncText(String(entry?.text || ''))

      return {
        ...entry?.id
          ? {
              id: String(entry.id).slice(0, 160),
            }
          : {},
        from: {
          kind: entry?.from?.kind === 'member' ? 'member' : 'user' as const,
          name: String(entry?.from?.name || (entry?.from?.kind === 'member' ? 'Bot' : 'You')).slice(0, 128),
          ...entry?.from?.source
            ? {
                source: String(entry.from.source).slice(0, 128),
              }
            : {},
        },
        text: compacted.text,
        at: Number(entry?.at || 0),
        ...entry?.thread
          ? {
              thread: String(entry.thread).slice(0, 128),
            }
          : {},
        ...compacted.truncated
          ? {
              truncated: true,
            }
          : {},
      }
    })

    const compact: GroupChatSyncRoom = {
      name: String(name).slice(0, 64),
      ...typeof room?.roomId === 'string' && room.roomId
        ? {
            roomId: String(room.roomId).slice(0, 128),
          }
        : {},
      log,
      holdDetection: room.holdDetection !== false,
      revision: Math.max(0, Number(room?.syncRevision ?? room?.revision ?? 0)),
      members: (Array.isArray(room.members) ? room.members : []).slice(0, GROUP_CHAT_MAX_MEMBERS).map(member => ({
        name: String(member?.name || '').slice(0, 128),
        ...member?.handle
          ? {
              handle: String(member.handle).slice(0, 128),
            }
          : {},
        ...member?.connectionId
          ? {
              connectionId: String(member.connectionId).slice(0, 128),
            }
          : {},
        ...member?.connectionKind
          ? {
              connectionKind: String(member.connectionKind).slice(0, 64),
            }
          : {},
        ...member?.connectionLabel
          ? {
              connectionLabel: String(member.connectionLabel).slice(0, 128),
            }
          : {},
        ...member?.sourceScoped
          ? {
              sourceScoped: true,
            }
          : {},
      })),
      ...typeof room?.image === 'string' && room.image.length <= GROUP_CHAT_SYNC_IMAGE_CHARS
        ? {
            image: room.image,
          }
        : {},
    }

    const key = groupChatRoomKey(name, room)
    rooms[key] = compact
    noteGroupChatSyncOmitted(compact, room.log.length)

    while (compact.log.length > 1 && groupChatGatewayJsonSize(envelope) > GROUP_CHAT_SYNC_MAX_BYTES) {
      compact.log.shift()
      noteGroupChatSyncOmitted(compact, room.log.length)
    }

    if (compact.image && groupChatGatewayJsonSize(envelope) > GROUP_CHAT_SYNC_MAX_BYTES) {
      delete compact.image
    }

    if (groupChatGatewayJsonSize(envelope) > GROUP_CHAT_SYNC_MAX_BYTES) {
      delete rooms[key]
    }
  }

  return envelope
}

function groupChatSyncEntryKey(entry: GroupMessage) {
  if (entry?.id) {
    return `id:${String(entry.id)}`
  }

  return JSON.stringify([
    Number(entry?.at || 0),
    String(entry?.from?.kind || ''),
    String(entry?.from?.name || ''),
    String(entry?.from?.source || ''),
    // Threadless entries (pre-thread rooms, older Desktop builds) get
    // SYNTHETIC `legacy-N` ids from assignLegacyThreads. Those ids are
    // position-derived — not stable across a gateway round-trip (the
    // projection copy may be threadless or numbered differently). Collapse
    // the whole synthetic family to one bucket, or the merge duplicates
    // every id-less entry — shifting watermarks and manufacturing phantom
    // member turns that re-submit into busy sessions.
    String(entry?.thread || 'legacy').replace(/^legacy-\d+$/, 'legacy'),
    String(entry?.text || ''),
  ])
}

/** Members dedupe on durable identity — the same (connectionId, name) pair
 *  botRosterKey seats them by everywhere else. `connectionLabel` and `handle`
 *  are display strings each machine re-derives (a connection rename, an older
 *  build with no handle), so keying on them seats one bot twice; both copies
 *  then answer to a single groupMemberKey in watermarks/sessions/stranded and
 *  the round engine gives that bot two turns. Deliberately unconditional,
 *  unlike groupMemberKey: the projection stamps `remoteSource` onto members it
 *  merges in, so a scoped/unscoped branch would fork a member from its own
 *  previously-merged copy. */
function groupChatSyncMemberKey(member: GroupMember) {
  return botRosterKey(member)
}

/** Merge two bounded projections without treating an absent room/message as
 *  deletion. Rooms are identified by durable room keys (id:<roomId> when the
 *  room carries one), so a rename is a same-key field update — never a
 *  distributed delete+create — and a disband tombstone follows the room
 *  itself. Gateway revisions order identity/membership/picture and
 *  tombstones; stable message ids make concurrent log union idempotent.
 *  `changedRooms`/`deletedRooms` accept display names or room keys. */
export function mergeGroupChatSyncSnapshots(
  remote: GroupChatSyncSnapshot | null | undefined,
  local: GroupChatSyncSnapshot | null | undefined,
  {
    changedRooms = [],
    deletedRooms = [],
    writeRevision = 0,
  } = {},
) {
  const remoteNorm = normalizeGroupChatSyncSnapshot(remote)
  const localNorm = normalizeGroupChatSyncSnapshot(local)

  const keysFor = (label: string, norm: GroupChatSyncSnapshot) => {
    const keys = new Set<string>()
    for (const [name, room] of Object.entries(norm.rooms)) {
      keys.add(groupChatRoomKey(name, room))
    }
    for (const key of Object.keys(norm.deleted)) {
      keys.add(key)
    }
    return keys
  }

  const merged: GroupChatSyncSnapshot = {
    version: 3,
    updatedAt: Math.max(Number(remoteNorm.updatedAt || 0), Number(localNorm.updatedAt || 0)),
    rooms: {},
    deleted: {},
  }

  const allKeys = new Set<string>([
    ...keysFor('remote', remoteNorm),
    ...keysFor('local', localNorm),
  ])

  for (const key of allKeys) {
    const remoteRoom = remoteNorm.rooms[key]
    const localRoom = localNorm.rooms[key]
    const remoteDeleted = Number(remoteNorm.deleted[key] || 0)
    const localDeleted = Number(localNorm.deleted[key] || 0)

    if (remoteDeleted > 0 && localDeleted > 0) {
      merged.deleted[key] = Math.max(remoteDeleted, localDeleted)
      continue
    }

    if (remoteDeleted > 0 && !localRoom) {
      merged.deleted[key] = remoteDeleted
      continue
    }

    if (localDeleted > 0 && !remoteRoom) {
      merged.deleted[key] = localDeleted
      continue
    }

    if (!remoteRoom && localRoom) {
      merged.rooms[key] = localRoom
      continue
    }

    if (remoteRoom && !localRoom) {
      merged.rooms[key] = remoteRoom
      continue
    }

    if (!remoteRoom || !localRoom) {
      continue
    }

    const remoteRev = Number(remoteRoom.revision || 0)
    const localRev = Number(localRoom.revision || 0)

    if (remoteRev > localRev) {
      merged.rooms[key] = remoteRoom
    } else if (localRev > remoteRev) {
      merged.rooms[key] = localRoom
    } else {
      // Same revision: union logs by stable id, keep newest metadata.
      const byId = new Map<string, GroupMessage>()
      for (const entry of [...remoteRoom.log, ...localRoom.log]) {
        byId.set(groupChatSyncEntryKey(entry), entry)
      }
      const log = Array.from(byId.values()).sort(
        (a, b) => Number(a.at || 0) - Number(b.at || 0),
      )
      merged.rooms[key] = {
        ...localRoom,
        ...remoteRoom,
        log,
        revision: writeRevision || localRev,
      }
    }
  }

  for (const room of Object.values(merged.rooms)) {
    noteGroupChatSyncOmitted(room, room.log.length)
  }

  return merged
}

/** Append one entry to a room log, dedupe by stable id. */
export function appendGroupChatEntry(
  room: GroupChat,
  entry: GroupMessage,
): GroupMessage[] {
  const log = Array.isArray(room.log) ? room.log.slice() : []
  const key = groupChatSyncEntryKey(entry)
  if (!log.some(existing => groupChatSyncEntryKey(existing) === key)) {
    log.push(entry)
  }
  return log
}

/** Assign synthetic legacy thread ids to pre-thread room entries. */
export function assignLegacyThreads(log: GroupMessage[]): GroupMessage[] {
  return log.map((entry, index) => ({
    ...entry,
    thread: entry.thread || `legacy-${index}`,
  }))
}

/** Resolve the thread id for a group message. */
export function groupThreadOf(entry: GroupMessage): string {
  return String(entry?.thread || 'legacy')
}

/** Mint a fresh thread id. */
export function mintGroupThreadId(): string {
  return `thread-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
}

/**
 * kai 2026-10-05: raised group-chat caps from 3/10/2 to 999 so rooms only
 * stop when the user hits Stop. Fork of NousResearch/hermes-agent.
 */
export const GROUP_CHAT_MAX_ROUNDS = 999
export const GROUP_CHAT_MAX_MESSAGES = 999
export const GROUP_CHAT_MAX_CONTINUATIONS = 999
export const GROUP_CHAT_MAX_MEMBERS = 64
