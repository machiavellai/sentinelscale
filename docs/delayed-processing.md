# Delayed Processing — How It Works

This doc explains BullMQ's delay mechanism and walks through how SentinelScale
uses it in `src/routes/upload.route.js` and `src/sockets/progress.socket.js`.
It's written for someone new to BullMQ, so it spells things out rather than
assuming prior knowledge.

## 1. What `opts.delay` actually is

Every BullMQ job has three lifecycle-relevant option groups: retries
(`attempts`/`backoff`), cleanup (`removeOnComplete`/`removeOnFail`), and
scheduling (`delay`). This doc is only about the third one.

When you call:

```js
queue.add('job-name', data, { delay: 15000 });
```

BullMQ does **not** run the job in 15 seconds. It stores the job in a Redis
sorted set — internally called the "delayed" set — scored by the timestamp
`now + delay`. A background process inside BullMQ periodically checks that
set and moves any job whose score has passed into the "waiting" set, where an
idle Worker can finally pick it up. So `delay` means "how long from now
before this job is even eligible to be picked up" — it says nothing about how
long the job itself takes to run once it starts.

**Don't confuse this with `backoff.delay`.** SentinelScale's queue
(`src/queues/ingestion.queue.js`) already has:

```js
backoff: { type: 'exponential', delay: 1000 }
```

That `delay` is the wait time *between retry attempts* after a job fails —
a completely different concept living in a different part of the options
object. `opts.delay` (top-level) is the *initial* wait before a job's first
attempt. Same field name, unrelated purpose.

## 2. Two ways to ask for a delay

SentinelScale's upload endpoint accepts either of these optional form fields
alongside the uploaded file:

- **`delay`** — a plain number of milliseconds. `delay=15000` means "process
  this 15 seconds from now."
- **`processAt`** — an absolute date/time string (anything `Date.parse` can
  read, e.g. an ISO 8601 timestamp like `2026-07-04T09:00:00Z`). The server
  converts this into a millisecond delay by subtracting the current time.

Only one is allowed per upload. If both are sent, the server rejects the
request with a 400 — there's no sensible rule for "which one wins" that
wouldn't just be a guess, so we don't guess.

## 3. Walkthrough of `upload.route.js`

```js
const MAX_DELAY_MS = parseInt(process.env.MAX_DELAY_MS, 10) || 24 * 60 * 60 * 1000;
```
Caps how far into the future an upload can be scheduled. Without a cap, a
client could schedule a job years out, and the uploaded file would sit on
disk the entire time doing nothing. Configurable via env var, same pattern
as the pre-existing `MAX_QUEUE_SIZE`.

```js
const scheduling = {};
```
A separate bucket from `processingOptions`. `processingOptions` is forwarded
straight into the Piscina image/video workers — those workers have no
business knowing about scheduling fields, so `delay`/`processAt` are routed
into their own object instead of being mixed in.

Inside `bb.on('field', ...)`, the two scheduling field names are
special-cased into `scheduling`; everything else keeps flowing into
`processingOptions` exactly as before.

Inside `bb.on('finish', ...)`, once the whole multipart body has arrived:

1. **Reject if both fields are present.** One code path, one error message.
2. **Resolve `delayMs`:**
   - From `delay`: `Number(...)`, then checked with `Number.isFinite` *and*
     `Number.isInteger`. This matters because `Number('abc')` is `NaN`, and
     `if (!NaN)` is `true` in JavaScript — a naive falsy check would let
     garbage input slip through silently. `Number.isFinite` is the correct
     way to catch it.
   - From `processAt`: `Date.parse(...)` gives milliseconds-since-epoch;
     subtracting `Date.now()` turns "run at this clock time" into "run this
     many milliseconds from now," which is the only form BullMQ understands.
   - If neither field was sent: `delayMs = 0`, meaning "no delay" — identical
     to the upload's original behavior before this feature existed.
3. **One shared validation catches two different mistakes:** `delayMs < 0`
   is true both when someone sends a negative `delay` directly, *and* when a
   `processAt` timestamp turns out to be in the past. Same underlying
   problem (asking to schedule something in the past), one check.
4. **Cap check:** `delayMs > MAX_DELAY_MS` → 400.
5. **Pass it through:** `ingestionQueue.add('process-media', {...}, { jobId, delay: delayMs })`.
   When `delayMs` is `0`, this is functionally identical to not passing
   `delay` at all — BullMQ treats a zero delay as immediate.
6. **Response:** the 202 body only mentions `scheduledFor` when a delay
   actually applies — no reason to tell a caller "scheduled for right now."

## 4. Walkthrough of `progress.socket.js`

```js
queueEvents.on('delayed', ({ jobId, delay }) => {
  io.to(jobId).emit('job:scheduled', {
    jobId,
    processAt: new Date(Number(delay)).toISOString(),
  });
});
```

This fires once, right when a delayed job is first stored in BullMQ's
delayed set — i.e., right after the upload route's `queue.add(..., { delay })`
call succeeds. Without it, a client subscribed to a delayed job's `jobId`
would hear nothing at all until the delay expires and the job actually
starts running; this event lets a UI show "scheduled for X" in the meantime.

**The gotcha worth remembering:** BullMQ names this event's payload field
`delay`, but it is *not* a duration — it's the absolute timestamp
(milliseconds since epoch) at which the job becomes eligible to run, and it
arrives as a string over Redis rather than a number. `Number(delay)` converts
it before wrapping it in `new Date(...)`.

## 5. How to test it manually

```bash
docker-compose up -d
npm run dev
```

```bash
curl -X POST http://localhost:3000/api/upload \
  -F "file=@test.jpg;type=image/jpeg" \
  -F "delay=15000"
```
Expect a `202` with a `jobId` and a `scheduledFor` timestamp roughly 15
seconds out.

```bash
redis-cli ZRANGE bull:media-ingestion:delayed 0 -1 WITHSCORES
```
The job should appear in the delayed sorted set, scored by its target
timestamp, until the delay elapses — at which point it disappears from this
set and normal `job:progress`/`job:completed` events fire as usual.

**Validation matrix** (all should return 400 except the last):
- `delay=-100`
- `delay=99999999999` (exceeds `MAX_DELAY_MS`)
- `delay=abc`
- both `delay` and `processAt` set
- `processAt` set to a timestamp in the past
- `processAt` set to a valid future ISO timestamp → `202`, not `400`
