# The Selection Filter — Wire Specification

The normative definition of the blob in `GET /stream/<blob>`, written for
someone implementing a DMEx consumer in another language. Where this and the
Elixir code disagree, this document is what a producer expects.

**Nothing here may be negotiated.** The blob carries a seed and a fingerprint
array; every other parameter — the fingerprint width, the hash construction, the
byte order, and the fact that the hashed key is the UTF-8 `message_type` string
— is fixed. This is the one place producer and consumer must agree exactly, and
the reason is the failure mode: disagreement does not produce a garbled filter
that fails loudly, it produces **false negatives**, which is silent message
loss.

## What the filter is

An [XOR filter](https://arxiv.org/abs/1912.08258) (Graf & Lemire) over the set
of `message_type` strings a subscription can decode, with 16-bit fingerprints.

It has **no false negatives**: every type the consumer selected is delivered,
always. A false positive — 2⁻¹⁶ per type tested — admits one extra message type,
which the consumer skips the way it skips any type it has no module for.

The set is static: built once, never added to. That is what makes an XOR filter
the right family here rather than Bloom or cuckoo, and it is ~15% smaller than a
Bloom filter at equal error with three lookups and no probing loop.

## Notation

All arithmetic is on unsigned 64-bit integers, wrapping on overflow, unless
stated otherwise. `>>` is a logical shift right. `^` is XOR.

## 1. Key hashing

The key is the `message_type` string — a producer's fully-qualified message
module name — encoded as **UTF-8**, with no normalization, case folding or
trimming.

```
key_hash(type) = first 8 bytes of SHA-256(utf8(type)), big-endian
```

SHA-256 over a faster non-cryptographic hash for one reason: it is unambiguous
and available identically everywhere. It runs once per type per connect, over at
most a few thousand types.

> `key_hash("A.B")`:
> `SHA-256("A.B") = 4b861d8bb4a8fc608807e14ee8ff4fdaa71d840dda64e6c27d70a835310a932b`,
> so `key_hash = 0x4b861d8bb4a8fc60`.

## 2. Mixing

Keys are combined with the filter's seed through the **murmur3 64-bit
finalizer** (`fmix64`):

```
fmix64(x):
    x ^= x >> 33
    x *= 0xff51afd7ed558ccd
    x ^= x >> 33
    x *= 0xc4ceb9fe1a85ec53
    x ^= x >> 33
    return x

h(key, seed) = fmix64(key_hash + seed)      // wrapping add
```

## 3. Geometry

For a set of `n` distinct types:

```
slots = ceil(1.23 * n) + 32,  rounded up to the next multiple of 3
block = slots / 3
```

`1.23` is the XOR filter's space constant. The `+32` is slack small sets need to
peel reliably — the asymptotic constant is not what governs at `n = 3`.

**A decoder does not compute `slots`.** It reads it from the blob's length (§6),
which is why no length field is carried.

## 4. Positions and fingerprint

Each key maps to exactly three slots, one in each block:

```
rotl64(x, r) = ((x << r) | (x >> (64 - r)))          // 64-bit rotate left
reduce(x, m) = (uint32(x) * m) >> 32                 // Lemire's fastrange

h0 = reduce(h,             block)
h1 = reduce(rotl64(h, 21), block) +     block
h2 = reduce(rotl64(h, 42), block) + 2 * block

fingerprint(h) = (h ^ (h >> 32)) & 0xFFFF
```

`reduce` maps a 32-bit value into `[0, m)` with a multiply and a shift. It is
**not** `x % m`, and substituting a modulo changes which slots a key lands in.

## 5. Membership

```
member?(filter, type):
    h = fmix64(key_hash(type) + filter.seed)
    return fingerprint(h) == fp[h0] ^ fp[h1] ^ fp[h2]
```

Three array reads. This is the only operation a **producer** performs;
construction is a consumer's concern.

## 6. The blob

```
bytes = seed (8 bytes, big-endian) || fp[0] .. fp[slots-1] (2 bytes each, big-endian)
blob  = base64url(bytes), unpadded
```

Unpadded base64url ([RFC 4648 §5](https://www.rfc-editor.org/rfc/rfc4648#section-5)):
its alphabet (`A–Z`, `a–z`, `0–9`, `-`, `_`) is entirely unreserved in a path
segment under [RFC 3986 §2.3](https://www.rfc-editor.org/rfc/rfc3986#section-2.3),
so the blob needs no percent-encoding and no part of it can be read as a path
separator. Standard base64's `+` and `/` would need both, and `=` padding is
noise in a URL.

**Decoding.** Let `L` be the decoded byte count.

```
L >= 10                     otherwise invalid
(L - 8) % 2 == 0            otherwise invalid
slots = (L - 8) / 2
slots % 3 == 0              otherwise invalid
block = slots / 3
```

A blob failing any of those is answered with `400`. One that decodes cleanly is
accepted; there is no integrity check, and there cannot usefully be one — a
well-formed blob built under a *different* encoding decodes fine and silently
matches the wrong things, which is why the parameters above are fixed.

## 7. Construction (consumers only)

Standard XOR filter peeling. Duplicate types must be removed first.

1. **Count.** For every key, compute `h` and its three positions, and for each
   position increment a counter and XOR `h` into an accumulator.
2. **Peel.** Repeatedly take any slot whose counter is exactly 1. The
   accumulator at that slot is the one remaining key `h`. Push `(h, slot)` onto
   a stack, then remove `h` from all three of its positions — decrementing each
   counter and XORing `h` out of each accumulator — queueing any position whose
   counter thereby becomes 1.
3. **Check.** If every key was peeled, continue. Otherwise retry with a new seed
   (§8); construction is randomized and can fail.
4. **Assign.** Walk the stack in **reverse peel order** (i.e. pop it). For each
   `(h, slot)`:

   ```
   fp[slot] = fingerprint(h) ^ fp[h0] ^ fp[h1] ^ fp[h2]
   ```

   `fp[slot]` is still zero at that moment, so including it in the XOR is
   harmless and the query in §5 recovers `fingerprint(h)` exactly.

## 8. Seed selection (consumers only, non-normative)

**A producer reads the seed out of the blob and never derives one**, so an
implementation may choose seeds however it likes. What follows is what DMEx's
own consumer does, recorded so that two DMEx consumers with the same message set
produce byte-identical URLs.

```
seed_0     = key_hash(sorted(types) joined with "\n")
seed_(i+1) = fmix64(seed_i)                        // on peel failure
```

Derived rather than random, so the blob is a pure function of the set: the same
handler compiles to the same URL on every machine and every build, which makes
it diffable, cacheable and correlatable in a proxy log. Sorted, so the order
clauses were written in cannot mint a new URL.

DMEx gives up after **8** seeds and raises at compile time.

## 9. Test vectors

Each is the complete blob for the given type set, under §8's seed derivation.

| types | seed | slots | blob |
| --- | --- | --- | --- |
| `["A.B"]` | `0x4b861d8bb4a8fc60` | 36 | `S4Ydi7So_GAAAAAAAAD8kAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA` |
| `["A.B", "C.D", "E.F"]` | `0x9b673d3aa5d72acd` | 36 | `m2c9OqXXKs0AAAAAAAAAAAAAAAB7wNzAAAAAAAAAtPQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA` |
| `["MyApp.Messages.OrderPlaced", "MyApp.Messages.OrderShipped"]` | `0x4e8bcd4f1869cca4` | 36 | `TovNTxhpzKQAAAAAAAAAAJJHAAAAAAAAAAAAAP2XAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA` |

An implementation reproducing §1–§6 agrees with a DMEx producer even if it picks
different seeds; matching the blobs above additionally confirms §8.

## 10. Measured behaviour

From `mix bench.fingerprint`, an 86-point sweep over set sizes 1–2000:

| | measured | expected |
| --- | --- | --- |
| false negatives | **0** in 45 660 membership tests | 0, by construction |
| false-positive rate | 14.65 ± 1.85 per million | 15.26 per million (2⁻¹⁶) |
| `recovered / encoded` | 1.0 at 84 of 86 points; worst 1.00069 | ~1.0 |
| bytes per type | 2.493 raw at n = 2000 | 2.46 asymptotically |
| chars per type | 3.329 after base64url | — |

`recovered / encoded` models the operationally meaningful figure: against a
producer knowing ten times the types a consumer selects, how much does the
consumer actually admit? A flat 1.0 with rare single-type excursions is the
argument for 16-bit fingerprints. At 8 bits the same universe would admit a
spurious type at roughly every other point, and one unlucky draw means a
connection receives an entire high-volume type it never asked for, for as long
as it stays connected.

## 11. Size limits

The blob lands in the request line, and therefore in every access-log line of
every intermediary. nginx's `large_client_header_buffers` and Apache's
`LimitRequestLine` both default to about 8 KB, which puts the ceiling at
**roughly 2 400 message types**.

This is why the filter is load-bearing rather than an optimization: spelled-out
module names at 25–40 bytes each would exhaust a conservative 2 KB request line
at around sixty types, which is an entirely ordinary handler. The filter is what
makes the URL viable, and the URL is what makes browser consumers viable.

Exceeding the limit fails loudly — a `414` at connect — rather than silently
truncating a selection.

## 12. The `/stream` namespace

`/stream/...` carries the *whole* of what distinguishes one subscription from
another: a connect request has no DMEx headers at all, so a producer answers it
from the request line plus SSE's own `Last-Event-ID`.

**Reserved names are matched before any decode is attempted**, which makes the
guarantee structural rather than probabilistic. The reserved list is closed and
currently holds one name:

- `stats` — a named sub-resource of the producer. Not yet implemented; `404`.

A segment that is neither a reserved name nor a decodable blob is a `400`. A
filter that decodes but matches none of the producer's known message types is a
`404`, which a consumer should **retry** rather than treat as fatal: a consumer
deployed ahead of the producer that will emit its types is legitimately in that
state, and the repair is the producer's next deploy.

An **empty** segment — the trailing slash in `GET /stream/` — is the same
resource as `GET /stream`: the whole log. A trailing slash is the kind of thing
that creeps into a configured URL, and answering it with a fatal `400` would let
one stray character kill a subscription permanently.
