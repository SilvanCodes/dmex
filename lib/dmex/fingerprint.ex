defmodule DMEx.Fingerprint do
  @moduledoc """
  A consumer's message list, encoded as an XOR filter for the request path.

  This is the whole of the selection wire format. A consumer builds one at
  compile time from the modules its handler matches on, encodes it into the path
  of the `GET` it opens, and the producer resolves it against its own message
  types to decide which rows to read. See `guides/fingerprint-spec.md` for the
  normative encoding, which a non-Elixir consumer has to reproduce exactly.

  ## Why a filter rather than the names

  `message_type`s are fully-qualified module names, so enumerating them costs
  25-40 bytes each and a handler with sixty of them already overruns a
  conservative request line. An [XOR filter](https://arxiv.org/abs/1912.08258)
  (Graf & Lemire) encodes the same set at ~2.46 bytes per type at 16-bit
  fingerprints, which is what makes the URL — and therefore a bare browser
  `EventSource` with real server-side selection — viable at all.

  XOR filters rather than Bloom or cuckoo because the set is **static**: built
  once from the derived list and never added to. That is exactly the constraint
  XOR filters trade mutability for, and they are ~15% smaller than a Bloom
  filter at equal error and faster to query — three lookups, no probing loop.

  ## The error direction is the point

  There are **no false negatives**: every type the consumer selected is
  delivered, always. A false positive costs one unwanted message type crossing
  the wire, which the consumer's ordinary skip path drops
  (`DMEx.Consumer.Pipeline`, section 2).

  That asymmetry is why membership is worth testing at all, and it is also why
  the encoding may never be negotiated. Producer and consumer disagreeing on a
  single constant here does not produce a garbled filter — it produces false
  negatives, which is silent message loss, and that is the one failure the
  scheme was chosen for not having.

  ## Sizing, and why 16 bits rather than 8

  Membership is tested on the *type string*, not per row. A false positive
  therefore does not sprinkle a small fraction of extra rows across the stream —
  it admits **one entire message type, for the whole life of the connection**.
  At an 8-bit fingerprint (2⁻⁸ ≈ 0.39%) a producer with 100 types and a consumer
  selecting 10 expects ~0.35 spurious types, and if the unlucky one is the
  highest-volume type in the system that connection receives all of it.

  16 bits costs 1.23 extra bytes per type — nothing at these set sizes — and
  cuts that probability by 256×. So the width is **pinned at 16 and is not
  selectable**: carrying it would mean framing it alongside the blob, and a
  leading width byte is indistinguishable from a seed that happens to start with
  that value unless the framing is pinned as carefully as the hash construction.
  One width, no negotiation, nothing to get wrong.

  ## Build time and query time are different times

  `build/1` and `encode/1` run **only at compile time**, in
  `DMEx.Consumer.Handler`. `decode/1` and `member?/2` run at connect time, in
  `DMEx.Producer.Plug`. Neither side pays for the other's half at runtime, and a
  filter that cannot be constructed is a compile error rather than a runtime
  fallback path.
  """

  import Bitwise

  @typedoc """
  A built filter: the seed it was constructed under, the block length its three
  hash positions are derived from, and the fingerprint array.

  The array is a binary rather than a list or a tuple, so `member?/2` is three
  `:binary.at/2` reads and `decode/1` is a slice with no per-element work.
  """
  @type t :: %__MODULE__{
          seed: non_neg_integer(),
          block: pos_integer(),
          fingerprints: binary()
        }

  defstruct [:seed, :block, :fingerprints]

  @u64 0xFFFFFFFFFFFFFFFF
  @u32 0xFFFFFFFF

  # 16-bit fingerprints. Pinned, not configurable — see the module docs.
  @fingerprint_mask 0xFFFF
  @fingerprint_bytes 2

  # The XOR filter's space overhead. 1.23 is the constant from the paper; the
  # +32 is the slack small sets need to peel reliably, since the asymptotic
  # factor is not what governs at n = 3.
  @overhead 1.23
  @slack 32

  # Peeling is randomized and can fail, at which point the only move is a fresh
  # seed. It succeeds with high probability and these sets are tiny, so a bound
  # this small is generous; exhausting it means something is wrong with the
  # input rather than with the dice.
  @max_attempts 8

  @doc """
  Builds a filter over `types`, or says it could not.

  Duplicates are collapsed first — two handler clauses matching the same struct
  are the same selection — and the resulting set must be non-empty.

  `{:error, :construction_failed}` means peeling did not succeed within
  #{@max_attempts} seeds. Its only caller turns that into a compile error, so
  there is no runtime path that has to cope with it.
  """
  @spec build([String.t()]) :: {:ok, t()} | {:error, :empty | :construction_failed}
  def build(types) when is_list(types) do
    case Enum.uniq(types) do
      [] ->
        {:error, :empty}

      types ->
        keys = Enum.map(types, &key_hash/1)

        attempt(keys, seed(types), capacity(length(types)), 1)
    end
  end

  @doc """
  Whether `type` is in the set the filter was built from.

  Never a false negative; a false positive at a rate of 2⁻¹⁶ per type tested.
  Three array reads and no loop, whatever the set size.
  """
  @spec member?(t(), String.t()) :: boolean()
  def member?(%__MODULE__{seed: seed, block: block, fingerprints: fps}, type)
      when is_binary(type) do
    hash = mixed(key_hash(type), seed)
    {h0, h1, h2} = positions(hash, block)

    fingerprint(hash) == bxor(at(fps, h0), bxor(at(fps, h1), at(fps, h2)))
  end

  @doc """
  Encodes a filter as the path segment: the seed then the fingerprint array,
  big-endian, as unpadded base64url.

  Unpadded base64url specifically ([RFC 4648 §5](https://www.rfc-editor.org/rfc/rfc4648#section-5)):
  its alphabet is entirely unreserved in a path segment under
  [RFC 3986](https://www.rfc-editor.org/rfc/rfc3986#section-2.3), so the blob
  needs no percent-encoding and no segment of it can ever be read as a path
  separator. Standard base64's `+` and `/` would need both, and `=` padding is
  noise in a URL.

  No length field is carried: the array length falls out of the decoded byte
  count at a fixed fingerprint width, and the block length falls out of that.
  """
  @spec encode(t()) :: String.t()
  def encode(%__MODULE__{seed: seed, fingerprints: fps}) do
    Base.url_encode64(<<seed::unsigned-big-64, fps::binary>>, padding: false)
  end

  @doc """
  Decodes a path segment back into a filter.

  Rejects anything that is not base64url, and anything whose decoded length is
  not a 64-bit seed followed by a whole, non-empty number of fingerprints
  filling three equal blocks. A caller answers `{:error, :invalid}` with a `400`
  — a client sending a blob DMEx cannot parse has a bug worth hearing about.

  It cannot detect a blob that decodes cleanly but was built under a *different*
  encoding; nothing can, which is why the constants are pinned rather than
  negotiated.
  """
  @spec decode(String.t()) :: {:ok, t()} | {:error, :invalid}
  def decode(blob) when is_binary(blob) do
    with {:ok, <<seed::unsigned-big-64, fps::binary>>} <- Base.url_decode64(blob, padding: false),
         {:ok, count} <- fingerprint_count(fps) do
      {:ok, %__MODULE__{seed: seed, block: div(count, 3), fingerprints: fps}}
    else
      _ -> {:error, :invalid}
    end
  end

  @doc "How many fingerprint slots the array holds. For diagnostics and the sweep."
  @spec slots(t()) :: pos_integer()
  def slots(%__MODULE__{block: block}), do: block * 3

  @doc false
  # Exposed for `bench/fingerprint_sweep.exs`, which tests millions of
  # candidates and would otherwise re-hash the same strings for every set size.
  @spec key_hash(String.t()) :: non_neg_integer()
  def key_hash(type) when is_binary(type) do
    <<hash::unsigned-big-64, _rest::binary>> = :crypto.hash(:sha256, type)

    hash
  end

  @doc false
  # The `member?/2` inner loop, against an already-hashed key. Same reason.
  @spec member_hash?(t(), non_neg_integer()) :: boolean()
  def member_hash?(%__MODULE__{seed: seed, block: block, fingerprints: fps}, key) do
    hash = mixed(key, seed)
    {h0, h1, h2} = positions(hash, block)

    fingerprint(hash) == bxor(at(fps, h0), bxor(at(fps, h1), at(fps, h2)))
  end

  ## Construction

  defp attempt(_keys, _seed, _capacity, attempt) when attempt > @max_attempts do
    {:error, :construction_failed}
  end

  defp attempt(keys, seed, capacity, attempt) do
    block = div(capacity, 3)

    case peel(keys, seed, capacity, block) do
      {:ok, stack} ->
        {:ok, %__MODULE__{seed: seed, block: block, fingerprints: assign(stack, capacity, block)}}

      :error ->
        # A fresh seed, derived rather than random, so a retry is as reproducible
        # as the first attempt was.
        attempt(keys, fmix64(seed), capacity, attempt + 1)
    end
  end

  # The peeling phase: repeatedly take a slot holding exactly one key, remove
  # that key from all three of its slots, and record the pair. A set that peels
  # completely is one whose fingerprints can be assigned so that every member
  # resolves; one that does not needs a different seed.
  #
  # `:atomics` rather than a map or `:array` because this is the one genuinely
  # mutable-array-shaped algorithm in DMEx, and it runs over a few thousand
  # slots at compile time. `signed: false` gives the u64 the XOR accumulator
  # needs without masking on every write.
  defp peel(keys, seed, capacity, block) do
    counts = :atomics.new(capacity, signed: false)
    xors = :atomics.new(capacity, signed: false)

    hashes = Enum.map(keys, &mixed(&1, seed))

    Enum.each(hashes, fn hash ->
      each_position(hash, block, fn slot ->
        :atomics.add(counts, slot, 1)
        :atomics.put(xors, slot, bxor(:atomics.get(xors, slot), hash))
      end)
    end)

    singletons = for slot <- 1..capacity, :atomics.get(counts, slot) == 1, do: slot

    case strip(singletons, counts, xors, block, [], 0) do
      {stack, peeled} when peeled == length(hashes) -> {:ok, stack}
      _ -> :error
    end
  end

  defp strip([], _counts, _xors, _block, stack, peeled), do: {stack, peeled}

  defp strip([slot | rest], counts, xors, block, stack, peeled) do
    # A slot queued as a singleton may since have been emptied by an earlier
    # peel, so the count is re-read rather than trusted.
    if :atomics.get(counts, slot) == 1 do
      hash = :atomics.get(xors, slot)
      freed = remove(hash, counts, xors, block)

      # Prepended, so the finished stack is in *reverse* peel order — which is
      # exactly the order assignment has to run in.
      strip(freed ++ rest, counts, xors, block, [{hash, slot} | stack], peeled + 1)
    else
      strip(rest, counts, xors, block, stack, peeled)
    end
  end

  # Takes a peeled key out of all three of its slots, and reports which of them
  # became singletons and are therefore peelable now.
  #
  # Checking the counts after all three decrements rather than during is safe
  # because a key's three positions are always distinct — one per block, and the
  # blocks are disjoint ranges — so no decrement here can affect another's count.
  defp remove(hash, counts, xors, block) do
    hash
    |> each_position(block, fn slot ->
      :atomics.sub(counts, slot, 1)
      :atomics.put(xors, slot, bxor(:atomics.get(xors, slot), hash))

      slot
    end)
    |> Enum.filter(&(:atomics.get(counts, &1) == 1))
  end

  # Walks the peel stack in reverse peel order, giving each key the slot it was
  # peeled from. Every slot it writes is still zero at that point, so XORing all
  # three positions in is the same as XORing the other two — and the query,
  # which XORs all three, then recovers the fingerprint exactly.
  defp assign(stack, capacity, block) do
    fps = :atomics.new(capacity, signed: false)

    Enum.each(stack, fn {hash, slot} ->
      value =
        each_position(hash, block, fn other -> :atomics.get(fps, other) end)
        |> Enum.reduce(fingerprint(hash), &bxor/2)

      :atomics.put(fps, slot, band(value, @fingerprint_mask))
    end)

    for slot <- 1..capacity, into: <<>>, do: <<:atomics.get(fps, slot)::unsigned-big-16>>
  end

  ## Geometry

  # `:atomics` is 1-indexed, and keeping that offset in exactly one place is
  # what stops it leaking into the peel, the assignment and the query
  # separately.
  defp each_position(hash, block, fun) do
    {h0, h1, h2} = positions(hash, block)

    [fun.(h0 + 1), fun.(h1 + 1), fun.(h2 + 1)]
  end

  defp positions(hash, block) do
    {
      reduce(hash, block),
      reduce(rotl64(hash, 21), block) + block,
      reduce(rotl64(hash, 42), block) + 2 * block
    }
  end

  defp capacity(count) do
    slots = trunc(Float.ceil(@overhead * count)) + @slack

    # Three equal blocks, so the rounding is up to the next multiple of three.
    slots + rem(3 - rem(slots, 3), 3)
  end

  # Lemire's fastrange: a multiply and a shift, mapping a 32-bit value into
  # [0, n) without a division and without the modulo bias `rem/2` would have.
  defp reduce(hash, n), do: bsr(band(hash, @u32) * n, 32)

  defp rotl64(x, n), do: band(bsl(x, n) ||| bsr(x, 64 - n), @u64)

  defp fingerprint(hash), do: band(bxor(hash, bsr(hash, 32)), @fingerprint_mask)

  defp mixed(key, seed), do: fmix64(band(key + seed, @u64))

  # The murmur3 64-bit finalizer. Chosen over splitmix64 or a rotation of the
  # SHA-256 output because it is the construction the reference XOR filter
  # implementations use, which is what makes the spec cheap to reproduce in
  # another language.
  defp fmix64(x) do
    x = band(bxor(x, bsr(x, 33)) * 0xFF51AFD7ED558CCD, @u64)
    x = band(bxor(x, bsr(x, 33)) * 0xC4CEB9FE1A85EC53, @u64)

    bxor(x, bsr(x, 33))
  end

  ## Encoding

  # The seed is derived from the sorted type list rather than drawn at random,
  # so the blob is a pure function of the message list: the same handler
  # compiles to the same URL on every machine and every build, which is what
  # makes it diffable, cacheable and correlatable in a proxy log. Sorted, so
  # reordering a handler's clauses cannot mint a new URL.
  #
  # This derivation is DMEx's own convention and *not* part of the wire format —
  # a producer only ever reads the seed back out of the blob, so another
  # implementation may choose its seed however it likes.
  defp seed(types) do
    types
    |> Enum.sort()
    |> Enum.join("\n")
    |> key_hash()
  end

  defp fingerprint_count(fps) do
    size = byte_size(fps)

    if size > 0 and rem(size, @fingerprint_bytes) == 0 and
         rem(div(size, @fingerprint_bytes), 3) == 0 do
      {:ok, div(size, @fingerprint_bytes)}
    else
      :error
    end
  end

  # Two `:binary.at/2` reads rather than a `:binary.part/3` plus a decode: both
  # are O(1), but this one allocates no sub-binary, and it runs three times per
  # type per connect.
  defp at(fps, index) do
    offset = index * @fingerprint_bytes

    bsl(:binary.at(fps, offset), 8) ||| :binary.at(fps, offset + 1)
  end
end
