defmodule DMEx.Test.ClosingAdapter do
  @moduledoc """
  A `Plug.Conn` adapter that reports the socket closed after N chunks.

  The log's send loop only returns when its connection closes, so a test
  that wants the loop to *finish* needs a conn that eventually stops accepting
  chunks. Every chunk is forwarded to the owning process as `{:chunk, binary}`,
  which is also how tests observe the wire bytes.
  """

  @behaviour Plug.Conn.Adapter

  @doc """
  Builds a conn that closes after `chunks` successful writes.

  Starts `:unset`, so a plug under test can set its own response headers and
  call `send_chunked/2` for real. Pass `state: :chunked` to hand the conn
  straight to code that expects an already-open stream.

  `path_info:` is what a mount would have left after its prefix — `[]` for a
  bare `GET /stream`, one segment for a filter blob. It defaults to `[]`, which
  is the whole-log path.
  """
  # `%Plug.Conn{}` rather than `Plug.Conn.t()`: a conn built as a literal keeps
  # Plug's own struct defaults in every field it does not set, and some of those
  # defaults are outside the types `t/0` declares — so no hand-built conn is a
  # `t/0`, and dialyzer rejects the tighter spec. Credo's preference for `t/0`
  # is the general rule and this is the case it does not cover.
  # credo:disable-for-next-line Credo.Check.Warning.SpecWithStruct
  @spec conn(pid(), pos_integer(), keyword()) :: %Plug.Conn{}
  def conn(owner, chunks, opts \\ []) do
    %Plug.Conn{
      adapter: {__MODULE__, {owner, chunks, 0}},
      state: Keyword.get(opts, :state, :unset),
      owner: owner,
      req_headers: Keyword.get(opts, :req_headers, []),
      method: Keyword.get(opts, :method, "GET"),
      path_info: Keyword.get(opts, :path_info, []),
      request_path: "/stream"
    }
  end

  @impl Plug.Conn.Adapter
  def send_chunked(state, _status, _headers), do: {:ok, "", state}

  @impl Plug.Conn.Adapter
  def chunk({owner, limit, sent}, body) do
    send(owner, {:chunk, IO.iodata_to_binary(body)})

    if sent + 1 >= limit do
      {:error, :closed}
    else
      {:ok, "", {owner, limit, sent + 1}}
    end
  end

  @impl Plug.Conn.Adapter
  def send_resp({owner, _, _} = state, status, _headers, body) do
    send(owner, {:resp, status, IO.iodata_to_binary(body)})

    {:ok, "", state}
  end

  @impl Plug.Conn.Adapter
  def send_file(state, _status, _headers, _file, _offset, _length), do: {:ok, "", state}

  @impl Plug.Conn.Adapter
  def read_req_body(state, _opts), do: {:ok, "", state}

  @impl Plug.Conn.Adapter
  def inform(_state, _status, _headers), do: :ok

  @impl Plug.Conn.Adapter
  def upgrade(_state, _protocol, _opts), do: {:error, :not_supported}

  @impl Plug.Conn.Adapter
  def push(_state, _path, _headers), do: {:error, :not_supported}

  @impl Plug.Conn.Adapter
  def get_peer_data(_state), do: %{address: {127, 0, 0, 1}, port: 0, ssl_cert: nil}

  @impl Plug.Conn.Adapter
  def get_http_protocol(_state), do: :"HTTP/1.1"
end
