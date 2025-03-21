defmodule Minne.Upload do
  @moduledoc """
  Similar to Plug.Upload, used internally to manage state of upload.
  Also passed to consumer after done parsing
  """

  @enforce_keys [:adapter]

  @type t() :: %__MODULE__{
          filename: String.t(),
          content_type: String.t(),
          size: non_neg_integer(),
          adapter: map() | atom() | nil,
          chunk_size: non_neg_integer(),
          remainder_bytes: binary() | nil,
          request_url: String.t()
        }

  defstruct filename: "",
            content_type: "",
            size: 0,
            adapter: nil,
            chunk_size: 0,
            remainder_bytes: "",
            request_url: ""

  def new(adapter) do
    %__MODULE__{adapter: adapter}
  end
end
