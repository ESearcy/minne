defmodule Minne.Adapter.S3 do
  require Logger
  alias Minne.Upload
  @behaviour Minne.Adapter

  # compile time is fine because these are not env vars
  @client Application.compile_env(:minne, :s3_client) || Minne.Clients.S3
  @min_chunk Application.compile_env(:minne, :chunk_size) || 5_242_880

  @type t() :: %__MODULE__{
          key: String.t(),
          bucket: String.t(),
          parts: list(),
          parts_count: non_neg_integer(),
          upload_id: String.t() | nil,
          hashes: map(),
          max_file_size: non_neg_integer()
        }

  defstruct key: "",
            bucket: "",
            parts: [],
            parts_count: 0,
            upload_id: nil,
            hashes: %{},
            max_file_size: 0

  @impl Minne.Adapter
  def default_opts() do
    # might want to make this length adjustable, but for the most part, is OK as S3 can handle pretty much anything.
    [length: 16_000_000_000, read_length: @min_chunk]
  end

  @impl Minne.Adapter
  def init(upload, opts) do
    %{
      upload
      | adapter: %{
          upload.adapter
          | bucket: opts[:bucket],
            max_file_size: opts[:max_file_size],
            hashes: %{
              sha256: :crypto.hash_init(:sha256),
              sha1: :crypto.hash_init(:sha),
              md5: :crypto.hash_init(:md5)
            }
        }
    }
  end

  @impl Minne.Adapter
  def start(upload, _opts) do
    adapter = %{upload.adapter | key: gen_timestamp_uuid_key(upload.request_url)}
    %{upload | adapter: adapter}
  end

  defp gen_timestamp_uuid_key(path) do
    segments = String.split(path, "/", trim: true)

    folder =
      cond do
        "upload" in segments ->
          "bytes"

        "kind" in segments ->
          Enum.at(segments, 5)

        "escalation" in segments ->
          "escalation"

        "files" in segments ->
          "bytes"

        true ->
          error = "Invalid Kind provided when uploading to #{path}"
          Logger.error(error)
          raise RuntimeError, error
      end

    now = NaiveDateTime.utc_now()

    folder <> "/#{now.year}/#{pad(now.month)}/#{pad(now.day)}/#{pad(now.hour)}/" <> UUID.uuid4()
  end

  defp pad(value) when value < 10, do: "0#{value}"
  defp pad(value), do: Integer.to_string(value)

  @impl Minne.Adapter
  def write_part(
        %Upload{adapter: %__MODULE__{parts_count: parts_count} = adapter} = upload,
        chunk,
        size,
        _final?,
        _opts
      )
      when size < @min_chunk and parts_count == 0 do
    @client.put_object(upload.adapter.bucket, upload.adapter.key, chunk)

    adapter = adapter |> update_hashes(chunk) |> finalize_hashes()

    {:ok,
     %{
       upload
       | size: size + upload.size,
         adapter: adapter
     }}
  end

  def write_part(
        %Upload{adapter: %{max_file_size: max}} = upload,
        chunk,
        size,
        final?,
        _opts
      ) do
    if upload.size + size <= max do
      IO.inspect("hit max")
      upload = upload |> set_upload_id() |> upload_part(size, chunk, final?)
      {:ok, upload}
    else
      abort_upload(upload)
      {:error, "#{upload.request_url} only supports files smaller than (#{max / 1_048_576} MB)"}
    end
  end

  # upload didnt start yet, nothing to abort
  defp abort_upload(%{adapter: %{parts: []}}) do
    %{}
  end

  defp abort_upload(%{adapter: %{bucket: bucket, key: key, upload_id: upload_id}}) do
    @client.abort_multipart_upload(bucket, key, upload_id)
  end

  @impl Minne.Adapter
  def close(%{adapter: %{upload_id: nil}} = upload, _opts) do
    upload
  end

  def close(
        %{
          adapter: %{upload_id: upload_id, bucket: bucket, key: key, parts: parts} = adapter
        } = upload,
        _opts
      ) do
    reversed_parts = Enum.map(parts, fn p -> Task.await(p, 10000) end) |> Enum.reverse()

    @client.complete_multipart_upload(
      bucket,
      key,
      upload_id,
      reversed_parts
    )

    %{upload | adapter: %{adapter | parts: reversed_parts}}
  end

  defp set_upload_id(%{adapter: %{upload_id: nil, bucket: bucket, key: key} = adapter} = uploaded) do
    %{body: %{upload_id: upload_id}} =
      @client.initiate_multipart_upload(bucket, key)

    %{uploaded | adapter: %{adapter | upload_id: upload_id}}
  end

  defp set_upload_id(%{adapter: %{upload_id: _upload_id}} = uploaded), do: uploaded

  # this represents the first chunk.
  # since its always a jacked up size, we batch the first chunk with the second.
  defp upload_part(%{chunk_size: 0} = uploaded, size, chunk, false) do
    upload = %{
      uploaded
      | chunk_size: @min_chunk
    }

    # this moves the chunk to the "remaining" flow to be picked up by the 2ed chunk and so on.
    upload_part(upload, size, chunk, false)
  end

  # all upload chunks need to be the same size, except the last chunk.
  defp upload_part(
         %{chunk_size: chunk_size, remainder_bytes: remainder, adapter: adapter} = uploaded,
         size,
         chunk,
         false
       ) do
    chunk = remainder <> chunk

    upload =
      case extract_chunk(chunk, chunk_size) do
        {<<>>, remaining} ->
          %{
            uploaded
            | remainder_bytes: remaining
          }

        {chunk_to_process, remaining} ->
          size = byte_size(chunk_to_process)
          parts_count = uploaded.adapter.parts_count + 1

          new_part_async = upload_async(uploaded, parts_count, chunk_to_process)
          adapter = adapter |> update_hashes(chunk_to_process)

          %{
            uploaded
            | size: size + uploaded.size,
              remainder_bytes: remaining,
              adapter: %{
                adapter
                | parts_count: parts_count,
                  parts: [new_part_async | uploaded.adapter.parts]
              }
          }

          # ensure no more then 1 chunk worth of data is left in remaining
      end

    if byte_size(upload.remainder_bytes) >= chunk_size do
      upload_part(upload, size, "", false)
    else
      upload
    end
  end

  # ensure final remaining bytes are uploaded
  defp upload_part(
         %{adapter: adapter} = uploaded,
         _size,
         remaining_bytes,
         true
       ) do
    size = byte_size(remaining_bytes)
    parts_count = uploaded.adapter.parts_count + 1

    new_part_async = upload_async(uploaded, parts_count, remaining_bytes)
    adapter = adapter |> update_hashes(remaining_bytes) |> finalize_hashes()

    %{
      uploaded
      | size: size + uploaded.size,
        remainder_bytes: "",
        adapter: %{
          adapter
          | parts_count: parts_count,
            parts: [new_part_async | uploaded.adapter.parts]
        }
    }
  end

  defp extract_chunk(data, size) do
    if byte_size(data) >= size do
      <<chunk::binary-size(size), remainder::binary>> = data
      {chunk, remainder}
    else
      # Not enough data to form a full chunk yet
      {<<>>, data}
    end
  end

  # launches async task to upload this part.
  defp upload_async(uploaded, parts_count, chunk) do
    Task.async(fn ->
      %{headers: headers} =
        @client.upload_part(
          uploaded.adapter.bucket,
          uploaded.adapter.key,
          uploaded.adapter.upload_id,
          parts_count,
          chunk
        )

      {parts_count, Minne.get_header(headers, "ETag")}
    end)
  end

  defp update_hashes(%{hashes: %{sha256: sha256, md5: md5, sha1: sha}} = adapter, chunk) do
    hashes = %{
      sha256: :crypto.hash_update(sha256, chunk),
      sha1: :crypto.hash_update(sha, chunk),
      md5: :crypto.hash_update(md5, chunk)
    }

    %{adapter | hashes: hashes}
  end

  defp finalize_hashes(%{hashes: %{sha256: sha256, md5: md5, sha1: sha}} = adapter) do
    hashes = %{
      sha256: :crypto.hash_final(sha256) |> Base.encode16(case: :lower),
      sha1: :crypto.hash_final(sha) |> Base.encode16(case: :lower),
      md5: :crypto.hash_final(md5) |> Base.encode16(case: :lower)
    }

    %{adapter | hashes: hashes}
  end
end
