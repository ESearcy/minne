defmodule Minne.Adapter.S3 do
  require Logger
  alias Minne.Upload
  @behaviour Minne.Adapter

  # @min_chunk 800
  @min_chunk 5_242_880

  @type t() :: %__MODULE__{
          key: String.t(),
          bucket: String.t(),
          parts: list(),
          parts_count: non_neg_integer(),
          upload_id: String.t() | nil,
          hashes: map()
        }

  defstruct key: "",
            bucket: "",
            parts: [],
            parts_count: 0,
            upload_id: nil,
            hashes: %{}

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
            hashes: %{
              sha256: :crypto.hash_init(:sha256),
              sha: :crypto.hash_init(:sha),
              md5: :crypto.hash_init(:md5)
            }
        }
    }
  end

  @impl Minne.Adapter
  def start(upload, _opts) do
    IO.inspect(upload.adapter, label: :url_start)
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
        %{adapter: %__MODULE__{parts_count: parts_count} = adapter} = upload,
        chunk,
        size,
        final?,
        _opts
      )
      when size < @min_chunk and parts_count == 0 do
    IO.inspect(upload.request_url, label: :url)
    IO.inspect("hit small file upload")

    ExAws.S3.put_object(upload.adapter.bucket, upload.adapter.key, chunk,
      content_disposition: "attachment; filename=\"#{upload.filename}\""
    )
    |> ExAws.request!()

    adapter = adapter |> update_hashes(chunk) |> finalize_hashes()

    %{
      upload
      | size: size + upload.size,
        adapter: adapter
    }
  end

  def write_part(%{adapter: adapter} = upload, chunk, size, final?, _opts) do
    IO.inspect(upload.request_url, label: :url)
    IO.inspect("hit big file upload")
    upload |> set_upload_id() |> upload_part(size, chunk, final?)
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
    reversed_parts = Enum.map(parts, &Task.await/1) |> Enum.reverse()

    ExAws.S3.complete_multipart_upload(
      bucket,
      key,
      upload_id,
      reversed_parts
    )
    |> ExAws.request!()

    %{upload | adapter: %{adapter | parts: reversed_parts}}
  end

  defp set_upload_id(%{adapter: %{upload_id: nil, bucket: bucket, key: key} = adapter} = uploaded) do
    %{body: %{upload_id: upload_id}} =
      ExAws.S3.initiate_multipart_upload(bucket, key,
        content_disposition: "attachment; filename=\"#{uploaded.filename}\""
      )
      |> ExAws.request!()

    %{uploaded | adapter: %{adapter | upload_id: upload_id}}
  end

  defp set_upload_id(%{adapter: %{upload_id: _upload_id}} = uploaded), do: uploaded

  defp gen_key(opts) do
    prefix = Keyword.get(opts, :upload_prefix, "upload")
    key_generator = Keyword.get(opts, :key_generator, &default_key_generator/0)

    Path.join([prefix, key_generator.()])
  end

  defp default_key_generator() do
    :crypto.strong_rand_bytes(64) |> Base.url_encode64() |> binary_part(0, 32)
  end

  # this represents the first chunk.
  # since its always a jacked up size, we batch the first chunk with the second.
  defp upload_part(%{chunk_size: 0} = uploaded, size, chunk, false) do
    parts_count = uploaded.adapter.parts_count + 1

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

    IO.inspect("2 target chunk size #{chunk_size}")

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

          IO.inspect("chunk size: #{size}")
          IO.inspect("total size #{(size + uploaded.size) / 1_048_576} MB")
          IO.inspect("parts_count #{parts_count}")

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
         %{chunk_size: chunk_size, adapter: adapter} = uploaded,
         size,
         remaining_bytes,
         true
       ) do
    size = byte_size(remaining_bytes)
    parts_count = uploaded.adapter.parts_count + 1

    IO.inspect("chunk size: #{size}")
    IO.inspect("total size #{(size + uploaded.size) / 1_048_576} MB")
    IO.inspect("parts_count #{parts_count}")

    new_part_async = upload_async(uploaded, parts_count, remaining_bytes)
    adapter = adapter |> update_hashes(remaining_bytes) |> finalize_hashes() |> IO.inspect()

    upload = %{
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
        ExAws.S3.upload_part(
          uploaded.adapter.bucket,
          uploaded.adapter.key,
          uploaded.adapter.upload_id,
          parts_count,
          chunk
        )
        |> ExAws.request!()

      {parts_count, Minne.get_header(headers, "ETag")}
    end)
  end

  defp update_hashes(%{hashes: %{sha256: sha256, md5: md5, sha: sha}} = adapter, chunk) do
    IO.inspect("adding to hash: #{to_string(chunk)}")

    hashes = %{
      sha256: :crypto.hash_update(sha256, chunk),
      sha: :crypto.hash_update(sha, chunk),
      md5: :crypto.hash_update(md5, chunk)
    }

    %{adapter | hashes: hashes}
  end

  defp finalize_hashes(%{hashes: %{sha256: sha256, md5: md5, sha: sha}} = adapter) do
    hashes = %{
      sha256: :crypto.hash_final(sha256) |> Base.encode16(case: :lower),
      sha: :crypto.hash_final(sha) |> Base.encode16(case: :lower),
      md5: :crypto.hash_final(md5) |> Base.encode16(case: :lower)
    }

    %{adapter | hashes: hashes}
  end
end
