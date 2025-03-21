defmodule Minne.Adapter.S3 do
  alias Minne.Upload
  @behaviour Minne.Adapter

  @min_chunk 5_242_880

  @type t() :: %__MODULE__{
          key: String.t(),
          bucket: String.t(),
          parts: list(),
          parts_count: non_neg_integer(),
          upload_id: String.t() | nil
        }

  defstruct key: "",
            bucket: "",
            parts: [],
            parts_count: 0,
            upload_id: nil

  @impl Minne.Adapter
  def default_opts() do
    # might want to make this length adjustable, but for the most part, is OK as S3 can handle pretty much anything.
    [length: 16_000_000_000, read_length: @min_chunk]
  end

  @impl Minne.Adapter
  def init(upload, opts) do
    %{
      upload
      | adapter: %{upload.adapter | key: gen_key(opts), bucket: opts[:bucket]}
    }
  end

  @impl Minne.Adapter
  def start(upload, _opts) do
    upload
  end

  @impl Minne.Adapter
  def write_part(
        %{adapter: %__MODULE__{parts_count: parts_count}} = upload,
        chunk,
        size,
        _opts
      )
      when size < @min_chunk and parts_count == 0 do
    IO.inspect(adapter, label: :one)

    adapter =
      adapter
      |> update_adapter_hashes(chunk)
      |> set_upload_key(url)

    ExAws.S3.put_object(upload.adapter.bucket, adapter.key, chunk,
      content_disposition: "attachment; filename=\"#{upload.filename}\""
    )
    |> ExAws.request!()

    %{
      upload
      | size: size + upload.size
    }

    # else
    #   Logger.error("aborting 1")

    #   {:error,
    #    "File too large for this api. (max: #{adapter.max_file_size}) please use /api/v3/files/upload instead"}
    # end
  end

  def write_part(%{adapter: adapter, request_url: url} = upload, chunk, size, _opts) do
    IO.inspect(adapter, label: :two)
    # if upload.size + size <= adapter.max_file_size do
    adapter =
      adapter
      |> update_adapter_hashes(chunk)
      |> set_upload_key(url)

    upload = upload |> set_upload_id() |> upload_part(size, chunk)
    %{upload | adapter: adapter}
    # else
    #   Logger.error("aborting 2")

    #   ExAws.S3.abort_multipart_upload(adapter.bucket, adapter.key, upload.upload_id)
    #   |> ExAws.request()

    #   {:error,
    #    "File too large for this api. (max: #{adapter.max_file_size} bytes) please use /api/v3/files/upload instead"}
    # end
  end

  defp update_adapter_hashes(
         %{
           hashes: %{
             md5: md5,
             sha256: sha256,
             sha: sha
           }
         } = adapter,
         new_chunk
       ) do
    new_sha256 = :crypto.hash_update(sha256, new_chunk)
    new_sha = :crypto.hash_update(sha, new_chunk)
    new_md5 = :crypto.hash_update(md5, new_chunk)
    %{adapter | hashes: %{sha256: new_sha256, sha: new_sha, md5: new_md5}}
  end

  defp set_upload_key(adaptor, url) do
    segments = String.split(url, "/", trim: true)

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
          Logger.error("unable to identify upload type #{url}")
      end

    now = NaiveDateTime.utc_now()

    key =
      folder <>
        "/#{now.year}/#{pad(now.month)}/#{pad(now.day)}/#{pad(now.hour)}/" <> UUID.uuid4()

    %{adaptor | key: key}
  end

  defp pad(value) when value < 10, do: "0#{value}"
  defp pad(value), do: Integer.to_string(value)

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

  # first uploaded chunk, track its size
  defp upload_part(%{size: 0} = uploaded, size, chunk, false) do
    parts_count = uploaded.adapter.parts_count + 1

    new_part_async = upload_async(uploaded, parts_count, chunk)

    %{
      uploaded
      | size: size + uploaded.size,
        chunk_size: size,
        adapter: %{
          uploaded.adapter
          | parts_count: parts_count,
            parts: [new_part_async | uploaded.adapter.parts]
        }
    }
  end

  # all subsuquent uploads need to be the same size, except the last chunk.
  defp upload_part(
         %{chunk_size: chunk_size, remainder_bytes: remainder} = uploaded,
         size,
         chunk,
         false
       ) do
    chunk = remainder <> chunk

    upload =
      case extract_chunk(chunk, chunk_size) do
        {nil, remaining} ->
          uploaded

        {chunk_to_process, remaining} ->
          size = byte_size(chunk_to_process)
          parts_count = uploaded.adapter.parts_count + 1

          IO.inspect("chunk size: #{size}")
          IO.inspect("total size #{(size + uploaded.size) / 1_048_576} MB")
          IO.inspect("parts_count #{parts_count}")

          new_part_async = upload_async(uploaded, parts_count, chunk_to_process)

          %{
            uploaded
            | size: size + uploaded.size,
              remainder_bytes: remaining,
              adapter: %{
                uploaded.adapter
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
         %{chunk_size: chunk_size} = uploaded,
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

    upload = %{
      uploaded
      | size: size + uploaded.size,
        remainder_bytes: "",
        adapter: %{
          uploaded.adapter
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
end
