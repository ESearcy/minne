defmodule Minne.Adapter.S3 do
  alias Minne.Upload
  @behaviour Minne.Adapter

  @min_chunk 5_242_880

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

  # hashes: %{}

  @impl Minne.Adapter
  def default_opts() do
    [length: 30_000_000_000, read_length: @min_chunk]
  end

  @impl Minne.Adapter
  def init(%{request_url: url} = upload, opts) do
    %{
      upload
      | adapter: %{
          upload.adapter
          | bucket: opts[:bucket],
            max_file_size: opts[:max_file_size],
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
    upload
  end

  @impl Minne.Adapter
  def write_part(
        %{adapter: %__MODULE__{parts_count: parts_count} = adapter, request_url: url} = upload,
        chunk,
        size,
        _opts
      )
      when size < @min_chunk and parts_count == 0 do
    adapter =
      adapter
      |> update_adapter_hashes(chunk)
      |> set_upload_key(url)

    ExAws.S3.put_object(upload.adapter.bucket, adapter.key, chunk,
      content_disposition: "attachment; filename=\"#{upload.filename}\""
    )
    |> ExAws.request!()

    adapter = adapter |> update_adapter_hashes(chunk)

    %{
      upload
      | size: size + upload.size,
        adapter: adapter
    }
  end

  def write_part(%{adapter: adapter, request_url: url} = upload, chunk, size, _opts) do
    IO.inspect(url, label: :url2)
    adapter = adapter |> update_adapter_hashes(chunk)
    upload = upload |> set_upload_id() |> upload_part(size, chunk)
    %{upload | adapter: adapter}
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
      folder <> "/#{now.year}/#{pad(now.month)}/#{pad(now.day)}/#{pad(now.hour)}/" <> UUID.uuid4()

    %{adaptor | key: key}
  end

  defp pad(value) when value < 10, do: "0#{value}"
  defp pad(value), do: Integer.to_string(value)

  @impl Minne.Adapter
  def close(%{adapter: %{upload_id: nil} = adapter} = upload, _opts) do
    adapter = adapter |> finalize_hashes()
    %{upload | adapter: adapter}
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

    adaptor = %{adapter | parts: reversed_parts} |> finalize_hashes()
    %{upload | adapter: adaptor}
  end

  defp finalize_hashes(
         %{
           hashes: %{
             md5: md5,
             sha256: sha256,
             sha: sha
           }
         } = adapter
       ) do
    new_sha256 = :crypto.hash_final(sha256) |> Base.encode16(case: :lower)
    new_sha = :crypto.hash_final(sha) |> Base.encode16(case: :lower)
    new_md5 = :crypto.hash_final(md5) |> Base.encode16(case: :lower)
    %{adapter | hashes: %{sha256: new_sha256, sha: new_sha, md5: new_md5}}
    # adapter
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

  defp upload_part(%{} = uploaded, size, body) do
    parts_count = uploaded.adapter.parts_count + 1

    new_part_async = upload_async(uploaded, parts_count, body)

    %{
      uploaded
      | size: size + uploaded.size,
        adapter: %{
          uploaded.adapter
          | parts_count: parts_count,
            parts: [new_part_async | uploaded.adapter.parts]
        }
    }
  end

  # launches async task to upload this part.
  defp upload_async(uploaded, parts_count, body) do
    Task.async(fn ->
      %{headers: headers} =
        ExAws.S3.upload_part(
          uploaded.adapter.bucket,
          uploaded.adapter.key,
          uploaded.adapter.upload_id,
          parts_count,
          body
        )
        |> ExAws.request!()

      {parts_count, Minne.get_header(headers, "ETag")}
    end)
  end
end
