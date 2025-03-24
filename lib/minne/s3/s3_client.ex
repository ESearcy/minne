defmodule Minne.Clients.S3 do
  @moduledoc """
  Implements the s3 client behavior
  """

  @behaviour Minne.Clients.S3Behaviour
  def put_object(
        bucket,
        key,
        body
      ) do
    ExAws.S3.put_object(bucket, key, body)
    |> ExAws.request!()
  end

  def complete_multipart_upload(
        bucket,
        key,
        upload_id,
        parts
      ) do
    ExAws.S3.complete_multipart_upload(
      bucket,
      key,
      upload_id,
      parts
    )
    |> ExAws.request!()
  end

  def initiate_multipart_upload(
        bucket,
        key
      ) do
    ExAws.S3.initiate_multipart_upload(bucket, key)
    |> ExAws.request!()
  end

  def upload_part(
        bucket,
        key,
        upload_id,
        part_number,
        chunk
      ) do
    ExAws.S3.upload_part(
      bucket,
      key,
      upload_id,
      part_number,
      chunk
    )
    |> ExAws.request!()
  end

  def abort_multipart_upload(
        bucket,
        key,
        upload_id
      ) do
    ExAws.S3.abort_multipart_upload(bucket, key, upload_id)
    |> ExAws.request!()
  end
end
