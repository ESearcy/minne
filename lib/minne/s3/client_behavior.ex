defmodule Minne.Clients.S3Behaviour do
  @moduledoc """
  Behaviour for the CloudflareClient to support mocking in tests.
  """
  @callback put_object(
              bucket :: binary(),
              key :: binary(),
              body :: binary()
            ) :: {:error, any()} | {:ok, map()}

  @callback complete_multipart_upload(
              bucket :: binary(),
              key :: binary(),
              upload_id :: binary(),
              parts :: list()
            ) :: {:error, any()} | {:ok, map()}

  @callback initiate_multipart_upload(
              bucket :: binary(),
              key :: binary()
            ) :: {:error, any()} | {:ok, map()}

  @callback upload_part(
              bucket :: binary(),
              key :: binary(),
              upload_id :: binary(),
              part_number :: integer(),
              chunk :: binary()
            ) :: {:error, any()} | {:ok, map()}

  @callback abort_multipart_upload(
              bucket :: binary(),
              key :: binary(),
              upload_id :: binary()
            ) :: {:error, any()} | {:ok, map()}
end
