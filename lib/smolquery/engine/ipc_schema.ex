defmodule Smolquery.Engine.IpcSchema do
  @moduledoc """
  The top-level field names of an Arrow IPC stream, read from its schema
  message without decoding a batch.

  `Smolquery.Engine.Connection.frame/4` loads a result into Polars through
  Arrow IPC, and Polars' IPC reader does not refuse a schema that names a
  column twice: with a batch it panics on the mismatch between fields and
  columns, and with none it keeps one of the two silently. So the names are
  read here first, and a result that repeats one is refused before Polars
  sees it.

  The schema message is the stream's first: a continuation marker
  (`0xFFFFFFFF`), the metadata's length, and a `Message` flatbuffer whose
  header is the `Schema` table. Only the tables on the way to each `Field`'s
  `name` are walked, by the field indices in Arrow's `Message.fbs` and
  `Schema.fbs`.
  """

  @message_header 2
  @schema_fields 1
  @field_name 0

  @doc """
  The names of the stream's top-level fields, in order, or `:error` when
  `ipc` does not start with a schema message this can read. A field Arrow
  leaves unnamed reads as `""`.

      iex> Smolquery.Engine.IpcSchema.field_names(<<0, 1, 2>>)
      :error
  """
  @spec field_names(binary()) :: {:ok, [String.t()]} | :error
  def field_names(
        <<0xFFFFFFFF::32, length::little-signed-32, metadata::binary-size(length), _::binary>>
      ) do
    message = table(metadata, uoffset(metadata, 0))

    with {:ok, schema} <- table_field(metadata, message, @message_header),
         {:ok, fields} <- vector_field(metadata, schema, @schema_fields) do
      names(metadata, fields)
    end
  rescue
    _malformed in [ArgumentError, MatchError] -> :error
  end

  def field_names(_ipc), do: :error

  defp names(buffer, fields) do
    {:ok, Enum.map(fields, &name(buffer, table(buffer, &1)))}
  end

  defp name(buffer, field) do
    case string_field(buffer, field, @field_name) do
      {:ok, name} -> name
      :error -> ""
    end
  end

  defp table(buffer, position) do
    <<_::binary-size(^position), soffset::little-signed-32, _::binary>> = buffer
    {position, position - soffset}
  end

  defp field_position(buffer, {position, vtable}, index) do
    <<_::binary-size(^vtable), vtable_size::little-16, _::binary>> = buffer
    slot = 4 + 2 * index

    if slot < vtable_size do
      at = vtable + slot
      <<_::binary-size(^at), offset::little-16, _::binary>> = buffer
      if offset == 0, do: :error, else: {:ok, position + offset}
    else
      :error
    end
  end

  defp uoffset(buffer, position) do
    <<_::binary-size(^position), offset::little-32, _::binary>> = buffer
    position + offset
  end

  defp table_field(buffer, table, index) do
    with {:ok, position} <- field_position(buffer, table, index),
         do: {:ok, table(buffer, uoffset(buffer, position))}
  end

  defp vector_field(buffer, table, index) do
    with {:ok, position} <- field_position(buffer, table, index) do
      vector = uoffset(buffer, position)
      <<_::binary-size(^vector), count::little-32, _::binary>> = buffer

      {:ok, for(element <- 0..(count - 1)//1, do: uoffset(buffer, vector + 4 + 4 * element))}
    end
  end

  defp string_field(buffer, table, index) do
    with {:ok, position} <- field_position(buffer, table, index) do
      string = uoffset(buffer, position)
      <<_::binary-size(^string), size::little-32, name::binary-size(size), _::binary>> = buffer

      {:ok, name}
    end
  end
end
