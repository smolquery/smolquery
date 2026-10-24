defmodule Smolquery.Engine.IpcSchema do
  @moduledoc """
  The top-level fields of an Arrow IPC stream, read from its schema message
  without decoding a batch: each one's name, and whether Polars can load it.

  `Smolquery.Engine.Connection.frame/4` loads a result into Polars through
  Arrow IPC, and Polars' IPC reader does not refuse a schema that names a
  column twice: with a batch it panics on the mismatch between fields and
  columns, and with none it keeps one of the two silently. So the names are
  read here first, and a result that repeats one is refused before Polars
  sees it.

  Polars' IPC reader also panics on an Arrow interval, which DuckDB exports
  for every `INTERVAL` (T-728). So each field's type is read too, and a field
  that is an interval, or holds one in a child (a list or a struct of
  intervals), is marked before Polars sees the stream: `:interval` for a field
  that is one, `:nested` for a field that holds one.

  The schema message is the stream's first: a continuation marker
  (`0xFFFFFFFF`), the metadata's length, and a `Message` flatbuffer whose
  header is the `Schema` table. Only the tables on the way to each `Field`'s
  `name`, `type_type` and `children` are walked, by the field indices in
  Arrow's `Message.fbs` and `Schema.fbs`.
  """

  @message_header 2
  @schema_fields 1
  @field_name 0
  @field_type_type 2
  @field_children 5
  @interval_type 11

  @typedoc """
  A top-level field: its name, and `:interval` when its own type is an Arrow
  interval, `:nested` when only a child holds one, `nil` when neither.
  """
  @type field :: {String.t(), :interval | :nested | nil}

  @doc """
  The stream's top-level fields, in order, or `:error` when `ipc` does not
  start with a schema message this can read. A field Arrow leaves unnamed
  reads as `""`.

      iex> Smolquery.Engine.IpcSchema.fields(<<0, 1, 2>>)
      :error
  """
  @spec fields(binary()) :: {:ok, [field()]} | :error
  def fields(
        <<0xFFFFFFFF::32, length::little-signed-32, metadata::binary-size(length), _::binary>>
      ) do
    message = table(metadata, uoffset(metadata, 0))

    with {:ok, schema} <- table_field(metadata, message, @message_header),
         {:ok, fields} <- vector_field(metadata, schema, @schema_fields) do
      {:ok,
       Enum.map(fields, fn position ->
         field = table(metadata, position)
         {name(metadata, field), interval_kind(metadata, field)}
       end)}
    end
  rescue
    _malformed in [ArgumentError, MatchError] -> :error
  end

  def fields(_ipc), do: :error

  defp interval_kind(buffer, field) do
    cond do
      type_tag(buffer, field) == @interval_type -> :interval
      interval?(buffer, field) -> :nested
      true -> nil
    end
  end

  defp interval?(buffer, field) do
    type_tag(buffer, field) == @interval_type or
      case vector_field(buffer, field, @field_children) do
        {:ok, children} -> Enum.any?(children, &interval?(buffer, table(buffer, &1)))
        :error -> false
      end
  end

  defp type_tag(buffer, field) do
    case field_position(buffer, field, @field_type_type) do
      {:ok, position} ->
        <<_::binary-size(^position), tag::8, _::binary>> = buffer
        tag

      :error ->
        0
    end
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
