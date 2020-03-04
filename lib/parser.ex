defmodule TDMS.Parser do
  @moduledoc """
  This module is the main parser for TDMS files.

  TDMS files organize data in a three-level hierarchy of objects.
  The top level is comprised of a single object that holds file-specific information like author or title.
  Each file can contain an unlimited number of groups, and each group can contain an unlimited number of channels.

  In the following illustration, the file example_events.tdms contains two groups, each of which contains two channels:
  - example_events.tdms
    - Measured Data
      - Amplitude Sweep
      - Phase Sweep
    - Events
      - Time
      - Description

  For more details about the internal structure of TDMS files, see https://www.ni.com/product-documentation/5696/en/
  """

  @lean_in_byte_size 28
  @tdms_file_tag "TDSm"
  @tdms_file_version_1 4712
  @tdms_file_version_2 4713

  alias TDMS.Parser.State
  alias TDMS.Parser.ValueParser
  alias TDMS.Parser.ParseError

  @doc """
  Parses the given TDMS file binary data and returns a hierarchical `TDMS.File` structure which
  contains a list of `TDMS.Group` and `TDMS.Channel`.

  ## Examples

      iex> TDMS.Parser.parse(File.read!("test/data/basic.tdms"))

      %TDMS.File{
        path: "/",
        properties: [
          %TDMS.Property{
            data_type: :string,
            name: "name",
            value: "ni-crio-9068-190fdf5_20190609_235850.tdms"
          }
        ],
        groups: [
          %TDMS.Group{
            name: "Temperature",
            path: "/'Temperature'",
            properties: [
              %TDMS.Property{data_type: :string, name: "name", value: "Temperature"}
            ]
            channels: [
              %TDMS.Channel{
                data: [24.172693869632123, 24.238202284912816, 24.22418907461031, ...],
                data_count: 201,
                data_type: :double,
                name: "ai.0",
                path: "/'Temperature'/'ai.0'",
                properties: [
                  %TDMS.Property{data_type: :string, name: "name", value: "ai.0"},
                  %TDMS.Property{
                    data_type: :string,
                    name: "datatype",
                    value: "DT_DOUBLE"
                  },
                  ...
                ]
              },
              %TDMS.Channel{
                data: [24.07053512461277, 24.136787008557807, 24.128304594848682, ...],
                data_count: 201,
                data_type: :double,
                name: "ai.1",
                path: "/'Temperature'/'ai.1'",
                properties: [
                  %TDMS.Property{data_type: :string, name: "name", value: "ai.1"},
                  %TDMS.Property{
                    data_type: :string,
                    name: "datatype",
                    value: "DT_DOUBLE"
                  },
                  ...
                ]
              },
              ...
            ]
          }
        ]
      }
  """
  def parse(stream) do
    try do
      tstart = start = System.monotonic_time(:microsecond)
      {:ok} = validate_tdms_file(stream)
      duration1 = System.monotonic_time(:microsecond) - start
      start = System.monotonic_time(:microsecond)
      {:ok, state, _stream} = parse(stream, State.new())
      duration2 = System.monotonic_time(:microsecond) - start

      IO.puts(
        "\r\n BBBB.1.1 parse(stream) state.parse_lead_in_count=#{
          inspect(state.parse_lead_in_count)
        } state.parse_lead_in_usec=#{inspect(state.parse_lead_in_usec)}"
      )

      IO.puts(
        "\r\n BBBB.1.2 parse(stream) state.parse_raw_data_count=#{
          inspect(state.parse_raw_data_count)
        } state.parse_raw_data_usec=#{inspect(state.parse_raw_data_usec)}"
      )

      IO.puts(
        "\r\n BBBB.1.3 parse(stream) state.parse_metadata_count=#{
          inspect(state.parse_metadata_count)
        } state.parse_metadata_usec=#{inspect(state.parse_metadata_usec)}"
      )

      IO.puts(
        "\r\n BBBB.1.3.1 parse(stream) state.parse_path_preamble_usec=#{
          inspect(state.parse_path_preamble_usec)
        }"
      )

      IO.puts(
        "\r\n BBBB.1.3.2 parse(stream) state.parse_path_read_raw_data_index_usec=#{
          inspect(state.parse_path_read_raw_data_index_usec)
        }"
      )

      IO.puts(
        "\r\n BBBB.1.3.3 parse(stream) state.parse_property_count=#{
          inspect(state.parse_property_count)
        }"
      )

      IO.puts(
        "\r\n BBBB.1.4 parse(stream) state.<all>(sec)=#{
          inspect(
            (state.parse_metadata_usec + state.parse_lead_in_usec + state.parse_raw_data_usec) /
              1_000_000
          )
        }"
      )

      start = System.monotonic_time(:microsecond)
      {state, _result} = build_tdms_file_hierarchy(state)
      duration3 = System.monotonic_time(:microsecond) - start
      tduration = System.monotonic_time(:microsecond) - tstart

      IO.puts(
        "\r\n BBBB.2 build_tdms_file_hierarchy(state) state.parse_channel_count=#{
          inspect(state.parse_channel_count)
        }"
      )

      IO.puts("\r\n BBBB.3 parse(stream) tduration=#{inspect(tduration / 1_000_000)}")

      IO.puts(
        "\r\n BBBB.4   parse(stream) validate_tdms_file(stream) duration1=#{
          inspect(duration1 / 1_000_000)
        } duration1(usec)=#{inspect(duration1)}"
      )

      IO.puts(
        "\r\n BBBB.5   parse(stream) parse(stream, state) duration2=#{
          inspect(duration2 / 1_000_000)
        }"
      )

      IO.puts(
        "\r\n BBBB.6   parse(stream) build_tdms_file_hierarchy(state) duration3=#{
          inspect(duration3 / 1_000_000)
        }"
      )

      # result
    catch
      :throw, %ParseError{message: message} -> {:error, message}
    end
  end

  def parse_file(path) do
    IO.puts("\r\n AAAA.1 About to parse #{path}")
    start = System.monotonic_time(:microsecond)
    stream = File.read!(path)
    duration = System.monotonic_time(:microsecond) - start

    IO.puts("\r\n AAAA.2 File.read!() duration usec=#{inspect(duration)}")

    start = System.monotonic_time(:microsecond)
    result = parse(stream)
    duration = System.monotonic_time(:microsecond) - start
    IO.puts("\r\n AAAA.3 parse() duration usec=#{inspect(duration)} ")
    IO.puts("\r\n AAAA.4 parse() result=#{inspect(result)} ")
    result
  end

  defp validate_tdms_file(stream) do
    case parse_lead_in(stream) do
      {:ok, :empty, _stream} ->
        throw(ParseError.new("Empty file"))

      {:ok, :no_lead_in, _stream} ->
        throw(ParseError.new("No TDMS file"))

      {:ok, _lead_in, _stream} ->
        {:ok}
    end
  end

  defp parse(stream, state) do
    start = System.monotonic_time(:microsecond)
    result = parse_lead_in(stream)
    duration = System.monotonic_time(:microsecond) - start

    new_state = %{
      state
      | parse_lead_in_count: state.parse_lead_in_count + 1,
        parse_lead_in_usec: state.parse_lead_in_usec + duration
    }

    case result do
      {:ok, :empty, stream} ->
        state = State.set_lead_in(new_state, nil)
        {:ok, state, stream}

      {:ok, :no_lead_in, stream} ->
        start = System.monotonic_time(:microsecond)
        {state, stream} = parse_raw_data(stream, new_state)
        duration = System.monotonic_time(:microsecond) - start

        new_state = %{
          state
          | parse_raw_data_count: state.parse_raw_data_count + 1,
            parse_raw_data_usec: state.parse_raw_data_usec + duration
        }

        parse(stream, new_state)

      {:ok, lead_in, stream} ->
        state = State.set_lead_in(new_state, lead_in)
        start = System.monotonic_time(:microsecond)
        {state, stream} = parse_metadata(stream, state)
        duration = System.monotonic_time(:microsecond) - start

        new_state = %{
          state
          | parse_metadata_count: state.parse_metadata_count + 1,
            parse_metadata_usec: state.parse_metadata_usec + duration
        }

        start = System.monotonic_time(:microsecond)
        {state, stream} = parse_raw_data(stream, new_state)
        duration = System.monotonic_time(:microsecond) - start

        new_state = %{
          state
          | parse_raw_data_count: state.parse_raw_data_count + 1,
            parse_raw_data_usec: state.parse_raw_data_usec + duration
        }

        parse(stream, new_state)
    end
  end

  defp parse_raw_data(stream, state) do
    raw_data_indexes = State.get_raw_data_indexes(state)

    {results, stream} =
      parse_data(stream, raw_data_indexes, state.lead_in.interleaved, state.lead_in.endian)

    state =
      Enum.reduce(results, state, fn {path, data}, state ->
        State.add_data(state, path, data)
      end)

    {state, stream}
  end

  defp parse_lead_in(stream) when byte_size(stream) == 0 do
    {:ok, :empty, stream}
  end

  defp parse_lead_in(stream) when byte_size(stream) < @lean_in_byte_size do
    {:ok, :no_lead_in, stream}
  end

  defp parse_lead_in(stream) do
    <<tdms_tag::binary-size(4), stream_without_tdms_tag::binary>> = stream

    case tdms_tag do
      @tdms_file_tag -> parse_toc(stream_without_tdms_tag)
      _ -> {:ok, :no_lead_in, stream}
    end
  end

  defp parse_toc(stream) do
    <<toc::little-unsigned-integer-size(32), stream::binary>> = stream

    endian =
      case TDMS.Parser.ToC.is_big_endian(toc) do
        true -> :big
        false -> :little
      end

    {version, stream} = ValueParser.parse_value(stream, :uint32, endian)
    {segment_length, stream} = ValueParser.parse_value(stream, :uint64, endian)
    {metadata_length, stream} = ValueParser.parse_value(stream, :uint64, endian)

    lead_in = %{
      toc: toc,
      endian: endian,
      interleaved: TDMS.Parser.ToC.is_interleaved(toc),
      segment_length: segment_length,
      metadata_length: metadata_length
    }

    case version do
      @tdms_file_version_1 -> {:ok, lead_in, stream}
      @tdms_file_version_2 -> {:ok, lead_in, stream}
      version -> throw(ParseError.new("Unsupported TDMS version: #{version}"))
    end
  end

  defp parse_metadata(stream, state) do
    {number_of_objects, stream} = ValueParser.parse_value(stream, :uint32, state.lead_in.endian)

    parse_paths(stream, number_of_objects, state)
  end

  defp build_tdms_file_hierarchy(state) do
    grouped_paths =
      Enum.group_by(state.paths, fn {path, _value} -> TDMS.Parser.Path.depth(path) end)

    {new_state, channels} = build_channels(state, grouped_paths[3] || [])
    groups = build_groups(grouped_paths[2] || [], channels)
    {file_path, %{properties: properties}} = List.first(grouped_paths[1])
    {new_state, TDMS.File.new(file_path, properties, groups)}
  end

  defp build_channels(state, paths) do
    new_state = %{state | parse_channel_count: state.parse_channel_count + Enum.count(paths)}

    result =
      sort_paths(paths)
      |> Enum.map(fn {path, %{raw_data_index: raw_data_index, properties: properties}} ->
        data = State.get_data(new_state, path)
        name = TDMS.Parser.Path.get_name(path)
        name_property = TDMS.Property.new("name", :string, name)

        type_property =
          TDMS.Property.new(
            "datatype",
            :string,
            ValueParser.data_type_to_property_value(raw_data_index.data_type)
          )

        TDMS.Channel.new(
          path,
          name,
          raw_data_index.data_type,
          length(data),
          [name_property | [type_property | properties]],
          data
        )
      end)

    {new_state, result}
  end

  defp sort_paths(paths) do
    Enum.sort_by(paths, fn {_path, %{order: order}} -> order end)
  end

  defp build_groups(paths, channels) do
    sorted_paths = sort_paths(paths)
    group_to_channels_map = build_group_to_channels_map(channels)

    sorted_paths
    |> Enum.map(fn {path, %{properties: properties}} ->
      name = TDMS.Parser.Path.get_name(path)
      name_property = TDMS.Property.new("name", :string, name)

      TDMS.Group.new(
        path,
        name,
        [name_property | properties],
        Map.get(group_to_channels_map, path)
      )
    end)
  end

  # defp dump_groups(prefix, groups) do
  #   IO.puts("\r\n#{prefix}")
  #   for group <- groups do
  #     IO.puts(
  #       "\r\n   path=#{inspect(group.path)} channel count=#{inspect(Enum.count(group.channels))}"
  #     )
  #   end
  #   IO.puts("\r\n======================================")
  # end

  defp build_group_to_channels_map(channels) do
    build_group_to_channels_map(channels, %{})
  end

  defp build_group_to_channels_map([], output_map), do: output_map

  defp build_group_to_channels_map([first_channel | remaining], output_map) do
    group_path = TDMS.Parser.Path.get_parent(first_channel.path)
    channel_list = Map.get(output_map, group_path, [])

    build_group_to_channels_map(
      remaining,
      Map.put(output_map, group_path, List.insert_at(channel_list, -1, first_channel))
    )
  end

  defp parse_paths(stream, 0, state) do
    {state, stream}
  end

  defp parse_paths(stream, number_of_objects, state) do
    {state, stream} = parse_path(stream, state)
    parse_paths(stream, number_of_objects - 1, state)
  end

  defp parse_path(stream, state) do
    start = System.monotonic_time(:microsecond)
    {path, stream} = ValueParser.parse_string(stream, state.lead_in.endian)
    <<raw_data_index::binary-size(4), stream::binary>> = stream
    duration = System.monotonic_time(:microsecond) - start
    new_state = %{state | parse_path_preamble_usec: state.parse_path_preamble_usec + duration}

    start = System.monotonic_time(:microsecond)
    {state, stream} = read_raw_data_index(stream, path, raw_data_index, new_state)
    duration = System.monotonic_time(:microsecond) - start

    new_state = %{
      state
      | parse_path_read_raw_data_index_usec: state.parse_path_read_raw_data_index_usec + duration
    }

    {new_state, stream}
  end

  defp read_raw_data_index(stream, path, raw_data_index, state) do
    case parse_raw_data_index(stream, path, raw_data_index, state) do
      {:previous, stream} ->
        previous_path = State.get_path_info(state, path)
        state = State.add_raw_data_index(state, previous_path.raw_data_index)
        {state, stream}

      {raw_data_index, stream} ->
        {number_of_properties, stream} =
          ValueParser.parse_value(stream, :uint32, state.lead_in.endian)

        {properties, stream} = parse_properties(stream, number_of_properties, state, [])

        new_state = %{
          state
          | parse_property_count: state.parse_property_count + number_of_properties
        }

        state =
          new_state
          |> State.add_metadata(path, properties, raw_data_index)
          |> State.add_raw_data_index(raw_data_index)

        {state, stream}
    end
  end

  defp parse_raw_data_index(stream, _path, <<0, 0, 0, 0>>, state) do
    {_empty, stream} = ValueParser.parse_value(stream, :uint32, state.lead_in.endian)
    {:previous, stream}
  end

  defp parse_raw_data_index(stream, path, <<255, 255, 255, 255>>, _state) do
    {%{path: path, data_type: :double, number_of_values: 0}, stream}
  end

  defp parse_raw_data_index(_stream, _path, <<69, 12, 00, 00>>, _state) do
    throw(ParseError.new("DAQmx Format Changing Scaler Parser is not implemented"))
  end

  defp parse_raw_data_index(_stream, _path, <<69, 13, 00, 00>>, _state) do
    throw(ParseError.new("DAQmx Digital Line Scaler Parser is not implemented"))
  end

  defp parse_raw_data_index(stream, path, _raw_data_index, state) do
    {data_type, stream} = ValueParser.parse_data_type(stream, state.lead_in.endian)

    {array_dimension, stream} = ValueParser.parse_value(stream, :uint32, state.lead_in.endian)

    if array_dimension != 1 do
      throw(
        ParseError.new(
          "In TDMS file format version 2.0, 1 is the only valid value for array dimension"
        )
      )
    end

    {number_of_values, stream} = ValueParser.parse_value(stream, :uint64, state.lead_in.endian)

    {total_size_bytes, stream} =
      case data_type do
        :string ->
          ValueParser.parse_value(stream, :uint64, state.lead_in.endian)

        _ ->
          {nil, stream}
      end

    {%{
       path: path,
       data_type: data_type,
       array_dimension: array_dimension,
       number_of_values: number_of_values,
       total_size_bytes: total_size_bytes
     }, stream}
  end

  defp parse_properties(stream, 0, _state, properties) do
    {Enum.reverse(properties), stream}
  end

  defp parse_properties(stream, number_of_properties, state, properties) do
    {property, stream} = parse_property(stream, state)
    parse_properties(stream, number_of_properties - 1, state, [property | properties])
  end

  defp parse_property(stream, state) do
    {property_name, stream} = ValueParser.parse_string(stream, state.lead_in.endian)
    {data_type, stream} = ValueParser.parse_data_type(stream, state.lead_in.endian)
    {value, stream} = ValueParser.parse_value(stream, data_type, state.lead_in.endian)
    {TDMS.Property.new(property_name, data_type, value), stream}
  end

  defp parse_data(stream, raw_data_indexes, false, endian) do
    raw_data_indexes
    |> Enum.reduce({%{}, stream}, fn raw_data_index, {results, stream} ->
      parse_data_noninterleaved(stream, raw_data_index, endian, results)
    end)
  end

  defp parse_data(stream, raw_data_indexes, true, endian) do
    parse_data_interleaved(stream, raw_data_indexes, endian, %{})
  end

  defp parse_data_interleaved(stream, [], _endian, results) do
    results = Enum.map(results, fn {path, data} -> {path, Enum.reverse(data)} end)
    {results, stream}
  end

  defp parse_data_interleaved(stream, raw_data_indexes, endian, results) do
    raw_data_indexes_with_data =
      raw_data_indexes
      |> Enum.map(fn index -> %{index | number_of_values: index.number_of_values - 1} end)
      |> Enum.filter(fn index -> index.number_of_values >= 0 end)

    {results, stream} =
      parse_channels_interleaved(stream, raw_data_indexes_with_data, endian, results)

    parse_data_interleaved(stream, raw_data_indexes_with_data, endian, results)
  end

  defp parse_channels_interleaved(stream, raw_data_indexes, endian, results) do
    raw_data_indexes
    |> Enum.reduce({results, stream}, fn raw_data_index, {results, stream} ->
      {value, stream} = parse_channel_single_value(stream, raw_data_index.data_type, endian)
      data = results[raw_data_index.path] || []
      results = Map.put(results, raw_data_index.path, [value | data])
      {results, stream}
    end)
  end

  defp parse_channel_single_value(stream, data_type, endian) do
    ValueParser.parse_value(stream, data_type, endian)
  end

  defp parse_data_noninterleaved(stream, raw_data_index, endian, results) do
    {data, stream} = parse_channel_data(stream, raw_data_index, endian, [])
    results = Map.put(results, raw_data_index.path, data)
    {results, stream}
  end

  defp parse_channel_data(stream, nil, _endian, data) do
    {data, stream}
  end

  defp parse_channel_data(
         stream,
         %{data_type: data_type, number_of_values: number_of_values},
         endian,
         data
       ) do
    parse_channel_data(stream, data_type, number_of_values, endian, data)
  end

  defp parse_channel_data(stream, _data_type, 0, _endian, data) do
    {Enum.reverse(data), stream}
  end

  defp parse_channel_data(stream, :string, number_of_values, endian, _data) do
    ValueParser.parse_raw_strings(stream, number_of_values, endian)
  end

  defp parse_channel_data(stream, data_type, number_of_values, endian, data) do
    {value, stream} = ValueParser.parse_value(stream, data_type, endian)
    parse_channel_data(stream, data_type, number_of_values - 1, endian, [value | data])
  end
end
