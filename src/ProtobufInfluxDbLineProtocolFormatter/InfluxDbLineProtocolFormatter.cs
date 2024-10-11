#region Copyright notice and license
// Protocol Buffers - Google's data interchange format
// Copyright 2015 Google Inc.  All rights reserved.
//
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file or at
// https://developers.google.com/open-source/licenses/bsd
#endregion

using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Xml.Linq;
using Google.Protobuf.Reflection;
using Google.Protobuf.WellKnownTypes;

namespace Google.Protobuf
{
    /// <summary>
    /// Reflection-based converter from messages to JSON.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Instances of this class are thread-safe, with no mutable state.
    /// </para>
    /// <para>
    /// This is a simple start to get JSON formatting working. As it's reflection-based,
    /// it's not as quick as baking calls into generated messages - but is a simpler implementation.
    /// (This code is generally not heavily optimized.)
    /// </para>
    /// </remarks>
    public sealed class InfluxDbLineProtocolFormatter
    {
        private static long InfluxDbMinTimestampNanoseconds = -9223372036854775808;
        private static long InfluxDbMaxTimestampNanoseconds = 9223372036854775807;
        private static long InfluxDbMinTimestampSeconds =
            InfluxDbMinTimestampNanoseconds / 1_000_000_000;
        private static long InfluxDbMaxTimestampSeconds =
            InfluxDbMaxTimestampNanoseconds / 1_000_000_000;

        internal const string AnyTypeUrlField = "@type";
        internal const string AnyDiagnosticValueField = "@value";
        internal const string AnyWellKnownTypeValueField = "value";
        private const string NameValueSeparator = "=";
        private const string ValueSeparator = ", ";
        private const string MultilineValueSeparator = ",";
        private const char ObjectOpenBracket = '{';
        private const char ObjectCloseBracket = '}';
        private const char ListBracketOpen = '[';
        private const char ListBracketClose = ']';

        /// <summary>
        /// Returns a formatter using the default settings.
        /// </summary>
        public static InfluxDbLineProtocolFormatter Default { get; } =
            new InfluxDbLineProtocolFormatter(Settings.Default);

        // A JSON formatter which *only* exists
        private static readonly InfluxDbLineProtocolFormatter diagnosticFormatter =
            new InfluxDbLineProtocolFormatter(Settings.Default);

        /// <summary>
        /// The JSON representation of the first 160 characters of Unicode.
        /// Empty strings are replaced by the static constructor.
        /// </summary>
        private static readonly string[] CommonRepresentations =
        {
            // C0 (ASCII and derivatives) control characters
            "\\u0000",
            "\\u0001",
            "\\u0002",
            "\\u0003", // 0x00
            "\\u0004",
            "\\u0005",
            "\\u0006",
            "\\u0007",
            "\\b",
            "\\t",
            "\\n",
            "\\u000b",
            "\\f",
            "\\r",
            "\\u000e",
            "\\u000f",
            "\\u0010",
            "\\u0011",
            "\\u0012",
            "\\u0013", // 0x10
            "\\u0014",
            "\\u0015",
            "\\u0016",
            "\\u0017",
            "\\u0018",
            "\\u0019",
            "\\u001a",
            "\\u001b",
            "\\u001c",
            "\\u001d",
            "\\u001e",
            "\\u001f",
            // Escaping of " and \ are required by www.json.org string definition.
            // Escaping of < and > are required for HTML security.
            "",
            "",
            "\\\"",
            "",
            "",
            "",
            "",
            "", // 0x20
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "", // 0x30
            "",
            "",
            "",
            "",
            "\\u003c",
            "",
            "\\u003e",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "", // 0x40
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "", // 0x50
            "",
            "",
            "",
            "",
            "\\\\",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "", // 0x60
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "", // 0x70
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "\\u007f",
            // C1 (ISO 8859 and Unicode) extended control characters
            "\\u0080",
            "\\u0081",
            "\\u0082",
            "\\u0083", // 0x80
            "\\u0084",
            "\\u0085",
            "\\u0086",
            "\\u0087",
            "\\u0088",
            "\\u0089",
            "\\u008a",
            "\\u008b",
            "\\u008c",
            "\\u008d",
            "\\u008e",
            "\\u008f",
            "\\u0090",
            "\\u0091",
            "\\u0092",
            "\\u0093", // 0x90
            "\\u0094",
            "\\u0095",
            "\\u0096",
            "\\u0097",
            "\\u0098",
            "\\u0099",
            "\\u009a",
            "\\u009b",
            "\\u009c",
            "\\u009d",
            "\\u009e",
            "\\u009f"
        };

        static InfluxDbLineProtocolFormatter()
        {
            for (int i = 0; i < CommonRepresentations.Length; i++)
            {
                if (CommonRepresentations[i] == "")
                {
                    CommonRepresentations[i] = ((char)i).ToString();
                }
            }
        }

        private readonly Settings settings;

        private bool DiagnosticOnly => ReferenceEquals(this, diagnosticFormatter);

        /// <summary>
        /// Creates a new formatted with the given settings.
        /// </summary>
        /// <param name="settings">The settings.</param>
        public InfluxDbLineProtocolFormatter(Settings settings)
        {
            this.settings = ProtoPreconditions.CheckNotNull(settings, nameof(settings));
        }

        /// <summary>
        /// Formats the specified message as line protocol.
        /// </summary>
        /// <param name="measurement">Name of the measurement.</param>
        /// <param name="tags">Set of tags.</param>
        /// <param name="message">The message to format.</param>
        /// <returns>The formatted message.</returns>
        public string Format(
            IMessage message,
            string measurement,
            HashSet<string> tags,
            Timestamp timestamp
        )
        {
            var tagWriter = new StringWriter();
            tagWriter.Write(measurement);
            tagWriter.Write(",");

            var fieldWriter = new StringWriter();
            Format(message, tagWriter, fieldWriter, tags, Span<char>.Empty);

            tagWriter.Write(" "); // Separator between tags and fields
            tagWriter.Write(fieldWriter.ToString());

            tagWriter.Write(" "); // Separator between fields and timestamp

            var milliseconds = (timestamp.Seconds * 1000) + (timestamp.Nanos / 1000000);
            tagWriter.Write(milliseconds);

            return tagWriter.ToString();
        }

        private void ValidateTimestamp(Google.Protobuf.WellKnownTypes.Timestamp timestamp)
        {
            var seconds = timestamp.Seconds;
            if (seconds < InfluxDbMinTimestampSeconds || seconds > InfluxDbMaxTimestampSeconds)
                throw new TimestampOutOfRangeException(
                    $"Timestamp outside Influxdb range: {timestamp}"
                );
        }

        /// <summary>
        /// Formats the specified message as JSON. When <see cref="Settings.Indentation"/> is not null,
        /// start indenting at the specified <paramref name="indentationLevel"/>.
        /// </summary>
        /// <param name="message">The message to format.</param>
        /// <param name="writer">The TextWriter to write the formatted message to.</param>
        /// <param name="indentationLevel">Indentation level to start at.</param>
        /// <remarks>To keep consistent indentation when embedding a message inside another JSON string,
        /// set <paramref name="indentationLevel"/>.</remarks>
        public void Format(
            IMessage message,
            TextWriter tagWriter,
            TextWriter fieldWriter,
            HashSet<string> tags,
            Span<char> path
        )
        {
            ProtoPreconditions.CheckNotNull(message, nameof(message));
            ProtoPreconditions.CheckNotNull(tagWriter, nameof(tagWriter));
            ProtoPreconditions.CheckNotNull(fieldWriter, nameof(fieldWriter));

            if (message.Descriptor.IsWellKnownType)
            {
                WriteWellKnownTypeValue(
                    tagWriter,
                    fieldWriter,
                    message.Descriptor,
                    message,
                    tags,
                    path
                );
            }
            else
            {
                WriteMessage(tagWriter, fieldWriter, message, tags, path);
            }
        }

        /// <summary>
        /// Converts a message to JSON for diagnostic purposes with no extra context.
        /// </summary>
        /// <remarks>
        /// <para>
        /// This differs from calling <see cref="Format(IMessage)"/> on the default JSON
        /// formatter in its handling of <see cref="Any"/>. As no type registry is available
        /// in <see cref="object.ToString"/> calls, the normal way of resolving the type of
        /// an <c>Any</c> message cannot be applied. Instead, a JSON property named <c>@value</c>
        /// is included with the base64 data from the <see cref="Any.Value"/> property of the message.
        /// </para>
        /// <para>The value returned by this method is only designed to be used for diagnostic
        /// purposes. It may not be parsable by <see cref="JsonParser"/>, and may not be parsable
        /// by other Protocol Buffer implementations.</para>
        /// </remarks>
        /// <param name="message">The message to format for diagnostic purposes.</param>
        /// <returns>The diagnostic-only JSON representation of the message</returns>
        //public static string ToDiagnosticString(IMessage message)
        //{
        //    ProtoPreconditions.CheckNotNull(message, nameof(message));
        //    return diagnosticFormatter.Format(message);
        //}

        private void WriteMessage(
            TextWriter tagWriter,
            TextWriter fieldWriter,
            IMessage message,
            HashSet<string> tags,
            Span<char> path
        )
        {
            //if (message == null)
            //{
            //    WriteNull(writer);
            //    return;
            //}
            //if (DiagnosticOnly)
            //{
            //    if (message is ICustomDiagnosticMessage customDiagnosticMessage)
            //    {
            //        writer.Write(customDiagnosticMessage.ToDiagnosticString());
            //        return;
            //    }
            //}

            //WriteBracketOpen(path, message);
            WriteMessageFields(tagWriter, fieldWriter, message, tags, path);
            //WriteBracketClose(writer, ObjectCloseBracket, writtenFields, indentationLevel);
        }

        private void WriteMessageFields(
            TextWriter tagWriter,
            TextWriter fieldWriter,
            IMessage message,
            //bool assumeFirstFieldWritten,
            HashSet<string> tags,
            Span<char> path
        )
        {
            var fields = message.Descriptor.Fields;
            // First non-oneof fields
            foreach (var field in fields.InFieldNumberOrder())
            {
                var accessor = field.Accessor;
                var value = accessor.GetValue(message);

                // TODO enable later
                //if (!ShouldFormatFieldValue(message, field, value))
                //{
                //    continue;
                //}

                //MaybeWriteValueSeparator(writer, first);
                //MaybeWriteValueWhitespace(writer, indentationLevel);

                string name = settings.PreserveProtoFieldNames
                    ? accessor.Descriptor.Name
                    : accessor.Descriptor.JsonName;

                var writer = ChooseWriter(tagWriter, fieldWriter, name, tags);
                WriteKey(writer, name, path);

                writer.Write(NameValueSeparator);
                WriteValue(writer, value);
            }
        }

        private Span<char> Combine(Span<char> path, string text)
        {
            var buffer = new char[path.Length + text.Length];

            // Copy the path into the new buffer
            path.CopyTo(buffer);

            // Copy the text into the remaining part of the new buffer
            text.AsSpan().CopyTo(buffer.AsSpan(path.Length));

            // Return the new buffer as a Span<char>
            return buffer;
        }

        private TextWriter ChooseWriter(
            TextWriter tagWriter,
            TextWriter fieldWriter,
            string name,
            HashSet<string> tags
        ) => tags.Contains(name) ? tagWriter : fieldWriter;

        private void WriteKey(TextWriter writer, string name, Span<char> path) =>
            WriteString(writer, Combine(path, name));

        //private void MaybeWriteValueSeparator(
        //    TextWriter tagWriter,
        //    TextWriter fieldWriter,
        //    bool first
        //)
        //{
        //    if (first)
        //    {
        //        return;
        //    }

        //    writer.Write(settings.Indentation == null ? ValueSeparator : MultilineValueSeparator);
        //}

        /// <summary>
        /// Determines whether or not a field value should be serialized according to the field,
        /// its value in the message, and the settings of this formatter.
        /// </summary>
        private bool ShouldFormatFieldValue(
            IMessage message,
            FieldDescriptor field,
            object value
        ) =>
            field.HasPresence
                // Fields that support presence *just* use that
                ? field.Accessor.HasValue(message)
                // Otherwise, format if either we've been asked to format default values, or if it's
                // not a default value anyway.
                : settings.FormatDefaultValues || !IsDefaultValue(field, value);

        // Converted from java/core/src/main/java/com/google/protobuf/Descriptors.java
        internal static string ToJsonName(string name)
        {
            StringBuilder result = new StringBuilder(name.Length);
            bool isNextUpperCase = false;
            foreach (char ch in name)
            {
                if (ch == '_')
                {
                    isNextUpperCase = true;
                }
                else if (isNextUpperCase)
                {
                    result.Append(char.ToUpperInvariant(ch));
                    isNextUpperCase = false;
                }
                else
                {
                    result.Append(ch);
                }
            }
            return result.ToString();
        }

        internal static string FromJsonName(string name)
        {
            StringBuilder result = new StringBuilder(name.Length);
            foreach (char ch in name)
            {
                if (char.IsUpper(ch))
                {
                    result.Append('_');
                    result.Append(char.ToLowerInvariant(ch));
                }
                else
                {
                    result.Append(ch);
                }
            }
            return result.ToString();
        }

        private static void WriteNull(TextWriter writer)
        {
            writer.Write("null");
        }

        private static bool IsDefaultValue(FieldDescriptor descriptor, object value)
        {
            if (descriptor.IsMap)
            {
                IDictionary dictionary = (IDictionary)value;
                return dictionary.Count == 0;
            }
            if (descriptor.IsRepeated)
            {
                IList list = (IList)value;
                return list.Count == 0;
            }
            return descriptor.FieldType switch
            {
                FieldType.Bool => (bool)value == false,
                FieldType.Bytes => (ByteString)value == ByteString.Empty,
                FieldType.String => (string)value == "",
                FieldType.Double => (double)value == 0.0,
                FieldType.SInt32
                or FieldType.Int32
                or FieldType.SFixed32
                or FieldType.Enum
                    => (int)value == 0,
                FieldType.Fixed32 or FieldType.UInt32 => (uint)value == 0,
                FieldType.Fixed64 or FieldType.UInt64 => (ulong)value == 0,
                FieldType.SFixed64 or FieldType.Int64 or FieldType.SInt64 => (long)value == 0,
                FieldType.Float => (float)value == 0f,
                FieldType.Message or FieldType.Group => value == null,
                _ => throw new ArgumentException("Invalid field type"),
            };
        }

        /// <summary>
        /// Writes a single value to the given writer as JSON. Only types understood by
        /// Protocol Buffers can be written in this way. This method is only exposed for
        /// advanced use cases; most users should be using <see cref="Format(IMessage)"/>
        /// or <see cref="Format(IMessage, TextWriter)"/>.
        /// </summary>
        /// <param name="writer">The writer to write the value to. Must not be null.</param>
        /// <param name="value">The value to write. May be null.</param>
        /// <param name="indentationLevel">The current indentationLevel. Not used when <see
        /// cref="Settings.Indentation"/> is null.</param>
        public void WriteValue(TextWriter writer, object value)
        {
            if (value == null || value is NullValue)
            {
                WriteNull(writer);
            }
            else if (value is bool b)
            {
                writer.Write(b ? "true" : "false");
            }
            else if (value is ByteString byteString)
            {
                // Nothing in Base64 needs escaping
                writer.Write('"');
                writer.Write(byteString.ToBase64());
                writer.Write('"');
            }
            else if (value is string str)
            {
                WriteString(writer, str.AsSpan());
            }
            //else if (value is IDictionary dictionary)
            //{
            //    WriteDictionary(writer, dictionary, measurement, tags);
            //}
            //else if (value is IList list)
            //{
            //    WriteList(writer, list, measurement, tags);
            //}
            else if (value is int || value is uint)
            {
                IFormattable formattable = (IFormattable)value;
                writer.Write(formattable.ToString("d", CultureInfo.InvariantCulture));
            }
            else if (value is long || value is ulong)
            {
                writer.Write('"');
                IFormattable formattable = (IFormattable)value;
                writer.Write(formattable.ToString("d", CultureInfo.InvariantCulture));
                writer.Write('"');
            }
            else if (value is System.Enum)
            {
                if (settings.FormatEnumsAsIntegers)
                {
                    WriteValue(writer, (int)value);
                }
                else
                {
                    string name = OriginalEnumValueHelper.GetOriginalName(value);
                    if (name != null)
                    {
                        WriteString(writer, name.AsSpan());
                    }
                    else
                    {
                        WriteValue(writer, (int)value);
                    }
                }
            }
            else if (value is float || value is double)
            {
                string text = ((IFormattable)value).ToString("r", CultureInfo.InvariantCulture);
                if (text == "NaN" || text == "Infinity" || text == "-Infinity")
                {
                    writer.Write('"');
                    writer.Write(text);
                    writer.Write('"');
                }
                else
                {
                    writer.Write(text);
                }
            }
            else if (value is IMessage message)
            {
                // TODO implement
                //Format(message, tagWriter, fieldWriter, indentationLevel);
            }
            // TODO throw here
            //else
            //{
            //    throw new ArgumentException("Unable to format value of type " + value.GetType());
            //}
        }

        /// <summary>
        /// Central interception point for well-known type formatting. Any well-known types which
        /// don't need special handling can fall back to WriteMessage. We avoid assuming that the
        /// values are using the embedded well-known types, in order to allow for dynamic messages
        /// in the future.
        /// </summary>
        private void WriteWellKnownTypeValue(
            TextWriter tagWriter,
            TextWriter fieldWriter,
            MessageDescriptor descriptor,
            object value,
            HashSet<string> tags,
            Span<char> path
        )
        {
            // TODO what should happen here?
            //string name = settings.PreserveProtoFieldNames
            //    ? descriptor.Name
            //    : descriptor.json;

            var name = descriptor.Name;
            var writer = ChooseWriter(tagWriter, fieldWriter, name, tags);

            // Currently, we can never actually get here, because null values are always handled by the
            // caller. But if we *could*, this would do the right thing.
            //if (value == null)
            //{
            //    WriteNull(writer);
            //    return;
            //}
            // For wrapper types, the value will either be the (possibly boxed) "native" value,
            // or the message itself if we're formatting it at the top level (e.g. just calling ToString
            // on the object itself). If it's the message form, we can extract the value first, which
            // *will* be the (possibly boxed) native value, and then proceed, writing it as if we were
            // definitely in a field. (We never need to wrap it in an extra string... WriteValue will do
            // the right thing.)
            if (descriptor.IsWrapperType)
            {
                if (value is IMessage message)
                {
                    value = message
                        .Descriptor.Fields[WrappersReflection.WrapperValueFieldNumber]
                        .Accessor.GetValue(message);
                }

                WriteValue(writer, value);
                return;
            }
            if (descriptor.FullName == Timestamp.Descriptor.FullName)
            {
                WriteTimestamp(writer, (IMessage)value);
                return;
            }
            //if (descriptor.FullName == Duration.Descriptor.FullName)
            //{
            //    WriteDuration(writer, (IMessage)value);
            //    return;
            //}
            //if (descriptor.FullName == FieldMask.Descriptor.FullName)
            //{
            //    WriteFieldMask(writer, (IMessage)value);
            //    return;
            //}
            //if (descriptor.FullName == Struct.Descriptor.FullName)
            //{
            //    WriteStruct(writer, (IMessage)value, indentationLevel);
            //    return;
            //}
            //if (descriptor.FullName == ListValue.Descriptor.FullName)
            //{
            //    var fieldAccessor = descriptor.Fields[ListValue.ValuesFieldNumber].Accessor;
            //    WriteList(writer, (IList)fieldAccessor.GetValue((IMessage)value), indentationLevel);
            //    return;
            //}
            //if (descriptor.FullName == Value.Descriptor.FullName)
            //{
            //    WriteStructFieldValue(writer, (IMessage)value, indentationLevel);
            //    return;
            //}
            //if (descriptor.FullName == Any.Descriptor.FullName)
            //{
            //    WriteAny(tagWriter, fieldWriter, (IMessage)value, tags, path);
            //    return;
            //}
            WriteMessage(tagWriter, fieldWriter, (IMessage)value, tags, path);
        }

        private void WriteTimestamp(TextWriter writer, IMessage value)
        {
            // TODO: In the common case where this *is* using the built-in Timestamp type, we could
            // avoid all the reflection at this point, by casting to Timestamp. In the interests of
            // avoiding subtle bugs, don't do that until we've implemented DynamicMessage so that we can
            // prove it still works in that case.
            int nanos = (int)
                value.Descriptor.Fields[Timestamp.NanosFieldNumber].Accessor.GetValue(value);
            long seconds = (long)
                value.Descriptor.Fields[Timestamp.SecondsFieldNumber].Accessor.GetValue(value);
            writer.Write(Timestamp.ToJson(seconds, nanos, DiagnosticOnly));
        }

        //private void WriteDuration(TextWriter tagWriter, TextWriter fieldWriter, IMessage value)
        //{
        //    // TODO: Same as for WriteTimestamp
        //    int nanos = (int)
        //        value.Descriptor.Fields[Duration.NanosFieldNumber].Accessor.GetValue(value);
        //    long seconds = (long)
        //        value.Descriptor.Fields[Duration.SecondsFieldNumber].Accessor.GetValue(value);
        //    writer.Write(Duration.ToJson(seconds, nanos, DiagnosticOnly));
        //}

        //private void WriteFieldMask(TextWriter tagWriter, TextWriter fieldWriter, IMessage value)
        //{
        //    var paths =
        //        (IList<string>)
        //            value.Descriptor.Fields[FieldMask.PathsFieldNumber].Accessor.GetValue(value);
        //    writer.Write(FieldMask.ToJson(paths, DiagnosticOnly));
        //}

        //private void WriteAny(
        //    TextWriter tagWriter,
        //    TextWriter fieldWriter,
        //    IMessage value,
        //    HashSet<string> tags,
        //    Span<char> tags
        //)
        //{
        //    //if (DiagnosticOnly)
        //    //{
        //    //    WriteDiagnosticOnlyAny(writer, value);
        //    //    return;
        //    //}

        //    string typeUrl = (string)
        //        value.Descriptor.Fields[Any.TypeUrlFieldNumber].Accessor.GetValue(value);
        //    ByteString data = (ByteString)
        //        value.Descriptor.Fields[Any.ValueFieldNumber].Accessor.GetValue(value);
        //    string typeName = Any.GetTypeName(typeUrl);
        //    MessageDescriptor descriptor = settings.TypeRegistry.Find(typeName);
        //    if (descriptor == null)
        //    {
        //        throw new InvalidOperationException(
        //            $"Type registry has no descriptor for type name '{typeName}'"
        //        );
        //    }
        //    IMessage message = descriptor.Parser.ParseFrom(data);
        //    WriteBracketOpen(writer, ObjectOpenBracket);
        //    WriteString(writer, AnyTypeUrlField);
        //    writer.Write(NameValueSeparator);
        //    WriteString(writer, typeUrl);

        //    if (descriptor.IsWellKnownType)
        //    {
        //        writer.Write(ValueSeparator);
        //        WriteString(writer, AnyWellKnownTypeValueField);
        //        writer.Write(NameValueSeparator);
        //        WriteWellKnownTypeValue(writer, descriptor, message);
        //    }
        //    else
        //    {
        //        WriteMessageFields(writer, message, true, indentationLevel);
        //    }
        //    WriteBracketClose(writer, ObjectCloseBracket, true, indentationLevel);
        //}

        //private void WriteDiagnosticOnlyAny(
        //    TextWriter tagWriter,
        //    TextWriter fieldWriter,
        //    IMessage value
        //)
        //{
        //    string typeUrl = (string)
        //        value.Descriptor.Fields[Any.TypeUrlFieldNumber].Accessor.GetValue(value);
        //    ByteString data = (ByteString)
        //        value.Descriptor.Fields[Any.ValueFieldNumber].Accessor.GetValue(value);
        //    writer.Write("{ ");
        //    WriteString(writer, AnyTypeUrlField);
        //    writer.Write(NameValueSeparator);
        //    WriteString(writer, typeUrl);
        //    writer.Write(ValueSeparator);
        //    WriteString(writer, AnyDiagnosticValueField);
        //    writer.Write(NameValueSeparator);
        //    writer.Write('"');
        //    writer.Write(data.ToBase64());
        //    writer.Write('"');
        //    writer.Write(" }");
        //}

        //private void WriteStruct(
        //    TextWriter tagWriter,
        //    TextWriter fieldWriter,
        //    IMessage message,
        //    int indentationLevel
        //)
        //{
        //    WriteBracketOpen(writer, ObjectOpenBracket);
        //    IDictionary fields = (IDictionary)
        //        message.Descriptor.Fields[Struct.FieldsFieldNumber].Accessor.GetValue(message);
        //    bool first = true;
        //    foreach (DictionaryEntry entry in fields)
        //    {
        //        string key = (string)entry.Key;
        //        IMessage value = (IMessage)entry.Value;
        //        if (string.IsNullOrEmpty(key) || value == null)
        //        {
        //            throw new InvalidOperationException(
        //                "Struct fields cannot have an empty key or a null value."
        //            );
        //        }

        //        MaybeWriteValueSeparator(writer, first);
        //        MaybeWriteValueWhitespace(writer, indentationLevel + 1);
        //        WriteString(writer, key);
        //        writer.Write(NameValueSeparator);
        //        WriteStructFieldValue(writer, value, indentationLevel + 1);
        //        first = false;
        //    }
        //    WriteBracketClose(writer, ObjectCloseBracket, !first, indentationLevel);
        //}

        //private void WriteStructFieldValue(
        //    TextWriter tagWriter,
        //    TextWriter fieldWriter,
        //    IMessage message,
        //    int indentationLevel
        //)
        //{
        //    var specifiedField = message
        //        .Descriptor.Oneofs[0]
        //        .Accessor.GetCaseFieldDescriptor(message);
        //    if (specifiedField == null)
        //    {
        //        throw new InvalidOperationException(
        //            "Value message must contain a value for the oneof."
        //        );
        //    }

        //    object value = specifiedField.Accessor.GetValue(message);

        //    switch (specifiedField.FieldNumber)
        //    {
        //        case Value.BoolValueFieldNumber:
        //        case Value.StringValueFieldNumber:
        //        case Value.NumberValueFieldNumber:
        //            WriteValue(writer, value);
        //            return;
        //        case Value.StructValueFieldNumber:
        //        case Value.ListValueFieldNumber:
        //            // Structs and ListValues are nested messages, and already well-known types.
        //            var nestedMessage = (IMessage)specifiedField.Accessor.GetValue(message);
        //            WriteWellKnownTypeValue(
        //                writer,
        //                nestedMessage.Descriptor,
        //                nestedMessage,
        //                indentationLevel
        //            );
        //            return;
        //        case Value.NullValueFieldNumber:
        //            WriteNull(writer);
        //            return;
        //        default:
        //            throw new InvalidOperationException(
        //                "Unexpected case in struct field: " + specifiedField.FieldNumber
        //            );
        //    }
        //}

        //internal void WriteList(
        //    TextWriter tagWriter,
        //    TextWriter fieldWriter,
        //    IList list,
        //    string measurement,
        //    HashSet<string> tags
        //)
        //{
        //    WriteBracketOpen(writer, ListBracketOpen);

        //    bool first = true;
        //    foreach (var value in list)
        //    {
        //        MaybeWriteValueSeparator(writer, first);
        //        MaybeWriteValueWhitespace(writer, meas);
        //        WriteValue(writer, value, indentationLevel + 1);
        //        first = false;
        //    }

        //    WriteBracketClose(writer, ListBracketClose, !first, indentationLevel);
        //}

        //internal void WriteDictionary(
        //    TextWriter tagWriter,
        //    TextWriter fieldWriter,
        //    IDictionary dictionary,
        //    string measurement,
        //    HashSet<string> tags
        //)
        //{
        //    WriteBracketOpen(writer, ObjectOpenBracket);

        //    bool first = true;
        //    // This will box each pair. Could use IDictionaryEnumerator, but that's ugly in terms of
        //    // disposal.
        //    foreach (DictionaryEntry pair in dictionary)
        //    {
        //        string keyText;
        //        if (pair.Key is string s)
        //        {
        //            keyText = s;
        //        }
        //        else if (pair.Key is bool b)
        //        {
        //            keyText = b ? "true" : "false";
        //        }
        //        else if (
        //            pair.Key is int
        //            || pair.Key is uint
        //            || pair.Key is long
        //            || pair.Key is ulong
        //        )
        //        {
        //            keyText = ((IFormattable)pair.Key).ToString("d", CultureInfo.InvariantCulture);
        //        }
        //        else
        //        {
        //            if (pair.Key == null)
        //            {
        //                throw new ArgumentException("Dictionary has entry with null key");
        //            }
        //            throw new ArgumentException(
        //                "Unhandled dictionary key type: " + pair.Key.GetType()
        //            );
        //        }

        //        MaybeWriteValueSeparator(writer, first);
        //        MaybeWriteValueWhitespace(writer, indentationLevel + 1);
        //        WriteString(writer, keyText);
        //        writer.Write(NameValueSeparator);
        //        WriteValue(writer, pair.Value, indentationLevel + 1);
        //        first = false;
        //    }

        //    WriteBracketClose(writer, ObjectCloseBracket, !first, indentationLevel);
        //}

        /// <summary>
        /// Writes a string (including leading and trailing double quotes) to a builder, escaping as
        /// required.
        /// </summary>
        /// <remarks>
        /// Other than surrogate pair handling, this code is mostly taken from
        /// src/google/protobuf/util/internal/json_escaping.cc.
        /// </remarks>
        internal static void WriteString(TextWriter writer, ReadOnlySpan<char> text)
        {
            for (int i = 0; i < text.Length; i++)
            {
                char c = text[i];
                if (c < 0xa0)
                {
                    writer.Write(CommonRepresentations[c]);
                    continue;
                }
                if (char.IsHighSurrogate(c))
                {
                    // Encountered first part of a surrogate pair.
                    // Check that we have the whole pair, and encode both parts as hex.
                    i++;
                    if (i == text.Length || !char.IsLowSurrogate(text[i]))
                    {
                        throw new ArgumentException(
                            "String contains low surrogate not followed by high surrogate"
                        );
                    }
                    HexEncodeUtf16CodeUnit(writer, c);
                    HexEncodeUtf16CodeUnit(writer, text[i]);
                    continue;
                }
                else if (char.IsLowSurrogate(c))
                {
                    throw new ArgumentException(
                        "String contains high surrogate not preceded by low surrogate"
                    );
                }
                switch ((uint)c)
                {
                    // These are not required by json spec
                    // but used to prevent security bugs in javascript.
                    case 0xfeff: // Zero width no-break space
                    case 0xfff9: // Interlinear annotation anchor
                    case 0xfffa: // Interlinear annotation separator
                    case 0xfffb: // Interlinear annotation terminator

                    case 0x00ad: // Soft-hyphen
                    case 0x06dd: // Arabic end of ayah
                    case 0x070f: // Syriac abbreviation mark
                    case 0x17b4: // Khmer vowel inherent Aq
                    case 0x17b5: // Khmer vowel inherent Aa
                        HexEncodeUtf16CodeUnit(writer, c);
                        break;

                    default:
                        if (
                            (c >= 0x0600 && c <= 0x0603)
                            || // Arabic signs
                            (c >= 0x200b && c <= 0x200f)
                            || // Zero width etc.
                            (c >= 0x2028 && c <= 0x202e)
                            || // Separators etc.
                            (c >= 0x2060 && c <= 0x2064)
                            || // Invisible etc.
                            (c >= 0x206a && c <= 0x206f)
                        )
                        {
                            HexEncodeUtf16CodeUnit(writer, c);
                        }
                        else
                        {
                            // No handling of surrogates here - that's done earlier
                            writer.Write(c);
                        }
                        break;
                }
            }
        }

        private const string Hex = "0123456789abcdef";

        private static void HexEncodeUtf16CodeUnit(TextWriter writer, char c)
        {
            writer.Write("\\u");
            writer.Write(Hex[(c >> 12) & 0xf]);
            writer.Write(Hex[(c >> 8) & 0xf]);
            writer.Write(Hex[(c >> 4) & 0xf]);
            writer.Write(Hex[(c >> 0) & 0xf]);
        }

        private string WriteBracketOpen(string path, IMessage message)
        {
            return $"{path}.{message.Descriptor.Name}";
        }

        private void WriteBracketClose(
            TextWriter writer,
            char closeChar,
            bool hasFields,
            int indentationLevel
        )
        {
            if (hasFields)
            {
                if (settings.Indentation != null)
                {
                    writer.WriteLine();
                    WriteIndentation(writer, indentationLevel);
                }
                else
                {
                    writer.Write(" ");
                }
            }

            writer.Write(closeChar);
        }

        private void MaybeWriteValueWhitespace(TextWriter writer, int indentationLevel)
        {
            if (settings.Indentation != null)
            {
                writer.WriteLine();
                WriteIndentation(writer, indentationLevel);
            }
        }

        private void WriteIndentation(TextWriter writer, int indentationLevel)
        {
            for (int i = 0; i < indentationLevel; i++)
            {
                writer.Write(settings.Indentation);
            }
        }

        /// <summary>
        /// Settings controlling JSON formatting.
        /// </summary>
        public sealed class Settings
        {
            /// <summary>
            /// Default settings, as used by <see cref="InfluxDbLineProtocolFormatter.Default"/>
            /// </summary>
            public static Settings Default { get; }

            // Workaround for the Mono compiler complaining about XML comments not being on
            // valid language elements.
            static Settings()
            {
                Default = new Settings(false);
            }

            /// <summary>
            /// Whether fields which would otherwise not be included in the formatted data
            /// should be formatted even when the value is not present, or has the default value.
            /// This option only affects fields which don't support "presence" (e.g.
            /// singular non-optional proto3 primitive fields).
            /// </summary>
            public bool FormatDefaultValues { get; }

            /// <summary>
            /// The type registry used to format <see cref="Any"/> messages.
            /// </summary>
            public TypeRegistry TypeRegistry { get; }

            /// <summary>
            /// Whether to format enums as ints. Defaults to false.
            /// </summary>
            public bool FormatEnumsAsIntegers { get; }

            /// <summary>
            /// Whether to use the original proto field names as defined in the .proto file. Defaults to
            /// false.
            /// </summary>
            public bool PreserveProtoFieldNames { get; }

            /// <summary>
            /// Indentation string, used for formatting. Setting null disables indentation.
            /// </summary>
            public string Indentation { get; }

            /// <summary>
            /// Creates a new <see cref="Settings"/> object with the specified formatting of default
            /// values and an empty type registry.
            /// </summary>
            /// <param name="formatDefaultValues"><c>true</c> if default values (0, empty strings etc)
            /// should be formatted; <c>false</c> otherwise.</param>
            public Settings(bool formatDefaultValues)
                : this(formatDefaultValues, TypeRegistry.Empty) { }

            /// <summary>
            /// Creates a new <see cref="Settings"/> object with the specified formatting of default
            /// values and type registry.
            /// </summary>
            /// <param name="formatDefaultValues"><c>true</c> if default values (0, empty strings etc)
            /// should be formatted; <c>false</c> otherwise.</param> <param name="typeRegistry">The <see
            /// cref="TypeRegistry"/> to use when formatting <see cref="Any"/> messages.</param>
            public Settings(bool formatDefaultValues, TypeRegistry typeRegistry)
                : this(formatDefaultValues, typeRegistry, false, false) { }

            /// <summary>
            /// Creates a new <see cref="Settings"/> object with the specified parameters.
            /// </summary>
            /// <param name="formatDefaultValues"><c>true</c> if default values (0, empty strings etc)
            /// should be formatted; <c>false</c> otherwise.</param> <param name="typeRegistry">The <see
            /// cref="TypeRegistry"/> to use when formatting <see cref="Any"/> messages.
            /// TypeRegistry.Empty will be used if it is null.</param> <param
            /// name="formatEnumsAsIntegers"><c>true</c> to format the enums as integers; <c>false</c> to
            /// format enums as enum names.</param> <param name="preserveProtoFieldNames"><c>true</c> to
            /// preserve proto field names; <c>false</c> to convert them to lowerCamelCase.</param> <param
            /// name="indentation">The indentation string to use for multi-line formatting. <c>null</c> to
            /// disable multi-line format.</param>
            private Settings(
                bool formatDefaultValues,
                TypeRegistry typeRegistry,
                bool formatEnumsAsIntegers,
                bool preserveProtoFieldNames,
                string indentation = null
            )
            {
                FormatDefaultValues = formatDefaultValues;
                TypeRegistry = typeRegistry ?? TypeRegistry.Empty;
                FormatEnumsAsIntegers = formatEnumsAsIntegers;
                PreserveProtoFieldNames = preserveProtoFieldNames;
                Indentation = indentation;
            }

            /// <summary>
            /// Creates a new <see cref="Settings"/> object with the specified formatting of default
            /// values and the current settings.
            /// </summary>
            /// <param name="formatDefaultValues"><c>true</c> if default values (0, empty strings etc)
            /// should be formatted; <c>false</c> otherwise.</param>
            public Settings WithFormatDefaultValues(bool formatDefaultValues) =>
                new Settings(
                    formatDefaultValues,
                    TypeRegistry,
                    FormatEnumsAsIntegers,
                    PreserveProtoFieldNames,
                    Indentation
                );

            /// <summary>
            /// Creates a new <see cref="Settings"/> object with the specified type registry and the
            /// current settings.
            /// </summary>
            /// <param name="typeRegistry">The <see cref="TypeRegistry"/> to use when formatting <see
            /// cref="Any"/> messages.</param>
            public Settings WithTypeRegistry(TypeRegistry typeRegistry) =>
                new Settings(
                    FormatDefaultValues,
                    typeRegistry,
                    FormatEnumsAsIntegers,
                    PreserveProtoFieldNames,
                    Indentation
                );

            /// <summary>
            /// Creates a new <see cref="Settings"/> object with the specified enums formatting option and
            /// the current settings.
            /// </summary>
            /// <param name="formatEnumsAsIntegers"><c>true</c> to format the enums as integers;
            /// <c>false</c> to format enums as enum names.</param>
            public Settings WithFormatEnumsAsIntegers(bool formatEnumsAsIntegers) =>
                new Settings(
                    FormatDefaultValues,
                    TypeRegistry,
                    formatEnumsAsIntegers,
                    PreserveProtoFieldNames,
                    Indentation
                );

            /// <summary>
            /// Creates a new <see cref="Settings"/> object with the specified field name formatting
            /// option and the current settings.
            /// </summary>
            /// <param name="preserveProtoFieldNames"><c>true</c> to preserve proto field names;
            /// <c>false</c> to convert them to lowerCamelCase.</param>
            public Settings WithPreserveProtoFieldNames(bool preserveProtoFieldNames) =>
                new Settings(
                    FormatDefaultValues,
                    TypeRegistry,
                    FormatEnumsAsIntegers,
                    preserveProtoFieldNames,
                    Indentation
                );

            /// <summary>
            /// Creates a new <see cref="Settings"/> object with the specified indentation and the current
            /// settings.
            /// </summary>
            /// <param name="indentation">The string to output for each level of indentation (nesting).
            /// The default is two spaces per level. Use null to disable indentation entirely.</param>
            /// <remarks>A non-null value for <see cref="Indentation"/> will insert additional line-breaks
            /// to the JSON output. Each line will contain either a single value, or braces. The default
            /// line-break is determined by <see cref="Environment.NewLine"/>, which is <c>"\n"</c> on
            /// Unix platforms, and <c>"\r\n"</c> on Windows. If <see cref="InfluxDbLineProtocolFormatter"/> seems to
            /// produce empty lines, you need to pass a <see cref="TextWriter"/> that uses a <c>"\n"</c>
            /// newline. See <see cref="InfluxDbLineProtocolFormatter.Format(Google.Protobuf.IMessage, TextWriter)"/>.
            /// </remarks>
            public Settings WithIndentation(string indentation = "  ") =>
                new Settings(
                    FormatDefaultValues,
                    TypeRegistry,
                    FormatEnumsAsIntegers,
                    PreserveProtoFieldNames,
                    indentation
                );
        }

        // Effectively a cache of mapping from enum values to the original name as specified in the
        // proto file, fetched by reflection. The need for this is unfortunate, as is its unbounded
        // size, but realistically it shouldn't cause issues.
        private static class OriginalEnumValueHelper
        {
            private static readonly ConcurrentDictionary<
                System.Type,
                Dictionary<object, string>
            > dictionaries = new ConcurrentDictionary<System.Type, Dictionary<object, string>>();

            [UnconditionalSuppressMessage(
                "Trimming",
                "IL2072",
                Justification = "The field for the value must still be present. It will be returned by reflection, will be in this collection, and its name can be resolved."
            )]
            [UnconditionalSuppressMessage(
                "Trimming",
                "IL2067",
                Justification = "The field for the value must still be present. It will be returned by reflection, will be in this collection, and its name can be resolved."
            )]
            internal static string GetOriginalName(object value)
            {
                // Warnings are suppressed on this method. However, this code has been tested in an AOT app
                // and verified that it works. Issue
                // https://github.com/protocolbuffers/protobuf/issues/14788 discusses changes to guarantee
                // that enum fields are never trimmed.
                Dictionary<object, string> nameMapping = dictionaries.GetOrAdd(
                    value.GetType(),
                    static t => GetNameMapping(t)
                );

                // If this returns false, originalName will be null, which is what we want.
                nameMapping.TryGetValue(value, out string originalName);
                return originalName;
            }

            private static Dictionary<object, string> GetNameMapping(
                [DynamicallyAccessedMembers(
                    DynamicallyAccessedMemberTypes.PublicFields
                        | DynamicallyAccessedMemberTypes.NonPublicFields
                )]
                    System.Type enumType
            )
            {
                return enumType
                    .GetTypeInfo()
                    .DeclaredFields.Where(f => f.IsStatic)
                    .Where(f =>
                        f.GetCustomAttributes<OriginalNameAttribute>()
                            .FirstOrDefault()
                            ?.PreferredAlias ?? true
                    )
                    .ToDictionary(
                        f => f.GetValue(null),
                        f =>
                            f.GetCustomAttributes<OriginalNameAttribute>()
                                .FirstOrDefault()
                                // If the attribute hasn't been applied, fall back to the name of the field.
                                ?.Name ?? f.Name
                    );
            }
        }
    }

    public class TimestampOutOfRangeException : Exception
    {
        public TimestampOutOfRangeException(string message)
            : base(message) { }
    }
}
