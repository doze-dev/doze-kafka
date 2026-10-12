package schemaregistry

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"

	// The well-known types a descriptor may import, registered so they resolve.
	_ "google.golang.org/protobuf/types/known/anypb"
	_ "google.golang.org/protobuf/types/known/durationpb"
	_ "google.golang.org/protobuf/types/known/emptypb"
	_ "google.golang.org/protobuf/types/known/fieldmaskpb"
	_ "google.golang.org/protobuf/types/known/structpb"
	_ "google.golang.org/protobuf/types/known/timestamppb"
	_ "google.golang.org/protobuf/types/known/wrapperspb"
)

// Some clients do not send a schema as .proto text. confluent-kafka for Python (and
// Go) registers a Protobuf message as the base64 of its serialized FileDescriptorProto, and
// Confluent's registry accepts that: it tries the text first and, failing that, reads the
// string as a descriptor. This is the second half of that, as .proto text, which then goes
// through the same parser and printer as any other schema, so the same message
// registered both ways is one schema.

// parseDescriptor reads s as a base64 FileDescriptorProto, or reports that it is not one.
// The text is Confluent's printing of the descriptor: type names keep their leading dot, a
// map field is a repeated nested *Entry message with map_entry set (not map<k, v>), and a
// proto2 file has no syntax line. It is not compiled (the compiler refuses map_entry written
// out); the descriptor itself is the checked form.
//
//fuzz:parser class=schema-texts FuzzDescriptor
func parseDescriptor(s string) (parsedSchema, bool) {
	raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(s))
	if err != nil || len(raw) == 0 {
		return nil, false
	}
	var fd descriptorpb.FileDescriptorProto
	if err := proto.Unmarshal(raw, &fd); err != nil || fd.GetName() == "" && len(fd.GetMessageType()) == 0 && len(fd.GetEnumType()) == 0 && len(fd.GetService()) == 0 {
		return nil, false
	}
	file, err := protodesc.NewFile(&fd, protoregistry.GlobalFiles)
	if err != nil {
		return nil, false
	}
	text := printDescriptor(&fd)
	canon, err := protoCanonical(text)
	if err != nil {
		canon = strings.TrimSpace(text)
	}
	return &protoSchema{text: canon, file: file}, true
}

func printDescriptor(fd *descriptorpb.FileDescriptorProto) string {
	var b strings.Builder
	proto2 := fd.GetSyntax() == "" || fd.GetSyntax() == "proto2"
	if !proto2 {
		fmt.Fprintf(&b, "syntax = %q;\n", fd.GetSyntax())
	}
	if fd.GetPackage() != "" {
		fmt.Fprintf(&b, "package %s;\n", fd.GetPackage())
	}
	for i, dep := range fd.GetDependency() {
		mod := ""
		for _, pd := range fd.GetPublicDependency() {
			if int(pd) == i {
				mod = "public "
			}
		}
		for _, wd := range fd.GetWeakDependency() {
			if int(wd) == i {
				mod = "weak "
			}
		}
		fmt.Fprintf(&b, "import %s%q;\n", mod, dep)
	}
	if o := fd.GetOptions(); o != nil {
		if o.JavaPackage != nil {
			fmt.Fprintf(&b, "option java_package = %q;\n", o.GetJavaPackage())
		}
		if o.JavaOuterClassname != nil {
			fmt.Fprintf(&b, "option java_outer_classname = %q;\n", o.GetJavaOuterClassname())
		}
		if o.JavaMultipleFiles != nil {
			fmt.Fprintf(&b, "option java_multiple_files = %t;\n", o.GetJavaMultipleFiles())
		}
		if o.GoPackage != nil {
			fmt.Fprintf(&b, "option go_package = %q;\n", o.GetGoPackage())
		}
		if o.Deprecated != nil {
			fmt.Fprintf(&b, "option deprecated = %t;\n", o.GetDeprecated())
		}
		if o.CsharpNamespace != nil {
			fmt.Fprintf(&b, "option csharp_namespace = %q;\n", o.GetCsharpNamespace())
		}
	}
	for _, m := range fd.GetMessageType() {
		writeMessage(&b, m, proto2, 0)
	}
	for _, e := range fd.GetEnumType() {
		writeEnum(&b, e, 0)
	}
	for _, s := range fd.GetService() {
		fmt.Fprintf(&b, "service %s {\n", s.GetName())
		for _, m := range s.GetMethod() {
			in, out := m.GetInputType(), m.GetOutputType()
			cs, ss := "", ""
			if m.GetClientStreaming() {
				cs = "stream "
			}
			if m.GetServerStreaming() {
				ss = "stream "
			}
			fmt.Fprintf(&b, "  rpc %s (%s%s) returns (%s%s);\n", m.GetName(), cs, in, ss, out)
		}
		b.WriteString("}\n")
	}
	return b.String()
}

func indent(n int) string { return strings.Repeat("  ", n) }

var scalarNames = map[descriptorpb.FieldDescriptorProto_Type]string{
	descriptorpb.FieldDescriptorProto_TYPE_DOUBLE: "double", descriptorpb.FieldDescriptorProto_TYPE_FLOAT: "float",
	descriptorpb.FieldDescriptorProto_TYPE_INT64: "int64", descriptorpb.FieldDescriptorProto_TYPE_UINT64: "uint64",
	descriptorpb.FieldDescriptorProto_TYPE_INT32: "int32", descriptorpb.FieldDescriptorProto_TYPE_FIXED64: "fixed64",
	descriptorpb.FieldDescriptorProto_TYPE_FIXED32: "fixed32", descriptorpb.FieldDescriptorProto_TYPE_BOOL: "bool",
	descriptorpb.FieldDescriptorProto_TYPE_STRING: "string", descriptorpb.FieldDescriptorProto_TYPE_BYTES: "bytes",
	descriptorpb.FieldDescriptorProto_TYPE_UINT32: "uint32", descriptorpb.FieldDescriptorProto_TYPE_SFIXED32: "sfixed32",
	descriptorpb.FieldDescriptorProto_TYPE_SFIXED64: "sfixed64", descriptorpb.FieldDescriptorProto_TYPE_SINT32: "sint32",
	descriptorpb.FieldDescriptorProto_TYPE_SINT64: "sint64",
}

func typeName(f *descriptorpb.FieldDescriptorProto) string {
	if n, ok := scalarNames[f.GetType()]; ok {
		return n
	}
	return f.GetTypeName()
}

func writeMessage(b *strings.Builder, m *descriptorpb.DescriptorProto, proto2 bool, depth int) {
	in := indent(depth)
	if isEmptyMessage(m) {
		fmt.Fprintf(b, "%smessage %s {}\n", in, m.GetName())
		return
	}
	fmt.Fprintf(b, "%smessage %s {\n", in, m.GetName())
	if m.GetOptions().GetMapEntry() {
		fmt.Fprintf(b, "%s  option map_entry = true;\n", in)
	}
	oneofFields := map[int32][]*descriptorpb.FieldDescriptorProto{}
	for _, f := range m.GetField() {
		if f.OneofIndex != nil && !f.GetProto3Optional() {
			oneofFields[f.GetOneofIndex()] = append(oneofFields[f.GetOneofIndex()], f)
		}
	}
	writeField := func(f *descriptorpb.FieldDescriptorProto, d int, label bool) {
		fi := indent(d)
		typ := typeName(f)
		prefix := ""
		if label {
			switch {
			case f.GetLabel() == descriptorpb.FieldDescriptorProto_LABEL_REPEATED:
				prefix = "repeated "
			case f.GetProto3Optional() || (proto2 && f.GetLabel() == descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL):
				prefix = "optional "
			case proto2 && f.GetLabel() == descriptorpb.FieldDescriptorProto_LABEL_REQUIRED:
				prefix = "required "
			}
		}
		var opts []string
		if fo := f.GetOptions(); fo != nil {
			if fo.Deprecated != nil {
				opts = append(opts, "deprecated = "+strconv.FormatBool(fo.GetDeprecated()))
			}
			if fo.Packed != nil {
				opts = append(opts, "packed = "+strconv.FormatBool(fo.GetPacked()))
			}
		}
		if f.DefaultValue != nil {
			v := f.GetDefaultValue()
			if f.GetType() == descriptorpb.FieldDescriptorProto_TYPE_STRING {
				v = strconv.Quote(v)
			}
			opts = append(opts, "default = "+v)
		}
		o := ""
		if len(opts) > 0 {
			o = " [" + strings.Join(opts, ", ") + "]"
		}
		fmt.Fprintf(b, "%s%s%s %s = %d%s;\n", fi, prefix, typ, f.GetName(), f.GetNumber(), o)
	}
	for _, r := range m.GetReservedRange() {
		if r.GetEnd()-1 == r.GetStart() {
			fmt.Fprintf(b, "%s  reserved %d;\n", in, r.GetStart())
		} else {
			fmt.Fprintf(b, "%s  reserved %d to %d;\n", in, r.GetStart(), r.GetEnd()-1)
		}
	}
	for _, r := range m.GetReservedName() {
		fmt.Fprintf(b, "%s  reserved %q;\n", in, r)
	}
	for _, f := range m.GetField() {
		if f.OneofIndex != nil && !f.GetProto3Optional() {
			continue
		}
		writeField(f, depth+1, true)
	}
	for i, o := range m.GetOneofDecl() {
		fs := oneofFields[int32(i)]
		if len(fs) == 0 {
			continue // a synthetic oneof behind a proto3 optional.
		}
		fmt.Fprintf(b, "%s  oneof %s {\n", in, o.GetName())
		for _, f := range fs {
			writeField(f, depth+2, false)
		}
		fmt.Fprintf(b, "%s  }\n", in)
	}
	for _, n := range m.GetNestedType() {
		writeMessage(b, n, proto2, depth+1)
	}
	for _, e := range m.GetEnumType() {
		writeEnum(b, e, depth+1)
	}
	fmt.Fprintf(b, "%s}\n", in)
}

func writeEnum(b *strings.Builder, e *descriptorpb.EnumDescriptorProto, depth int) {
	in := indent(depth)
	fmt.Fprintf(b, "%senum %s {\n", in, e.GetName())
	if eo := e.GetOptions(); eo != nil && eo.AllowAlias != nil {
		fmt.Fprintf(b, "%s  option allow_alias = %t;\n", in, eo.GetAllowAlias())
	}
	for _, v := range e.GetValue() {
		fmt.Fprintf(b, "%s  %s = %d;\n", in, v.GetName(), v.GetNumber())
	}
	fmt.Fprintf(b, "%s}\n", in)
}

func isEmptyMessage(m *descriptorpb.DescriptorProto) bool {
	return len(m.GetField()) == 0 && len(m.GetNestedType()) == 0 && len(m.GetEnumType()) == 0 && len(m.GetReservedRange()) == 0 &&
		len(m.GetReservedName()) == 0 && len(m.GetOneofDecl()) == 0 && !m.GetOptions().GetMapEntry()
}
