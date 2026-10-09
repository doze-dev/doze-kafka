package schemaregistry

// Protobuf compatibility, after Confluent's protobuf SchemaDiff.
//
// Like the JSON Schema check, this is a comparison of declarations by name, not a
// proof about messages on the wire. Messages are matched by name and fields by
// number. A field may change its name, its number's position in the file, or be added
// or removed; it may not change its type to another that is not wire-compatible, nor
// the message type it points at, nor (in proto2) between optional, required and
// repeated. Messages and packages are compared by name: removing a message, or
// changing the package, breaks readers. Enums and services are not examined, as in
// Confluent.

import (
	"fmt"
	"sort"
	"strings"

	"google.golang.org/protobuf/reflect/protoreflect"
)

// protoGroup is the set of scalar types that are wire-compatible with one another.
func protoGroup(k protoreflect.Kind) string {
	switch k {
	case protoreflect.Int32Kind, protoreflect.Int64Kind, protoreflect.Uint32Kind, protoreflect.Uint64Kind,
		protoreflect.BoolKind, protoreflect.EnumKind:
		return "varint"
	case protoreflect.Sint32Kind, protoreflect.Sint64Kind:
		return "zigzag"
	case protoreflect.Fixed32Kind, protoreflect.Sfixed32Kind:
		return "fixed32"
	case protoreflect.Fixed64Kind, protoreflect.Sfixed64Kind:
		return "fixed64"
	case protoreflect.StringKind, protoreflect.BytesKind:
		return "bytes"
	case protoreflect.FloatKind:
		return "float"
	case protoreflect.DoubleKind:
		return "double"
	case protoreflect.MessageKind, protoreflect.GroupKind:
		return "message"
	}
	return k.String()
}

type protoChange struct {
	typ, path, text string
}

var protoCompatible = map[string]bool{
	"MESSAGE_ADDED": true, "FIELD_ADDED": true, "FIELD_REMOVED": true, "ONEOF_FIELD_ADDED": true,
}

type protoDiff struct {
	changes []protoChange
	oldFile protoreflect.FileDescriptor
	newFile protoreflect.FileDescriptor
}

func (d *protoDiff) add(typ, path, text string) {
	if !protoCompatible[typ] {
		d.changes = append(d.changes, protoChange{typ, path, text})
	}
}

// protoCanRead: a reader of `reader` reads what a writer of `writer` wrote when
// going from writer to reader changes nothing incompatibly.
func protoCanRead(reader, writer *protoSchema) []string {
	d := &protoDiff{oldFile: writer.file, newFile: reader.file}
	d.compareFiles(writer.file, reader.file)
	var out []string
	for _, c := range d.changes {
		out = append(out, fmt.Sprintf("{errorType:\"%s\", description:\"%s\"}", c.typ, c.text))
	}
	return out
}

func (d *protoDiff) compareFiles(o, u protoreflect.FileDescriptor) {
	if o.Package() != u.Package() {
		d.add("PACKAGE_CHANGED", "#", "The package name in the new schema does not match its name in the old schema")
	}
	om, um := messagesOf(o), messagesOf(u)
	names := sortedKeys(om)
	for _, name := range names {
		if _, ok := um[name]; !ok {
			d.add("MESSAGE_REMOVED", "#/"+name, fmt.Sprintf("The message '%s' in the old schema is missing from the new schema", name))
		}
	}
	for _, name := range sortedKeys(um) {
		if _, ok := om[name]; !ok {
			d.add("MESSAGE_ADDED", "#/"+name, "")
		}
	}
	for _, name := range names {
		if nm, ok := um[name]; ok {
			d.compareMessages(name, om[name], nm)
		}
	}
}

// messagesOf lists every message in a file, nested ones included, by name relative to
// the package. Map entries are not messages a user wrote.
func messagesOf(f protoreflect.FileDescriptor) map[string]protoreflect.MessageDescriptor {
	out := map[string]protoreflect.MessageDescriptor{}
	prefix := string(f.Package())
	var walk func(ms protoreflect.MessageDescriptors)
	walk = func(ms protoreflect.MessageDescriptors) {
		for i := 0; i < ms.Len(); i++ {
			m := ms.Get(i)
			if m.IsMapEntry() {
				continue
			}
			name := string(m.FullName())
			if prefix != "" {
				name = strings.TrimPrefix(name, prefix+".")
			}
			out[name] = m
			walk(m.Messages())
		}
	}
	walk(f.Messages())
	return out
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func fieldsByNumber(m protoreflect.MessageDescriptor) map[protoreflect.FieldNumber]protoreflect.FieldDescriptor {
	out := map[protoreflect.FieldNumber]protoreflect.FieldDescriptor{}
	for i := 0; i < m.Fields().Len(); i++ {
		f := m.Fields().Get(i)
		out[f.Number()] = f
	}
	return out
}

// label is a field's explicit label, or "" where the syntax leaves it out (a plain
// proto3 field).
func label(f protoreflect.FieldDescriptor) string {
	switch {
	case f.Cardinality() == protoreflect.Repeated:
		return "repeated"
	case f.Cardinality() == protoreflect.Required:
		return "required"
	case f.ContainingOneof() != nil && !f.ContainingOneof().IsSynthetic():
		return ""
	case f.Syntax() == protoreflect.Proto2 || f.HasOptionalKeyword():
		return "optional"
	}
	return ""
}

func (d *protoDiff) compareMessages(name string, o, u protoreflect.MessageDescriptor) {
	of, uf := fieldsByNumber(o), fieldsByNumber(u)
	nums := make([]int, 0, len(of))
	for n := range of {
		nums = append(nums, int(n))
	}
	sort.Ints(nums)
	for _, n := range nums {
		num := protoreflect.FieldNumber(n)
		path := fmt.Sprintf("#/%s/%d", name, n)
		of1 := of[num]
		uf1, ok := uf[num]
		if !ok {
			if label(of1) == "required" {
				d.add("REQUIRED_FIELD_REMOVED", path, "A required field in the old schema is missing from the new schema")
			} else {
				d.add("FIELD_REMOVED", path, "")
			}
			continue
		}
		d.compareFields(path, of1, uf1)
	}
	unums := make([]int, 0, len(uf))
	for n := range uf {
		if _, ok := of[n]; !ok {
			unums = append(unums, int(n))
		}
	}
	sort.Ints(unums)
	for _, n := range unums {
		f := uf[protoreflect.FieldNumber(n)]
		path := fmt.Sprintf("#/%s/%d", name, n)
		if label(f) == "required" {
			d.add("REQUIRED_FIELD_ADDED", path, "A required field was added in the new schema")
		} else {
			d.add("FIELD_ADDED", path, "")
		}
	}

	// A field that was an ordinary field and becomes a member of a oneof: allowed into a
	// new oneof one at a time, never into one that already existed.
	inOneof := func(f protoreflect.FieldDescriptor) bool {
		return f.ContainingOneof() != nil && !f.ContainingOneof().IsSynthetic()
	}
	oldOneofs := map[string]bool{}
	for i := 0; i < o.Oneofs().Len(); i++ {
		if one := o.Oneofs().Get(i); !one.IsSynthetic() {
			oldOneofs[string(one.Name())] = true
		}
	}
	for i := 0; i < u.Oneofs().Len(); i++ {
		one := u.Oneofs().Get(i)
		if one.IsSynthetic() {
			continue
		}
		moved := 0
		for j := 0; j < one.Fields().Len(); j++ {
			nf := one.Fields().Get(j)
			if of1, ok := of[nf.Number()]; ok && !inOneof(of1) {
				moved++
			}
		}
		switch {
		case moved == 0:
		case oldOneofs[string(one.Name())]:
			d.add("FIELD_MOVED_TO_EXISTING_ONEOF", fmt.Sprintf("#/%s/%s", name, one.Name()), "A field was moved into a oneof that already existed in the old schema")
		case moved > 1:
			d.add("MULTIPLE_FIELDS_MOVED_TO_ONEOF", fmt.Sprintf("#/%s/%s", name, one.Name()), "Several existing fields were moved into one new oneof")
		}
	}

	// A oneof that is in both loses a member only by breaking readers.
	ooneofs := map[string]protoreflect.OneofDescriptor{}
	for i := 0; i < o.Oneofs().Len(); i++ {
		if one := o.Oneofs().Get(i); !one.IsSynthetic() {
			ooneofs[string(one.Name())] = one
		}
	}
	for i := 0; i < u.Oneofs().Len(); i++ {
		un := u.Oneofs().Get(i)
		if un.IsSynthetic() {
			continue
		}
		on, ok := ooneofs[string(un.Name())]
		if !ok {
			continue
		}
		inNew := map[protoreflect.FieldNumber]bool{}
		for j := 0; j < un.Fields().Len(); j++ {
			inNew[un.Fields().Get(j).Number()] = true
		}
		for j := 0; j < on.Fields().Len(); j++ {
			f := on.Fields().Get(j)
			if !inNew[f.Number()] {
				d.add("ONEOF_FIELD_REMOVED", fmt.Sprintf("#/%s/%d", name, f.Number()), "A field of a oneof in the old schema is missing from the same oneof in the new schema")
			}
		}
	}
}

func (d *protoDiff) typeName(f protoreflect.FieldDescriptor, file protoreflect.FileDescriptor) string {
	m := f.Message()
	if m == nil {
		return ""
	}
	name := string(m.FullName())
	if p := string(file.Package()); p != "" && m.ParentFile() == file {
		name = strings.TrimPrefix(name, p+".")
	}
	return name
}

func (d *protoDiff) compareFields(path string, o, u protoreflect.FieldDescriptor) {
	// Labels are compared where the syntax makes them explicit (a plain proto3 field has
	// none and may become repeated). A field of a length-delimited type reads the same
	// optional or repeated; a number does not (repeated numbers are packed).
	ol, ul := label(o), label(u)
	lengthDelimited := func(f protoreflect.FieldDescriptor) bool {
		switch f.Kind() {
		case protoreflect.StringKind, protoreflect.BytesKind, protoreflect.MessageKind, protoreflect.GroupKind:
			return true
		}
		return false
	}
	if ol != "" && ul != "" && ol != ul && !(ol != "required" && ul != "required" && lengthDelimited(o) && lengthDelimited(u)) {
		d.add("FIELD_LABEL_CHANGED", path, "The label of a field in the new schema does not match its label in the old schema")
	}
	switch {
	case o.IsMap() != u.IsMap():
		d.add("FIELD_KIND_CHANGED", path, "The kind of a field at path '"+path+"' in the new schema does not match its kind in the old schema")
	case o.IsMap():
		d.compareMapParts(path+"/key", o.MapKey(), u.MapKey())
		d.compareMapParts(path+"/value", o.MapValue(), u.MapValue())
	default:
		d.compareKinds(path, o, u)
	}
}

func (d *protoDiff) compareMapParts(path string, o, u protoreflect.FieldDescriptor) {
	d.compareKinds(path, o, u)
}

func (d *protoDiff) compareKinds(path string, o, u protoreflect.FieldDescriptor) {
	og, ug := protoGroup(o.Kind()), protoGroup(u.Kind())
	switch {
	case og == "message" || ug == "message":
		if og != ug {
			d.add("FIELD_KIND_CHANGED", path, fmt.Sprintf("The kind of a field at path '%s' in the new schema does not match its kind in the old schema", path))
			return
		}
		if d.typeName(o, d.oldFile) != d.typeName(u, d.newFile) {
			d.add("FIELD_NAMED_TYPE_CHANGED", path, fmt.Sprintf("The type of a MESSAGE field at path '%s' in the new schema does not match its type in the old schema", path))
		}
	case og != ug:
		d.add("FIELD_SCALAR_KIND_CHANGED", path, fmt.Sprintf("The kind of a SCALAR field at path '%s' in the new schema does not match its kind in the old schema", path))
	}
}
