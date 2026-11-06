package schemaregistry

import (
	"encoding/base64"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

// R11: a Protobuf schema registered as a base64 FileDescriptorProto that imports another
// file, the way confluent-kafka for Python registers a message whose type uses a type
// from another file: the dependency first, then the message's file with a reference.

func descriptorB64(fd *descriptorpb.FileDescriptorProto) string {
	b, err := proto.Marshal(fd)
	if err != nil {
		panic(err)
	}
	return base64.StdEncoding.EncodeToString(b)
}

var (
	depDescriptor = descriptorB64(&descriptorpb.FileDescriptorProto{
		Name:    proto.String("dep.proto"),
		Package: proto.String("dep"),
		Syntax:  proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{{
			Name: proto.String("Addr"),
			Field: []*descriptorpb.FieldDescriptorProto{{
				Name: proto.String("city"), Number: proto.Int32(1), JsonName: proto.String("city"),
				Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
				Type:  descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
			}},
		}},
	})
	mainDescriptor = descriptorB64(&descriptorpb.FileDescriptorProto{
		Name:       proto.String("main.proto"),
		Package:    proto.String("app"),
		Syntax:     proto.String("proto3"),
		Dependency: []string{"dep.proto"},
		MessageType: []*descriptorpb.DescriptorProto{{
			Name: proto.String("Person"),
			Field: []*descriptorpb.FieldDescriptorProto{{
				Name: proto.String("home"), Number: proto.Int32(1), JsonName: proto.String("home"),
				Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
				Type:     descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(),
				TypeName: proto.String(".dep.Addr"),
			}},
		}},
	})
)

// optsDescriptor is a proto2 field carrying json_name with other options, to learn the
// order Confluent prints them in.
var optsDescriptor = descriptorB64(&descriptorpb.FileDescriptorProto{
	Name:    proto.String("opts.proto"),
	Package: proto.String("o"),
	MessageType: []*descriptorpb.DescriptorProto{{
		Name: proto.String("M"),
		Field: []*descriptorpb.FieldDescriptorProto{
			{
				Name: proto.String("a_b"), Number: proto.Int32(1), JsonName: proto.String("aB"),
				Label:        descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
				Type:         descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
				DefaultValue: proto.String("hi"),
				Options:      &descriptorpb.FieldOptions{Deprecated: proto.Bool(true)},
			},
			{
				Name: proto.String("n"), Number: proto.Int32(2), JsonName: proto.String("custom"),
				Label:   descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum(),
				Type:    descriptorpb.FieldDescriptorProto_TYPE_INT32.Enum(),
				Options: &descriptorpb.FieldOptions{Packed: proto.Bool(true)},
			},
		},
	}},
})

const depProtoText = `syntax = "proto3"; package dep; message Addr { string city = 1; }`

func init() {
	scenarios = append(scenarios, scenario{
		name: "protobuf-descriptor-with-import",
		steps: []scriptStep{
			post("/subjects/pdep/versions", typedBody("PROTOBUF", depProtoText)),
			post("/subjects/pmain/versions", refBody("PROTOBUF", mainDescriptor, Reference{Name: "dep.proto", Subject: "pdep", Version: 1})),
			get("/subjects/pmain/versions/latest"),
			post("/subjects/pmain", refBody("PROTOBUF", mainDescriptor, Reference{Name: "dep.proto", Subject: "pdep", Version: 1})),
			post("/compatibility/subjects/pmain/versions/latest", refBody("PROTOBUF", mainDescriptor, Reference{Name: "dep.proto", Subject: "pdep", Version: 1})),
			post("/subjects/pnoref/versions", typedBody("PROTOBUF", mainDescriptor)),
			post("/subjects/pdepd/versions", typedBody("PROTOBUF", depDescriptor)),
			get("/subjects/pdepd/versions/latest"),
			post("/subjects/pmain2/versions", refBody("PROTOBUF", mainDescriptor, Reference{Name: "dep.proto", Subject: "pdepd", Version: 1})),
			get("/subjects/pmain2/versions/latest"),
			get("/subjects/pdep/versions/1/referencedby"),
			post("/subjects/popts/versions", typedBody("PROTOBUF", optsDescriptor)),
			get("/subjects/popts/versions/latest"),
			post("/subjects/ptext/versions", typedBody("PROTOBUF", `syntax = "proto2"; package t; message M { optional string b = 2 [default = "hi", deprecated = true]; optional int32 c = 3 [json_name = "see"]; repeated int32 d = 4 [packed = true, deprecated = false, json_name = "dee"]; }`)),
			get("/subjects/ptext/versions/latest"),
		},
		cleanup: cleanupSubjects("pmain2", "pmain", "pnoref", "pdepd", "pdep", "popts", "ptext"),
	})
}
