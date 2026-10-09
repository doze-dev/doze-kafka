package schemaregistry

import "strings"

func pmsg(body ...string) string {
	return `syntax = "proto3";` + "\n" + strings.Join(body, "\n") + "\n"
}

func protoCorpus() []corpusCase {
	var cs []corpusCase
	add := func(name, old, new string) { cs = append(cs, corpusCase{Name: name, Old: old, New: new}) }
	msg := func(fields ...string) string { return "message M {\n" + strings.Join(fields, "\n") + "\n}" }
	base := pmsg(msg("  int64 id = 1;"))
	add("identical", base, base)
	add("add field", base, pmsg(msg("  int64 id = 1;", "  string name = 2;")))
	add("remove field", pmsg(msg("  int64 id = 1;", "  string name = 2;")), base)
	add("rename field", base, pmsg(msg("  int64 key = 1;")))
	add("renumber field", base, pmsg(msg("  int64 id = 2;")))
	add("swap field numbers", pmsg(msg("  int64 a = 1;", "  string b = 2;")), pmsg(msg("  int64 a = 2;", "  string b = 1;")))
	add("remove and reuse number", pmsg(msg("  int64 a = 1;", "  string b = 2;")), pmsg(msg("  int64 a = 1;", "  int32 c = 2;")))
	add("reserved number", pmsg(msg("  int64 a = 1;", "  string b = 2;")), pmsg(msg("  reserved 2;", "  int64 a = 1;")))
	add("reserved name", pmsg(msg("  int64 a = 1;", "  string b = 2;")), pmsg(msg("  reserved \"b\";", "  int64 a = 1;")))
	add("reorder fields in text", pmsg(msg("  int64 a = 1;", "  string b = 2;")), pmsg(msg("  string b = 2;", "  int64 a = 1;")))

	// Scalar type changes.
	scalars := []string{"int32", "int64", "uint32", "uint64", "sint32", "sint64", "bool", "fixed32", "fixed64", "sfixed32", "sfixed64", "float", "double", "string", "bytes"}
	for _, a := range scalars {
		for _, b := range scalars {
			if a == b {
				continue
			}
			add("scalar "+a+" to "+b, pmsg(msg("  "+a+" f = 1;")), pmsg(msg("  "+b+" f = 1;")))
		}
	}

	// Labels.
	add("singular to repeated", pmsg(msg("  int64 f = 1;")), pmsg(msg("  repeated int64 f = 1;")))
	add("repeated to singular", pmsg(msg("  repeated int64 f = 1;")), pmsg(msg("  int64 f = 1;")))
	add("repeated string to singular", pmsg(msg("  repeated string f = 1;")), pmsg(msg("  string f = 1;")))
	add("singular string to repeated", pmsg(msg("  string f = 1;")), pmsg(msg("  repeated string f = 1;")))
	add("singular to optional", pmsg(msg("  int64 f = 1;")), pmsg(msg("  optional int64 f = 1;")))
	add("optional to singular", pmsg(msg("  optional int64 f = 1;")), pmsg(msg("  int64 f = 1;")))
	add("repeated packed option", pmsg(msg("  repeated int64 f = 1;")), pmsg(msg("  repeated int64 f = 1 [packed = false];")))
	add("deprecated option", pmsg(msg("  int64 f = 1;")), pmsg(msg("  int64 f = 1 [deprecated = true];")))

	// Messages and nesting.
	add("add message", base, pmsg(msg("  int64 id = 1;"), "message N {\n  int32 x = 1;\n}"))
	add("remove message", pmsg(msg("  int64 id = 1;"), "message N {\n  int32 x = 1;\n}"), base)
	add("rename message", base, pmsg("message Other {\n  int64 id = 1;\n}"))
	add("message field type", pmsg("message A {\n  int32 x = 1;\n}", "message B {\n  A a = 1;\n}"), pmsg("message A {\n  int32 x = 1;\n}", "message B {\n  A a = 1;\n}"))
	add("message field retyped", pmsg("message A {\n  int32 x = 1;\n}", "message C {\n  int32 x = 1;\n}", "message B {\n  A a = 1;\n}"), pmsg("message A {\n  int32 x = 1;\n}", "message C {\n  int32 x = 1;\n}", "message B {\n  C a = 1;\n}"))
	add("message field to scalar", pmsg("message A {\n  int32 x = 1;\n}", "message B {\n  A a = 1;\n}"), pmsg("message A {\n  int32 x = 1;\n}", "message B {\n  string a = 1;\n}"))
	add("scalar to message field", pmsg("message A {\n  int32 x = 1;\n}", "message B {\n  string a = 1;\n}"), pmsg("message A {\n  int32 x = 1;\n}", "message B {\n  A a = 1;\n}"))
	add("nested add field", pmsg("message O {\n  message I {\n    int32 x = 1;\n  }\n  I i = 1;\n}"), pmsg("message O {\n  message I {\n    int32 x = 1;\n    string y = 2;\n  }\n  I i = 1;\n}"))
	add("nested remove field", pmsg("message O {\n  message I {\n    int32 x = 1;\n    string y = 2;\n  }\n  I i = 1;\n}"), pmsg("message O {\n  message I {\n    int32 x = 1;\n  }\n  I i = 1;\n}"))
	add("nested message removed", pmsg("message O {\n  message I {\n    int32 x = 1;\n  }\n  I i = 1;\n}"), pmsg("message O {\n  int32 other = 2;\n}"))
	add("nested message added", pmsg("message O {\n  int32 other = 2;\n}"), pmsg("message O {\n  message I {\n    int32 x = 1;\n  }\n  I i = 1;\n  int32 other = 2;\n}"))
	add("nested message moved", pmsg("message O {\n  message I {\n    int32 x = 1;\n  }\n  I i = 1;\n}"), pmsg("message I {\n  int32 x = 1;\n}", "message O {\n  I i = 1;\n}"))
	add("package added", base, "syntax = \"proto3\";\npackage p.q;\n"+msg("  int64 id = 1;")+"\n")
	add("package changed", "syntax = \"proto3\";\npackage p.q;\n"+msg("  int64 id = 1;")+"\n", "syntax = \"proto3\";\npackage p.r;\n"+msg("  int64 id = 1;")+"\n")
	add("package removed", "syntax = \"proto3\";\npackage p.q;\n"+msg("  int64 id = 1;")+"\n", base)

	// Enums.
	enum := func(vals ...string) string { return "enum E {\n" + strings.Join(vals, "\n") + "\n}" }
	withEnum := func(e string) string { return pmsg(e, "message M {\n  E e = 1;\n}") }
	add("enum add value", withEnum(enum("  A = 0;", "  B = 1;")), withEnum(enum("  A = 0;", "  B = 1;", "  C = 2;")))
	add("enum remove value", withEnum(enum("  A = 0;", "  B = 1;", "  C = 2;")), withEnum(enum("  A = 0;", "  B = 1;")))
	add("enum rename value", withEnum(enum("  A = 0;", "  B = 1;")), withEnum(enum("  A = 0;", "  X = 1;")))
	add("enum renumber value", withEnum(enum("  A = 0;", "  B = 1;")), withEnum(enum("  A = 0;", "  B = 2;")))
	add("enum rename", withEnum(enum("  A = 0;")), pmsg("enum F {\n  A = 0;\n}", "message M {\n  F e = 1;\n}"))
	add("enum to int32", withEnum(enum("  A = 0;")), pmsg(enum("  A = 0;"), "message M {\n  int32 e = 1;\n}"))
	add("int32 to enum", pmsg(enum("  A = 0;"), "message M {\n  int32 e = 1;\n}"), withEnum(enum("  A = 0;")))
	add("enum to string", withEnum(enum("  A = 0;")), pmsg(enum("  A = 0;"), "message M {\n  string e = 1;\n}"))
	add("enum allow_alias", withEnum(enum("  A = 0;", "  B = 1;")), withEnum("enum E {\n  option allow_alias = true;\n  A = 0;\n  B = 1;\n  C = 1;\n}"))
	add("nested enum add value", pmsg("message M {\n  enum E {\n    A = 0;\n  }\n  E e = 1;\n}"), pmsg("message M {\n  enum E {\n    A = 0;\n    B = 1;\n  }\n  E e = 1;\n}"))
	add("top-level enum added", base, pmsg(enum("  A = 0;"), msg("  int64 id = 1;")))
	add("top-level enum removed", pmsg(enum("  A = 0;"), msg("  int64 id = 1;")), base)

	// Oneofs.
	add("add oneof", base, pmsg(msg("  int64 id = 1;", "  oneof o {", "    string a = 2;", "    int32 b = 3;", "  }")))
	add("remove oneof", pmsg(msg("  int64 id = 1;", "  oneof o {", "    string a = 2;", "    int32 b = 3;", "  }")), base)
	add("field into oneof", pmsg(msg("  int64 id = 1;", "  string a = 2;")), pmsg(msg("  int64 id = 1;", "  oneof o {", "    string a = 2;", "  }")))
	add("field out of oneof", pmsg(msg("  int64 id = 1;", "  oneof o {", "    string a = 2;", "  }")), pmsg(msg("  int64 id = 1;", "  string a = 2;")))
	add("oneof add member", pmsg(msg("  oneof o {", "    string a = 1;", "  }")), pmsg(msg("  oneof o {", "    string a = 1;", "    int32 b = 2;", "  }")))
	add("oneof remove member", pmsg(msg("  oneof o {", "    string a = 1;", "    int32 b = 2;", "  }")), pmsg(msg("  oneof o {", "    string a = 1;", "  }")))
	add("oneof rename", pmsg(msg("  oneof o {", "    string a = 1;", "  }")), pmsg(msg("  oneof p {", "    string a = 1;", "  }")))
	add("oneof member moved between oneofs", pmsg(msg("  oneof o {", "    string a = 1;", "  }", "  oneof p {", "    int32 b = 2;", "  }")), pmsg(msg("  oneof o {", "    string a = 1;", "    int32 b = 2;", "  }")))

	// Maps.
	add("add map", base, pmsg(msg("  int64 id = 1;", "  map<string, int32> m = 2;")))
	add("remove map", pmsg(msg("  int64 id = 1;", "  map<string, int32> m = 2;")), base)
	add("map value type", pmsg(msg("  map<string, int32> m = 1;")), pmsg(msg("  map<string, int64> m = 1;")))
	add("map key type", pmsg(msg("  map<string, int32> m = 1;")), pmsg(msg("  map<int32, int32> m = 1;")))
	add("map to repeated message", pmsg(msg("  map<string, int32> m = 1;")), pmsg("message Entry {\n  string key = 1;\n  int32 value = 2;\n}", msg("  repeated Entry m = 1;")))

	// proto2.
	p2 := func(fields ...string) string {
		return "syntax = \"proto2\";\nmessage M {\n" + strings.Join(fields, "\n") + "\n}\n"
	}
	add("proto2 add optional", p2("  optional int64 id = 1;"), p2("  optional int64 id = 1;", "  optional string name = 2;"))
	add("proto2 add required", p2("  optional int64 id = 1;"), p2("  optional int64 id = 1;", "  required string name = 2;"))
	add("proto2 remove optional", p2("  optional int64 id = 1;", "  optional string name = 2;"), p2("  optional int64 id = 1;"))
	add("proto2 remove required", p2("  optional int64 id = 1;", "  required string name = 2;"), p2("  optional int64 id = 1;"))
	add("proto2 optional to required", p2("  optional int64 id = 1;"), p2("  required int64 id = 1;"))
	add("proto2 required to optional", p2("  required int64 id = 1;"), p2("  optional int64 id = 1;"))
	add("proto2 optional to repeated", p2("  optional int64 id = 1;"), p2("  repeated int64 id = 1;"))
	add("proto2 default value", p2("  optional int64 id = 1;"), p2("  optional int64 id = 1 [default = 5];"))
	add("proto2 required same", p2("  required int64 id = 1;"), p2("  required int64 id = 1;"))
	add("syntax proto2 to proto3", p2("  optional int64 id = 1;"), pmsg(msg("  int64 id = 1;")))
	add("syntax proto3 to proto2", pmsg(msg("  int64 id = 1;")), p2("  optional int64 id = 1;"))
	add("proto2 extensions range", p2("  optional int64 id = 1;"), p2("  optional int64 id = 1;", "  extensions 100 to 199;"))

	// Options and imports.
	add("java_package option", base, pmsg(`option java_package = "com.x";`, msg("  int64 id = 1;")))
	add("java_outer_classname option", pmsg(`option java_outer_classname = "A";`, msg("  int64 id = 1;")), pmsg(`option java_outer_classname = "B";`, msg("  int64 id = 1;")))
	add("well-known import added", base, pmsg(`import "google/protobuf/timestamp.proto";`, "message M {\n  int64 id = 1;\n  google.protobuf.Timestamp t = 2;\n}"))
	add("well-known field retyped", pmsg(`import "google/protobuf/timestamp.proto";`, "message M {\n  google.protobuf.Timestamp t = 1;\n}"), pmsg(`import "google/protobuf/duration.proto";`, "message M {\n  google.protobuf.Duration t = 1;\n}"))
	add("well-known to scalar", pmsg(`import "google/protobuf/timestamp.proto";`, "message M {\n  google.protobuf.Timestamp t = 1;\n}"), pmsg(msg("  int64 t = 1;")))
	add("wrapper to scalar", pmsg(`import "google/protobuf/wrappers.proto";`, "message M {\n  google.protobuf.Int64Value t = 1;\n}"), pmsg(msg("  int64 t = 1;")))
	add("unused import", base, pmsg(`import "google/protobuf/empty.proto";`, msg("  int64 id = 1;")))

	// Services.
	svc := func(methods ...string) string { return "service S {\n" + strings.Join(methods, "\n") + "\n}" }
	add("service added", base, pmsg(msg("  int64 id = 1;"), svc("  rpc Get(M) returns (M);")))
	add("service method added", pmsg(msg("  int64 id = 1;"), svc("  rpc Get(M) returns (M);")), pmsg(msg("  int64 id = 1;"), svc("  rpc Get(M) returns (M);", "  rpc Put(M) returns (M);")))
	add("service method removed", pmsg(msg("  int64 id = 1;"), svc("  rpc Get(M) returns (M);", "  rpc Put(M) returns (M);")), pmsg(msg("  int64 id = 1;"), svc("  rpc Get(M) returns (M);")))
	add("service removed", pmsg(msg("  int64 id = 1;"), svc("  rpc Get(M) returns (M);")), base)

	// Several things at once.
	add("many compatible changes", pmsg(msg("  int64 id = 1;", "  string a = 2;")), pmsg(msg("  int64 id = 1;", "  string a = 2;", "  string b = 3;", "  repeated int32 c = 4;")))
	add("add and remove", pmsg(msg("  int64 id = 1;", "  string a = 2;")), pmsg(msg("  int64 id = 1;", "  string b = 3;")))
	add("empty message", pmsg("message M {\n}"), pmsg("message M {\n}"))
	add("empty message add field", pmsg("message M {\n}"), base)
	add("multiple messages same", pmsg(msg("  int64 id = 1;"), "message N {\n  int32 x = 1;\n}"), pmsg(msg("  int64 id = 1;"), "message N {\n  int32 x = 1;\n}"))
	add("multiple messages reordered", pmsg(msg("  int64 id = 1;"), "message N {\n  int32 x = 1;\n}"), pmsg("message N {\n  int32 x = 1;\n}", msg("  int64 id = 1;")))
	add("multiple messages one changes", pmsg(msg("  int64 id = 1;"), "message N {\n  int32 x = 1;\n}"), pmsg(msg("  int64 id = 1;"), "message N {\n  string x = 1;\n}"))

	// Not Protobuf.
	add("invalid new", base, "syntax = \"proto3\"; message {")
	add("invalid old", "syntax = \"proto3\"; message {", base)
	return cs
}
