package schemaregistry

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/bufbuild/protocompile"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type protoSchema struct {
	text string
	file protoreflect.FileDescriptor
}

func (p *protoSchema) Type() string { return TypeProtobuf }

func (p *protoSchema) Canonical(bool) string { return p.text }

const protoMainName = "schema.proto"

func parseProtobuf(text string, refs []resolvedRef) (parsedSchema, error) {
	sources := map[string]string{protoMainName: text}
	for _, r := range refs {
		sources[r.Name] = r.Text
	}
	c := protocompile.Compiler{
		Resolver: protocompile.WithStandardImports(&protocompile.SourceResolver{
			Accessor: func(path string) (io.ReadCloser, error) {
				if s, ok := sources[path]; ok {
					return io.NopCloser(strings.NewReader(s)), nil
				}
				return nil, fmt.Errorf("%s: no such file", path)
			},
		}),
	}
	files, err := c.Compile(context.Background(), protoMainName)
	if err != nil {
		return nil, err
	}
	canon, err := protoCanonical(text)
	if err != nil {
		canon = strings.TrimSpace(text)
	}
	return &protoSchema{text: canon, file: files[0]}, nil
}
