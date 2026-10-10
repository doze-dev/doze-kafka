package schemaregistry

// Protobuf schemas are stored the way Confluent prints them, so the same schema
// written two ways (comments, spacing, declaration order of messages and enums) is one
// schema with one id, and a client reading the schema back sees the layout Confluent's
// registry would give it. The layout, learned from its output:
//
//   - syntax, package, then a blank line and the imports, a blank line and the file
//     options, a blank line and the types (every message in order, then every enum),
//     then each service after a blank line;
//   - inside a message the parts that exist come as sections separated by a blank
//     line: reserved, options, fields, oneofs, groups, nested types (messages, then
//     enums), extension ranges;
//   - comments are dropped; two-space indent; services print "rpc Name (In) returns (Out);".

import (
	"fmt"
	"io"
	"strings"

	"github.com/bufbuild/protocompile/ast"
	"github.com/bufbuild/protocompile/parser"
	"github.com/bufbuild/protocompile/reporter"
)

func protoCanonical(text string) (string, error) {
	h := reporter.NewHandler(nil)
	file, err := parser.Parse(protoMainName, strings.NewReader(text), h)
	if err != nil {
		return "", err
	}
	p := &protoPrinter{file: file}
	p.printFile()
	return p.b.String(), nil
}

type protoPrinter struct {
	file *ast.FileNode
	b    strings.Builder
}

// raw is the source text of a node with its runs of white space collapsed.
func (p *protoPrinter) raw(n ast.Node) string {
	return strings.Join(strings.Fields(p.file.NodeInfo(n).RawText()), " ")
}

// blank writes the empty line that separates sections. In a nested message it holds
// two spaces: Confluent indents the text of a nested message, and a blank line is
// padded to the indent rather than left empty.
func (p *protoPrinter) blank(indent int) {
	if indent > 0 {
		p.b.WriteString("  ")
	}
	p.b.WriteByte('\n')
}

func (p *protoPrinter) line(indent int, format string, args ...any) {
	p.b.WriteString(strings.Repeat("  ", indent))
	fmt.Fprintf(&p.b, format, args...)
	p.b.WriteByte('\n')
}

func (p *protoPrinter) printFile() {
	var (
		imports, options, messages, enums, extends, services []ast.FileElement
	)
	if p.file.Syntax != nil {
		p.line(0, "syntax = %s;", p.raw(p.file.Syntax.Syntax))
	}
	for _, d := range p.file.Decls {
		switch n := d.(type) {
		case *ast.PackageNode:
			p.line(0, "package %s;", p.raw(n.Name))
		case *ast.ImportNode:
			imports = append(imports, d)
		case *ast.OptionNode:
			options = append(options, d)
		case *ast.MessageNode:
			messages = append(messages, d)
		case *ast.EnumNode:
			enums = append(enums, d)
		case *ast.ExtendNode:
			extends = append(extends, d)
		case *ast.ServiceNode:
			services = append(services, d)
		}
	}
	if len(imports) > 0 {
		p.b.WriteByte('\n')
		for _, d := range imports {
			n := d.(*ast.ImportNode)
			mod := ""
			switch {
			case n.Public != nil:
				mod = "public "
			case n.Weak != nil:
				mod = "weak "
			}
			p.line(0, "import %s%s;", mod, p.raw(n.Name))
		}
	}
	if len(options) > 0 {
		p.b.WriteByte('\n')
		for _, d := range options {
			p.line(0, "option %s;", p.optionText(d.(*ast.OptionNode)))
		}
	}
	if len(messages)+len(enums) > 0 {
		p.b.WriteByte('\n')
		for _, d := range messages {
			p.message(d.(*ast.MessageNode), 0)
		}
		for _, d := range enums {
			p.enum(d.(*ast.EnumNode), 0)
		}
	}
	if len(extends) > 0 {
		p.b.WriteByte('\n')
	}
	for _, d := range extends {
		p.extend(d.(*ast.ExtendNode), 0)
	}
	if len(services) > 0 {
		p.b.WriteByte('\n')
	}
	for _, d := range services {
		p.service(d.(*ast.ServiceNode))
	}
}

func (p *protoPrinter) optionText(o *ast.OptionNode) string {
	return p.raw(o.Name) + " = " + p.raw(o.Val)
}

func (p *protoPrinter) compactOptions(c *ast.CompactOptionsNode) string {
	if c == nil || len(c.Options) == 0 {
		return ""
	}
	var parts []string
	for _, o := range c.Options {
		parts = append(parts, p.optionText(o))
	}
	return " [" + strings.Join(parts, ", ") + "]"
}

func (p *protoPrinter) label(l ast.FieldLabel) string {
	if l.KeywordNode == nil {
		return ""
	}
	return l.Val + " "
}

func (p *protoPrinter) field(f *ast.FieldNode, indent int) {
	p.line(indent, "%s%s %s = %s%s;", p.label(f.Label), p.raw(f.FldType), f.Name.Val, p.raw(f.Tag), p.compactOptions(f.Options))
}

func (p *protoPrinter) mapField(f *ast.MapFieldNode, indent int) {
	p.line(indent, "map<%s, %s> %s = %s%s;", p.raw(f.MapType.KeyType), p.raw(f.MapType.ValueType), f.Name.Val, p.raw(f.Tag), p.compactOptions(f.Options))
}

func (p *protoPrinter) message(m *ast.MessageNode, indent int) {
	if len(m.Decls) == 0 { // Confluent prints an empty message on one line (recorded from a descriptor).
		p.line(indent, "message %s {}", m.Name.Val)
		return
	}
	p.line(indent, "message %s {", m.Name.Val)
	p.messageBody(m.Decls, indent+1)
	p.line(indent, "}")
}

func (p *protoPrinter) messageBody(decls []ast.MessageElement, indent int) {
	var reserved, options, fields, oneofs, groups, nested, enums, ranges []ast.MessageElement
	for _, d := range decls {
		switch d.(type) {
		case *ast.ReservedNode:
			reserved = append(reserved, d)
		case *ast.OptionNode:
			options = append(options, d)
		case *ast.FieldNode, *ast.MapFieldNode:
			fields = append(fields, d)
		case *ast.OneofNode:
			oneofs = append(oneofs, d)
		case *ast.GroupNode:
			groups = append(groups, d)
		case *ast.MessageNode:
			nested = append(nested, d)
		case *ast.EnumNode:
			enums = append(enums, d)
		case *ast.ExtensionRangeNode:
			ranges = append(ranges, d)
		}
	}
	first := true
	section := func(n int, body func()) {
		if n == 0 {
			return
		}
		if !first {
			p.blank(indent - 1)
		}
		first = false
		body()
	}
	section(len(reserved), func() {
		for _, d := range reserved {
			p.line(indent, "%s", p.statement(d))
		}
	})
	section(len(options), func() {
		for _, d := range options {
			p.line(indent, "option %s;", p.optionText(d.(*ast.OptionNode)))
		}
	})
	section(len(fields), func() {
		for _, d := range fields {
			switch f := d.(type) {
			case *ast.FieldNode:
				p.field(f, indent)
			case *ast.MapFieldNode:
				p.mapField(f, indent)
			}
		}
	})
	section(len(oneofs), func() {
		for _, d := range oneofs {
			o := d.(*ast.OneofNode)
			p.line(indent, "oneof %s {", o.Name.Val)
			for _, od := range o.Decls {
				switch f := od.(type) {
				case *ast.FieldNode:
					p.field(f, indent+1)
				case *ast.OptionNode:
					p.line(indent+1, "option %s;", p.optionText(f))
				}
			}
			p.line(indent, "}")
		}
	})
	section(len(groups), func() {
		for _, d := range groups {
			g := d.(*ast.GroupNode)
			p.line(indent, "%sgroup %s = %s%s {", p.label(g.Label), g.Name.Val, p.raw(g.Tag), p.compactOptions(g.Options))
			p.messageBody(g.Decls, indent+1)
			p.line(indent, "}")
		}
	})
	section(len(nested)+len(enums), func() {
		for _, d := range nested {
			p.message(d.(*ast.MessageNode), indent)
		}
		for _, d := range enums {
			p.enum(d.(*ast.EnumNode), indent)
		}
	})
	section(len(ranges), func() {
		for _, d := range ranges {
			p.line(indent, "%s", p.statement(d))
		}
	})
}

// statement is a one-line declaration (reserved, extensions) as written, with its
// spacing normalized and a closing semicolon.
func (p *protoPrinter) statement(n ast.Node) string {
	s := p.raw(n)
	if !strings.HasSuffix(s, ";") {
		s += ";"
	}
	return s
}

func (p *protoPrinter) enum(e *ast.EnumNode, indent int) {
	p.line(indent, "enum %s {", e.Name.Val)
	for _, d := range e.Decls {
		switch n := d.(type) {
		case *ast.OptionNode:
			p.line(indent+1, "option %s;", p.optionText(n))
		case *ast.ReservedNode:
			p.line(indent+1, "%s", p.statement(n))
		}
	}
	for _, d := range e.Decls {
		if v, ok := d.(*ast.EnumValueNode); ok {
			p.line(indent+1, "%s = %s%s;", v.Name.Val, p.raw(v.Number), p.compactOptions(v.Options))
		}
	}
	p.line(indent, "}")
}

func (p *protoPrinter) extend(e *ast.ExtendNode, indent int) {
	p.line(indent, "extend %s {", p.raw(e.Extendee))
	for _, d := range e.Decls {
		if f, ok := d.(*ast.FieldNode); ok {
			p.field(f, indent+1)
		}
	}
	p.line(indent, "}")
}

func (p *protoPrinter) service(s *ast.ServiceNode) {
	p.line(0, "service %s {", s.Name.Val)
	for _, d := range s.Decls {
		switch n := d.(type) {
		case *ast.OptionNode:
			p.line(1, "option %s;", p.optionText(n))
		case *ast.RPCNode:
			p.line(1, "rpc %s (%s) returns (%s);", n.Name.Val, p.rpcType(n.Input), p.rpcType(n.Output))
		}
	}
	p.line(0, "}")
}

func (p *protoPrinter) rpcType(t *ast.RPCTypeNode) string {
	if t.Stream != nil {
		return "stream " + p.raw(t.MessageType)
	}
	return p.raw(t.MessageType)
}

var _ = io.EOF
