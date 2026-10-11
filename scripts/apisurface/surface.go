package main

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"io/fs"
	"sort"
	"strings"
)

// Surface returns the exported declarations of the non-test package in dir,
// sorted, one per line. Struct fields and interface methods that are exported
// are listed under their type; unexported ones are not part of the surface.
func Surface(dir string) ([]string, error) {
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, dir, func(fi fs.FileInfo) bool {
		return !strings.HasSuffix(fi.Name(), "_test.go")
	}, 0)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, pkg := range pkgs {
		if strings.HasSuffix(pkg.Name, "_test") {
			continue
		}
		for _, f := range pkg.Files {
			for _, d := range f.Decls {
				out = append(out, declLines(fset, d)...)
			}
		}
	}
	sort.Strings(out)
	return out, nil
}

func expr(fset *token.FileSet, n ast.Node) string {
	var b bytes.Buffer
	if err := printer.Fprint(&b, fset, n); err != nil {
		return "?"
	}
	return strings.Join(strings.Fields(b.String()), " ")
}

func declLines(fset *token.FileSet, d ast.Decl) []string {
	var out []string
	switch d := d.(type) {
	case *ast.FuncDecl:
		if !d.Name.IsExported() {
			return nil
		}
		if d.Recv != nil && len(d.Recv.List) == 1 {
			if !ast.IsExported(recvName(d.Recv.List[0].Type)) {
				return nil
			}
			out = append(out, fmt.Sprintf("method (%s) %s%s", expr(fset, d.Recv.List[0].Type), d.Name.Name, strings.TrimPrefix(expr(fset, d.Type), "func")))
			return out
		}
		out = append(out, "func "+d.Name.Name+strings.TrimPrefix(expr(fset, d.Type), "func"))
	case *ast.GenDecl:
		for _, s := range d.Specs {
			switch s := s.(type) {
			case *ast.TypeSpec:
				if !s.Name.IsExported() {
					continue
				}
				out = append(out, typeLines(fset, s)...)
			case *ast.ValueSpec:
				kind := strings.ToLower(d.Tok.String())
				typ := ""
				if s.Type != nil {
					typ = " " + expr(fset, s.Type)
				}
				for _, n := range s.Names {
					if n.IsExported() {
						out = append(out, fmt.Sprintf("%s %s%s", kind, n.Name, typ))
					}
				}
			}
		}
	}
	return out
}

func recvName(t ast.Expr) string {
	switch t := t.(type) {
	case *ast.StarExpr:
		return recvName(t.X)
	case *ast.IndexExpr:
		return recvName(t.X)
	case *ast.Ident:
		return t.Name
	}
	return ""
}

func typeLines(fset *token.FileSet, s *ast.TypeSpec) []string {
	name := s.Name.Name
	switch t := s.Type.(type) {
	case *ast.StructType:
		out := []string{"type " + name + " struct"}
		for _, f := range t.Fields.List {
			typ := expr(fset, f.Type)
			if len(f.Names) == 0 { // embedded
				if ast.IsExported(recvName(f.Type)) || strings.Contains(typ, ".") {
					out = append(out, fmt.Sprintf("field %s embeds %s", name, typ))
				}
				continue
			}
			for _, n := range f.Names {
				if n.IsExported() {
					out = append(out, fmt.Sprintf("field %s.%s %s", name, n.Name, typ))
				}
			}
		}
		return out
	case *ast.InterfaceType:
		out := []string{"type " + name + " interface"}
		for _, m := range t.Methods.List {
			if len(m.Names) == 0 {
				out = append(out, fmt.Sprintf("iface %s embeds %s", name, expr(fset, m.Type)))
				continue
			}
			for _, n := range m.Names {
				if n.IsExported() {
					out = append(out, fmt.Sprintf("iface %s.%s%s", name, n.Name, strings.TrimPrefix(expr(fset, m.Type), "func")))
				}
			}
		}
		return out
	default:
		eq := " "
		if s.Assign.IsValid() {
			eq = " = "
		}
		return []string{"type " + name + eq + expr(fset, s.Type)}
	}
}
