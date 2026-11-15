package guarantees

import (
	"flag"
	"fmt"
	"go/ast"
	"go/build/constraint"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"unicode"
	"unicode/utf8"
)

const directive = "//kaferry:guarantee"

var unmapped = flag.Bool("unmapped", false, "list system and compatibility tests that cite no guarantee")

// evidence is one test function and the guarantees it cites.
type evidence struct {
	file  string // repository-relative
	name  string
	tags  string // //go:build expression, empty for unit tests
	cites []string
}

func TestGuaranteeMap(t *testing.T) {
	// Given the registry and every test function in the repository,
	// when directives are collected, then each guarantee has product evidence
	// and every directive is well-formed and attached to a test.
	root := repositoryRoot(t)
	tests, problems := scan(t, root)
	known, registryProblems := checkRegistry(root)
	cited, citationProblems := collectCitations(tests, known)
	for _, p := range append(append(problems, registryProblems...), citationProblems...) {
		t.Error(p)
	}
	if testing.Verbose() {
		report(t, cited)
	}
	if *unmapped {
		listUnmapped(t, tests)
	}
}

func checkRegistry(root string) (map[string]bool, []string) {
	known := map[string]bool{}
	var problems []string
	for _, g := range Registry {
		if known[g.ID] {
			problems = append(problems, "duplicate guarantee "+g.ID)
		}
		known[g.ID] = true
		if err := checkAnchor(root, g.Doc); err != nil {
			problems = append(problems, fmt.Sprintf("%s: %v", g.ID, err))
		}
	}
	return known, problems
}

func collectCitations(tests []evidence, known map[string]bool) (map[string][]evidence, []string) {
	cited := map[string][]evidence{}
	var problems []string
	for _, e := range tests {
		for _, id := range e.cites {
			switch {
			case !known[id]:
				problems = append(problems, fmt.Sprintf("%s %s: unknown guarantee %q", e.file, e.name, id))
			case requires(e.tags, "harness"):
				problems = append(problems, fmt.Sprintf("%s %s: harness tests check fixtures, not product guarantees; cite %s from a product test", e.file, e.name, id))
			default:
				cited[id] = append(cited[id], e)
			}
		}
	}
	for _, g := range Registry {
		if len(cited[g.ID]) == 0 {
			problems = append(problems, fmt.Sprintf("%s has no test evidence (%s)", g.ID, g.Doc))
		}
	}
	return cited, problems
}

func report(t *testing.T, cited map[string][]evidence) {
	for _, g := range Registry {
		t.Logf("%s (%s)", g.ID, g.Doc)
		for _, e := range cited[g.ID] {
			t.Logf("    %-12s %s (%s)", suite(e.tags), e.name, e.file)
		}
	}
}

// listUnmapped reports end-to-end tests that cite nothing: each should either
// cite a guarantee, check a fixture under the harness tag, or be deleted.
func listUnmapped(t *testing.T, tests []evidence) {
	for _, e := range tests {
		if len(e.cites) == 0 && strings.HasPrefix(e.file, "src/tests/") && !strings.HasPrefix(e.file, "src/tests/guarantees/") {
			t.Logf("unmapped %-12s %s (%s)", suite(e.tags), e.name, e.file)
		}
	}
}

func suite(tags string) string {
	if tags == "" {
		return "default"
	}
	for _, s := range []string{"harness", "migration", "qualification", "container", "fault", "load"} {
		if requires(tags, s) {
			return s
		}
	}
	return "integration"
}

// requires reports whether a build expression can hold only with tag set.
func requires(expr, tag string) bool {
	if expr == "" {
		return false
	}
	parsed, err := constraint.Parse("//go:build " + expr)
	if err != nil {
		return false
	}
	return !parsed.Eval(func(name string) bool { return name != tag })
}

func scan(t *testing.T, root string) ([]evidence, []string) {
	var tests []evidence
	var problems []string
	err := filepath.WalkDir(filepath.Join(root, "src"), func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, "_test.go") {
			return err
		}
		rel, _ := filepath.Rel(root, path)
		rel = filepath.ToSlash(rel)
		found, issues := scanFile(path, rel)
		tests = append(tests, found...)
		problems = append(problems, issues...)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return tests, problems
}

func scanFile(path, rel string) ([]evidence, []string) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, nil, parser.ParseComments)
	if err != nil {
		return nil, []string{err.Error()}
	}
	tags := buildConstraint(fset, file)
	var tests []evidence
	var problems []string
	attached := map[*ast.Comment]bool{}
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || !isTest(fn) {
			continue
		}
		e, issues := citations(fn, evidence{file: rel, name: fn.Name.Name, tags: tags}, attached)
		tests = append(tests, e)
		problems = append(problems, issues...)
	}
	return tests, append(problems, strayDirectives(fset, file, rel, attached)...)
}

func buildConstraint(fset *token.FileSet, file *ast.File) string {
	header := fset.Position(file.Package).Line
	for _, group := range file.Comments {
		for _, c := range group.List {
			if constraint.IsGoBuild(c.Text) && fset.Position(c.Pos()).Line < header {
				return strings.TrimSpace(strings.TrimPrefix(c.Text, "//go:build"))
			}
		}
	}
	return ""
}

func citations(fn *ast.FuncDecl, e evidence, attached map[*ast.Comment]bool) (evidence, []string) {
	if fn.Doc == nil {
		return e, nil
	}
	var problems []string
	for _, c := range fn.Doc.List {
		ids, ok := parseDirective(c.Text)
		if !ok {
			continue
		}
		attached[c] = true
		if len(ids) == 0 {
			problems = append(problems, fmt.Sprintf("%s %s: empty %s", e.file, e.name, directive))
		}
		e.cites = append(e.cites, ids...)
	}
	return e, problems
}

func strayDirectives(fset *token.FileSet, file *ast.File, rel string, attached map[*ast.Comment]bool) []string {
	var problems []string
	for _, group := range file.Comments {
		for _, c := range group.List {
			if _, ok := parseDirective(c.Text); ok && !attached[c] {
				problems = append(problems, fmt.Sprintf("%s:%d: %s must directly precede a Test or Fuzz function", rel, fset.Position(c.Pos()).Line, directive))
			}
		}
	}
	return problems
}

// isTest applies go test's rule: TestXxx or FuzzXxx where Xxx does not start
// with a lowercase letter, taking one *testing.T or *testing.F parameter.
func isTest(fn *ast.FuncDecl) bool {
	if fn.Recv != nil || fn.Type.Params == nil || len(fn.Type.Params.List) != 1 || len(fn.Type.Params.List[0].Names) > 1 {
		return false
	}
	param, kind := "", ""
	if star, ok := fn.Type.Params.List[0].Type.(*ast.StarExpr); ok {
		if sel, ok := star.X.(*ast.SelectorExpr); ok {
			param = sel.Sel.Name
		}
	}
	name := fn.Name.Name
	switch {
	case strings.HasPrefix(name, "Test"):
		kind, name = "T", name[len("Test"):]
	case strings.HasPrefix(name, "Fuzz"):
		kind, name = "F", name[len("Fuzz"):]
	default:
		return false
	}
	first, _ := utf8.DecodeRuneInString(name)
	return param == kind && (name == "" || !unicode.IsLower(first))
}

func parseDirective(text string) ([]string, bool) {
	rest, ok := strings.CutPrefix(text, directive)
	if !ok || (rest != "" && rest[0] != ' ') {
		return nil, false
	}
	return strings.Fields(rest), true
}

var headingPattern = regexp.MustCompile(`^#{1,6}\s+(.+?)\s*$`)

func checkAnchor(root, doc string) error {
	path, anchor, ok := strings.Cut(doc, "#")
	if !ok {
		return fmt.Errorf("%s needs a heading anchor", doc)
	}
	data, err := os.ReadFile(filepath.Join(root, path))
	if err != nil {
		return err
	}
	inFence := false
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "```") {
			inFence = !inFence
		}
		if m := headingPattern.FindStringSubmatch(line); m != nil && !inFence && slug(m[1]) == anchor {
			return nil
		}
	}
	return fmt.Errorf("%s has no heading #%s", path, anchor)
}

// slug follows GitHub's heading anchors for the plain headings used in docs.
func slug(heading string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(heading) {
		switch {
		case r == ' ':
			b.WriteRune('-')
		case r == '-' || r == '_' || r >= 'a' && r <= 'z' || r >= '0' && r <= '9':
			b.WriteRune(r)
		}
	}
	return b.String()
}

func repositoryRoot(t *testing.T) string {
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found")
		}
		dir = parent
	}
}

func TestGuaranteeMapRejectsMisplacedAndHarnessEvidence(t *testing.T) {
	// Given directives on a harness test, helpers and a fuzz test,
	// when the file is scanned, then only real tests cite guarantees,
	// directives on helpers are reported and the harness tag is detected.
	path := filepath.Join(t.TempDir(), "x_test.go")
	source := `//go:build integration && harness

package x

//kaferry:guarantee delivery
func TestFixture(t *testing.T) {}

//kaferry:guarantee delivery
func helper() {}

// Ordinary prose first.
//kaferry:guarantee  caps   shutdown
func FuzzCaps(f *testing.F) {}

//kaferry:guaranteed not a directive
func TestOther(t *testing.T) {}

//kaferry:guarantee caps
func Testhelper(t *testing.T) {}

//kaferry:guarantee caps
func TestWithoutT() {}

//kaferry:guarantee caps
func Testé(t *testing.T) {}
`
	if err := os.WriteFile(path, []byte(source), 0o600); err != nil {
		t.Fatal(err)
	}
	tests, problems := scanFile(path, "x_test.go")
	if len(problems) != 4 || !strings.Contains(problems[0], "x_test.go:8:") || !strings.Contains(problems[1], "x_test.go:18:") || !strings.Contains(problems[2], "x_test.go:21:") || !strings.Contains(problems[3], "x_test.go:24:") {
		t.Fatalf("problems = %q, want stray directives at lines 8, 18, 21 and 24", problems)
	}
	got := map[string]string{}
	for _, e := range tests {
		got[e.name] = strings.Join(e.cites, " ")
		if !requires(e.tags, "harness") || requires(e.tags, "fault") {
			t.Errorf("%s tags %q: harness requirement misread", e.name, e.tags)
		}
	}
	want := map[string]string{"TestFixture": "delivery", "FuzzCaps": "caps shutdown", "TestOther": ""}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("cites = %v, want %v", got, want)
	}
	if slug("Added assignments and lookups") != "added-assignments-and-lookups" || slug("Endpoint WITH properties") != "endpoint-with-properties" {
		t.Fatal("heading anchors differ from GitHub's")
	}
	if checkAnchor(repositoryRoot(t), decisions+"#no-such-heading") == nil {
		t.Fatal("missing heading accepted")
	}
}
