package console

import (
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"strings"
)

// The Compatibility tab (Sc-09): the level a new version must satisfy, for this
// subject or as the registry's default, and the mode; and a check of the latest
// version at a level before it is saved. A subject's own level wins over the
// global one, and dropping it hands the subject back.

// levelRow is one level in Sc-09's table.
type levelRow struct {
	Level, Says string
	Now         bool // the level in force for the subject.
	On          bool // the one chosen in the form.
}

// levelSays is what each level asks of a new version, as Sc-09 says it.
var levelSays = map[string]string{
	"NONE":                "Nothing is checked. Any schema may follow.",
	"BACKWARD":            "Readers on the new schema can read data written with the previous version.",
	"BACKWARD_TRANSITIVE": "The same, against every earlier version, not only the last.",
	"FORWARD":             "Readers on the previous version can read data written with the new schema.",
	"FORWARD_TRANSITIVE":  "The same, for every earlier version.",
	"FULL":                "Both BACKWARD and FORWARD, against the previous version.",
	"FULL_TRANSITIVE":     "Both, against every earlier version.",
}

// levelOrder is Sc-09's order: NONE, then each level and its transitive form.
var levelOrder = []string{"NONE", "BACKWARD", "BACKWARD_TRANSITIVE", "FORWARD", "FORWARD_TRANSITIVE", "FULL", "FULL_TRANSITIVE"}

func levelRows(now string) []levelRow {
	rows := make([]levelRow, 0, len(levelOrder))
	for _, l := range levelOrder {
		rows = append(rows, levelRow{Level: l, Says: levelSays[l], Now: l == now, On: l == now})
	}
	return rows
}

// schemaConfig saves the Compatibility tab: the level, for the subject or the
// registry (scope), and the mode, for the subject or the registry. A level of
// "default" drops the subject's own, so it follows the registry's.
func (c *Console) schemaConfig(w http.ResponseWriter, r *http.Request) {
	if c.registryOff(w, r) {
		return
	}
	ctx := r.Context()
	subject := groupFromSegment(r.PathValue("subject"))
	level := r.FormValue("level")
	// Remove it is a button in the same form as the level rows: its "default" is sent
	// beside the level that is checked, and is what was asked.
	if r.ParseForm() == nil && slices.Contains(r.Form["level"], "default") {
		level = "default"
	}
	global := r.FormValue("scope") == "global"
	var err error
	switch {
	case level == "":
	case global && level != "default":
		err = c.reg.SetGlobalCompat(ctx, level)
	case level == "default":
		err = c.reg.ClearCompat(ctx, subject)
		var re *regError
		if errors.As(err, &re) && re.Status == http.StatusNotFound {
			err = nil // it was not set.
		}
	default:
		err = c.reg.SetCompat(ctx, subject, level)
	}
	if err != nil {
		c.fail(w, r, err)
		return
	}
	if mode := r.FormValue("mode"); mode != "" {
		if err := c.setMode(r, subject, mode, global); err != nil {
			c.fail(w, r, err)
			return
		}
	}
	c.redirect(w, r, c.schemaURL(subject)+"?tab=compat", "Compatibility updated")
}

// setMode sets the subject's mode, or the registry's; "default" drops the
// subject's own. A mode that is already so is left as it is.
func (c *Console) setMode(r *http.Request, subject, mode string, global bool) error {
	ctx := r.Context()
	if global {
		now, _ := c.reg.Mode(ctx)
		if now == mode {
			return nil
		}
		return c.reg.SetMode(ctx, "", mode)
	}
	if mode == "default" {
		err := c.reg.ClearMode(ctx, subject)
		var re *regError
		if errors.As(err, &re) && re.Status == http.StatusNotFound {
			return nil
		}
		return err
	}
	// The mode in force is left alone: the form sends the switch as it stands, and
	// setting it again would give the subject a mode of its own it did not ask for.
	if now, _, _ := c.reg.SubjectMode(ctx, subject); now == mode {
		return nil
	}
	return c.reg.SetMode(ctx, subject, mode)
}

// schemaMode is Sc-15's "Set READWRITE for this subject": the subject's mode alone.
func (c *Console) schemaMode(w http.ResponseWriter, r *http.Request) {
	if c.registryOff(w, r) {
		return
	}
	subject := groupFromSegment(r.PathValue("subject"))
	mode := r.FormValue("mode")
	if mode == "" {
		c.fail(w, r, badInput("say which mode: READWRITE or READONLY"))
		return
	}
	if err := c.setMode(r, subject, mode, r.FormValue("scope") == "global"); err != nil {
		c.fail(w, r, err)
		return
	}
	c.redirect(w, r, c.schemaURL(subject), "Mode is "+strings.ToUpper(mode))
}

// schemaLevelCheck is Sc-09's "Test v3 against this level": whether the latest
// version passes against the versions before it at a level, without saving it.
func (c *Console) schemaLevelCheck(w http.ResponseWriter, r *http.Request) {
	if c.registryOff(w, r) {
		return
	}
	ctx := r.Context()
	subject := groupFromSegment(r.PathValue("subject"))
	level := r.FormValue("level")
	if levelSays[level] == "" {
		c.fail(w, r, badInput("%q is not a compatibility level", level))
		return
	}
	versions, err := c.reg.Versions(ctx, subject)
	if err != nil {
		c.failSubject(w, r, err, subject)
		return
	}
	latest, err := c.reg.Version(ctx, subject, 0)
	if err != nil {
		c.failSubject(w, r, err, subject)
		return
	}
	out := map[string]any{"Level": level, "Latest": latest.Version}
	if len(versions) < 2 {
		out["Alone"] = true
		c.partial(w, "schema_level_check", out)
		return
	}
	// Against the one before it; a transitive level, against every one before it.
	against := strconv.Itoa(versions[len(versions)-2])
	if strings.HasSuffix(level, "_TRANSITIVE") {
		against = ""
	}
	a, err := c.reg.CheckAt(ctx, subject, schemaBody{Schema: latest.Schema, SchemaType: latest.SchemaType, References: refsOf(latest)}, level, against, false)
	if err != nil {
		c.fail(w, r, err)
		return
	}
	var compared []int
	for _, v := range a.Compared {
		if v != latest.Version {
			compared = append(compared, v)
		}
	}
	out["OK"], out["Compared"] = a.Compatible, compared
	if !a.Compatible && len(a.Findings) > 0 && len(a.Findings[0].Reasons) > 0 {
		f := a.Findings[0]
		out["Why"] = fmt.Sprintf("against v%d: %s", f.Version, f.Reasons[0].Description)
	}
	c.partial(w, "schema_level_check", out)
}
