package sql

import "fmt"

// Scope identifies the clause that owns a setting.
type Scope string

const (
	SourceSettings      Scope = "source"
	DestinationSettings Scope = "destination"
	CopySettings        Scope = "copy"
)

func (s Scope) Names() []string {
	switch s {
	case SourceSettings:
		return []string{"GROUP", "START", "MAX_STALENESS"}
	case DestinationSettings:
		return []string{"ROTATE_AFTER_SIZE", "ROTATE_AFTER_AGE", "PARTITIONING"}
	default:
		return []string{"WORKER_STOP_AFTER_RECORDS", "ON_ERROR", "ON_MISSING_TIMESTAMP", "ON_REJECTED_TIMESTAMP"}
	}
}

func (s Scope) Clause() string {
	switch s {
	case SourceSettings:
		return "WITH after the FROM endpoint"
	case DestinationSettings:
		return "WITH after the INTO endpoint"
	default:
		return "OPTIONS"
	}
}

// SettingScope returns the owner of a recognized setting.
func SettingScope(name string) Scope {
	for _, scope := range []Scope{SourceSettings, DestinationSettings, CopySettings} {
		for _, known := range scope.Names() {
			if name == known {
				return scope
			}
		}
	}
	return ""
}

// CheckScope never includes an unknown input name in a diagnostic.
func CheckScope(name string, expected Scope) error {
	actual := SettingScope(name)
	if actual == "" {
		return fmt.Errorf("unsupported setting; use the settings supported in %s", expected.Clause())
	}
	if actual != expected {
		return fmt.Errorf("%s is a %s setting; put it in %s", name, actual, actual.Clause())
	}
	return nil
}
