package pglock

import (
	"bytes"
	"os"
	"testing"
)

func TestValidateSchemaSQL(t *testing.T) {
	var buf bytes.Buffer
	for i, cmd := range createTableSchemaCommands {
		if err := cmd.Execute(&buf, createTableTemplateValue{TableName: "locks", Modifier: "IF NOT EXISTS"}); err != nil {
			t.Fatalf("cannot render buffer (%d): %v", i, err)
		}
		buf.WriteString(";\n")
	}
	current, err := os.ReadFile("schema.sql")
	if err != nil {
		t.Fatalf("cannot read schema.sql: %v", err)
	}
	if !bytes.Equal(current, buf.Bytes()) {
		t.Logf("%s (%d)", current, len(current))
		t.Logf("%s (%d)", buf.String(), buf.Len())
		t.Fatalf("schema mismatch")
	}
}
