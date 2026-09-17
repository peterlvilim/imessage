package config

import (
	"os"
	"path/filepath"
	"testing"

	"gopkg.in/yaml.v3"
	"maunium.net/go/mautrix/bridge/bridgeconfig"
)

const secretsTestYAML = `
appservice:
  as_token: PLACEHOLDER
  hs_token: PLACEHOLDER
imessage:
  platform: bluebubbles
  bluebubbles_password: PLACEHOLDER
`

func writeSecret(t *testing.T, dir, name, value string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(value), 0o600); err != nil {
		t.Fatalf("write %s: %v", p, err)
	}
	return p
}

func unmarshalTestConfig(t *testing.T) (*Config, error) {
	t.Helper()
	cfg := &Config{BaseConfig: &bridgeconfig.BaseConfig{}}
	cfg.BaseConfig.Bridge = &cfg.Bridge
	return cfg, yaml.Unmarshal([]byte(secretsTestYAML), cfg)
}

// The whole point: a secret in a file reaches the parsed config, so nothing has
// to render it into the YAML first.
func TestFileSecretsOverrideConfig(t *testing.T) {
	dir := t.TempDir()
	// Trailing newline on one, and a password full of the characters that break
	// a shell renderer -- the failure mode this replaces.
	t.Setenv("MAUTRIX_APPSERVICE__AS_TOKEN_FILE", writeSecret(t, dir, "as", "as-from-file\n"))
	t.Setenv("MAUTRIX_APPSERVICE__HS_TOKEN_FILE", writeSecret(t, dir, "hs", "hs-from-file"))
	t.Setenv("MAUTRIX_IMESSAGE__BLUEBUBBLES_PASSWORD_FILE",
		writeSecret(t, dir, "bb", "p@ss'w|rd&with\\slash\"and#hash and spaces\n"))

	cfg, err := unmarshalTestConfig(t)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if cfg.AppService.ASToken != "as-from-file" {
		t.Errorf("as_token = %q, want %q", cfg.AppService.ASToken, "as-from-file")
	}
	if cfg.AppService.HSToken != "hs-from-file" {
		t.Errorf("hs_token = %q, want %q", cfg.AppService.HSToken, "hs-from-file")
	}
	want := "p@ss'w|rd&with\\slash\"and#hash and spaces"
	if cfg.IMessage.BlueBubblesPassword != want {
		t.Errorf("bluebubbles_password = %q, want %q", cfg.IMessage.BlueBubblesPassword, want)
	}
}

// An unset variable must leave the config value alone, so this is opt-in.
func TestFileSecretsAbsentLeavesConfig(t *testing.T) {
	cfg, err := unmarshalTestConfig(t)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if cfg.AppService.ASToken != "PLACEHOLDER" {
		t.Errorf("as_token = %q, want the config value untouched", cfg.AppService.ASToken)
	}
}

// A named file that cannot be read must be fatal. Starting with a placeholder
// surfaces later as an authentication error that says nothing about the cause.
func TestFileSecretsMissingFileIsFatal(t *testing.T) {
	t.Setenv("MAUTRIX_APPSERVICE__AS_TOKEN_FILE", filepath.Join(t.TempDir(), "nope"))
	if _, err := unmarshalTestConfig(t); err == nil {
		t.Fatal("expected an error for an unreadable secret file, got nil")
	}
}

// So is an empty one: it means the secret was never written.
func TestFileSecretsEmptyFileIsFatal(t *testing.T) {
	t.Setenv("MAUTRIX_APPSERVICE__AS_TOKEN_FILE", writeSecret(t, t.TempDir(), "empty", "\n"))
	if _, err := unmarshalTestConfig(t); err == nil {
		t.Fatal("expected an error for an empty secret file, got nil")
	}
}
