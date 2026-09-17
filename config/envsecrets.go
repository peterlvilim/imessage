// mautrix-imessage - A Matrix-iMessage puppeting bridge.
// Copyright (C) 2026 Tulir Asokan
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

package config

import (
	"fmt"
	"os"
	"strings"
)

// Reading secrets out of files named by the environment, so a deployment never
// has to write them into the config file.
//
// Every bridgev2 bridge does this natively: `env_config_prefix: MAUTRIX_` turns
// on env overrides, and a variable whose name ends in _FILE makes the bridge
// read the value out of that path, with `__` in the name standing for `.` in the
// config. This bridge predates all of it and mautrix-go's legacy bridgeconfig
// has no env mechanism of any kind, so the only way in was to render the secrets
// into the YAML before starting -- which means a shell script that has to quote
// arbitrary values correctly, a tmpfs to hold the result, and secrets on a
// filesystem the process can read for as long as it runs.
//
// Same variable names as the bridgev2 bridges, so a compose file does not have
// to care which generation a bridge belongs to.
//
// Applied from Config.UnmarshalYAML, which is the one point that is after the
// file is parsed and before anything reads it: bridge.go takes the tokens in
// MakeAppService, well before the Child.Init hook.

type fileSecret struct {
	env    string
	assign func(*Config, string)
}

var fileSecrets = []fileSecret{
	{"MAUTRIX_APPSERVICE__AS_TOKEN_FILE", func(c *Config, v string) { c.AppService.ASToken = v }},
	{"MAUTRIX_APPSERVICE__HS_TOKEN_FILE", func(c *Config, v string) { c.AppService.HSToken = v }},
	{"MAUTRIX_IMESSAGE__BLUEBUBBLES_PASSWORD_FILE", func(c *Config, v string) { c.IMessage.BlueBubblesPassword = v }},
	// The double-puppet credential. In this fork that is normally the shared
	// doublepuppet appservice token in "as_token:<token>" form, which
	// custompuppet.go prefix-matches to assert identity instead of logging in;
	// upstream's HMAC-shared-secret and "appservice" modes still work if set.
	// Kept out of the config file for the same reason as the rest -- it is
	// generated per-deployment and never has to be seen by a human.
	{"MAUTRIX_BRIDGE__LOGIN_SHARED_SECRET_FILE", func(c *Config, v string) { c.Bridge.LoginSharedSecret = v }},
}

// applyFileSecrets overwrites config values from the files named by the
// environment. An unset variable leaves the config value alone; a set one that
// cannot be read is fatal rather than ignored, because the alternative is
// starting with whatever placeholder the config held and failing later as an
// authentication error that says nothing about the real cause.
func (c *Config) applyFileSecrets() error {
	for _, s := range fileSecrets {
		path := os.Getenv(s.env)
		if path == "" {
			continue
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("failed to read %s from %s: %w", s.env, path, err)
		}
		// Trailing newline only. A secret may legitimately contain spaces, and
		// trimming those would corrupt it silently.
		c2 := strings.TrimRight(string(data), "\r\n")
		if c2 == "" {
			return fmt.Errorf("%s names %s, which is empty", s.env, path)
		}
		s.assign(c, c2)
	}
	return nil
}

// umConfig avoids infinite recursion in UnmarshalYAML: the alias has the same
// fields and tags but none of the methods. Same shape as BridgeConfig's.
type umConfig Config

func (c *Config) UnmarshalYAML(unmarshal func(interface{}) error) error {
	if err := unmarshal((*umConfig)(c)); err != nil {
		return err
	}
	return c.applyFileSecrets()
}
